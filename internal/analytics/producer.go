// Package analytics streams one event per proxied request to Kafka, and provides
// a consumer that aggregates those events into Postgres for reporting.
//
// Design principle (ADR-006): analytics must NEVER slow down or fail a request.
// The producer is fire-and-forget with a bounded in-memory buffer:
//   - Emit() does a non-blocking send to a channel and returns instantly.
//   - A background worker batches from the channel and writes to Kafka.
//   - If the buffer is full (Kafka slow/down), events are DROPPED, not queued
//     unboundedly and not blocking the caller.
//
// Availability of the proxy always beats completeness of analytics.
package analytics

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// KafkaEmitter implements proxy.Emitter, writing request logs to a Kafka topic.
type KafkaEmitter struct {
	writer  *kafka.Writer
	ch      chan domain.RequestLog
	log     *slog.Logger
	dropped atomic.Int64
	wg      sync.WaitGroup
	closeMu sync.Once
}

// NewKafkaEmitter creates the emitter and starts its background worker. bufferSize
// bounds how many events can be in flight before new ones are dropped.
func NewKafkaEmitter(brokers []string, topic string, bufferSize int, log *slog.Logger) *KafkaEmitter {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // partition by key (route id) for locality
		BatchTimeout: 200 * time.Millisecond,
		BatchSize:    100,
		// Async=false: we do our own batching/backpressure via the channel, and
		// want WriteMessages errors surfaced to our worker for logging.
		RequiredAcks: kafka.RequireOne,
	}
	e := &KafkaEmitter{
		writer: w,
		ch:     make(chan domain.RequestLog, bufferSize),
		log:    log,
	}
	e.wg.Add(1)
	go e.run()
	return e
}

// Emit queues an event without blocking. On a full buffer the event is dropped and
// counted — this is the fire-and-forget guarantee.
func (e *KafkaEmitter) Emit(rl domain.RequestLog) {
	select {
	case e.ch <- rl:
	default:
		e.dropped.Add(1)
	}
}

// run is the background worker: it drains the channel into batches and writes them
// to Kafka. Batching amortises the network cost across many events.
func (e *KafkaEmitter) run() {
	defer e.wg.Done()

	// Periodically report drops so a chronically-full buffer is visible.
	report := time.NewTicker(30 * time.Second)
	defer report.Stop()

	for {
		select {
		case rl, ok := <-e.ch:
			if !ok {
				return // channel closed by Close()
			}
			e.writeBatch(e.collect(rl))
		case <-report.C:
			if d := e.dropped.Swap(0); d > 0 {
				e.log.Warn("analytics events dropped (buffer full)", "count", d)
			}
		}
	}
}

// collect takes the first event and greedily drains up to a batch's worth more
// that are already waiting, without blocking.
func (e *KafkaEmitter) collect(first domain.RequestLog) []domain.RequestLog {
	batch := []domain.RequestLog{first}
	for len(batch) < 100 {
		select {
		case rl, ok := <-e.ch:
			if !ok {
				return batch
			}
			batch = append(batch, rl)
		default:
			return batch
		}
	}
	return batch
}

func (e *KafkaEmitter) writeBatch(batch []domain.RequestLog) {
	msgs := make([]kafka.Message, 0, len(batch))
	for _, rl := range batch {
		val, err := json.Marshal(rl)
		if err != nil {
			continue
		}
		msgs = append(msgs, kafka.Message{Key: []byte(rl.RouteID), Value: val})
	}
	// Bounded write timeout: never let a Kafka stall wedge the worker forever.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.writer.WriteMessages(ctx, msgs...); err != nil {
		e.log.Warn("analytics write failed", "err", err, "batch", len(msgs))
	}
}

// Close stops the worker and flushes the Kafka writer. Safe to call once.
func (e *KafkaEmitter) Close() {
	e.closeMu.Do(func() {
		close(e.ch)
		e.wg.Wait()
		_ = e.writer.Close()
	})
}
