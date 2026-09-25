package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/1morshed1/gatewayx/internal/domain"
	"github.com/1morshed1/gatewayx/internal/store"
)

// Consumer reads request-log events from Kafka and rolls them up into the
// request_analytics table. It runs as its own process (`gateway analytics`) so
// analytics processing scales and fails independently of the data plane.
type Consumer struct {
	reader *kafka.Reader
	store  *store.Store
	log    *slog.Logger
}

// NewConsumer builds a consumer in a consumer group, so multiple instances share
// the topic's partitions (horizontal scaling) and offsets are tracked in Kafka.
func NewConsumer(brokers []string, topic, groupID string, st *store.Store, log *slog.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	return &Consumer{reader: r, store: st, log: log}
}

// Run consumes until the context is cancelled. It aggregates events in memory and
// flushes rollups to Postgres every flushInterval — batching keeps DB write load
// proportional to distinct (minute,route,status) buckets, not raw request volume.
func (c *Consumer) Run(ctx context.Context) error {
	const flushInterval = 5 * time.Second
	agg := make(map[aggKey]*store.AnalyticsBucket)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(agg) == 0 {
			return
		}
		batch := make([]store.AnalyticsBucket, 0, len(agg))
		for _, b := range agg {
			batch = append(batch, *b)
		}
		if err := c.store.AddAnalytics(ctx, batch); err != nil {
			c.log.Warn("analytics flush failed", "err", err)
			return // keep agg so we retry next tick
		}
		agg = make(map[aggKey]*store.AnalyticsBucket)
	}

	// Read in a goroutine so the ticker can flush even during a read wait.
	msgCh := make(chan domain.RequestLog, 1000)
	go c.readLoop(ctx, msgCh)

	for {
		select {
		case <-ctx.Done():
			flush()
			return c.reader.Close()
		case <-ticker.C:
			flush()
		case rl := <-msgCh:
			k := aggKey{
				bucket:      rl.Time.Truncate(time.Minute),
				routeID:     rl.RouteID,
				statusClass: rl.Status / 100,
			}
			if !k.valid() {
				continue // route-less events (e.g. 404 no match) aren't aggregated
			}
			b, ok := agg[k]
			if !ok {
				b = &store.AnalyticsBucket{
					Bucket:      k.bucket,
					RouteID:     rl.RouteID,
					RouteName:   rl.RouteName,
					StatusClass: k.statusClass,
				}
				agg[k] = b
			}
			b.Count++
			b.TotalDuration += rl.DurationMs
		}
	}
}

func (c *Consumer) readLoop(ctx context.Context, out chan<- domain.RequestLog) {
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			c.log.Warn("kafka read error", "err", err)
			continue
		}
		var rl domain.RequestLog
		if err := json.Unmarshal(m.Value, &rl); err != nil {
			c.log.Warn("bad analytics event", "err", err)
			continue
		}
		select {
		case out <- rl:
		case <-ctx.Done():
			return
		}
	}
}

type aggKey struct {
	bucket      time.Time
	routeID     string
	statusClass int
}

// route_id can be empty (404s with no matched route). Postgres route_id is UUID
// NOT NULL, so we skip aggregating events without a route rather than fail inserts.
func (k aggKey) valid() bool { return k.routeID != "" }
