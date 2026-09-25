package circuitbreaker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewManager(client, cfg)
}

// TestOpensAfterThreshold: threshold consecutive failures trip the breaker, and
// once open it denies requests.
func TestOpensAfterThreshold(t *testing.T) {
	m := newTestManager(t, Config{FailureThreshold: 3, OpenTimeout: time.Minute, Window: time.Minute})
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if st, _ := m.Record(ctx, "svc", false); st == StateOpen {
			t.Fatalf("opened too early at failure %d", i+1)
		}
	}
	// 3rd failure hits the threshold.
	if st, _ := m.Record(ctx, "svc", false); st != StateOpen {
		t.Fatalf("want open after threshold, got %s", st)
	}

	_, allowed, _ := m.Allow(ctx, "svc")
	if allowed {
		t.Fatal("open breaker should deny requests")
	}
}

// TestHalfOpenThenClose: after the open timeout a probe is allowed (half-open),
// and a success closes the breaker.
func TestHalfOpenThenClose(t *testing.T) {
	m := newTestManager(t, Config{FailureThreshold: 1, OpenTimeout: 80 * time.Millisecond, Window: time.Minute})
	ctx := context.Background()

	// One failure opens it (threshold=1).
	if st, _ := m.Record(ctx, "svc", false); st != StateOpen {
		t.Fatalf("want open, got %s", st)
	}
	if _, allowed, _ := m.Allow(ctx, "svc"); allowed {
		t.Fatal("should be denied while open")
	}

	time.Sleep(120 * time.Millisecond) // past OpenTimeout

	state, allowed, _ := m.Allow(ctx, "svc")
	if !allowed || state != StateHalfOpen {
		t.Fatalf("want half_open+allowed after timeout, got state=%s allowed=%v", state, allowed)
	}

	// A success in half-open closes it.
	if st, _ := m.Record(ctx, "svc", true); st != StateClosed {
		t.Fatalf("want closed after successful probe, got %s", st)
	}
}

// TestSuccessResetsFailures: a success before the threshold clears the counter.
func TestSuccessResetsFailures(t *testing.T) {
	m := newTestManager(t, Config{FailureThreshold: 3, OpenTimeout: time.Minute, Window: time.Minute})
	ctx := context.Background()

	_, _ = m.Record(ctx, "svc", false)
	_, _ = m.Record(ctx, "svc", false)
	_, _ = m.Record(ctx, "svc", true) // reset
	// Two more failures should NOT open it (counter was reset).
	_, _ = m.Record(ctx, "svc", false)
	if st, _ := m.Record(ctx, "svc", false); st == StateOpen {
		t.Fatal("success should have reset the failure counter")
	}
}
