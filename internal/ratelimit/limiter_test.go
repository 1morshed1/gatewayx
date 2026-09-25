package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestLimiter spins up an in-memory Redis (miniredis) so the limiter's Lua
// script runs against a real Redis command surface without a container.
func newTestLimiter(t *testing.T) (*Limiter, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client), mr
}

// TestAllowsUpToLimit verifies exactly `limit` requests pass, then the rest are
// blocked — the core correctness property.
func TestAllowsUpToLimit(t *testing.T) {
	lim, _ := newTestLimiter(t)
	ctx := context.Background()
	const limit = 3

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := lim.Allow(ctx, "k", limit, time.Minute)
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		if res.Allowed {
			allowed++
		}
	}
	if allowed != limit {
		t.Fatalf("allowed %d, want %d", allowed, limit)
	}
}

// TestWindowSlides verifies that once the window passes, capacity frees up.
func TestWindowSlides(t *testing.T) {
	lim, mr := newTestLimiter(t)
	ctx := context.Background()
	const limit = 2
	window := time.Second

	for i := 0; i < limit; i++ {
		if res, _ := lim.Allow(ctx, "k", limit, window); !res.Allowed {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	// Now blocked.
	if res, _ := lim.Allow(ctx, "k", limit, window); res.Allowed {
		t.Fatal("should be blocked at limit")
	}

	// Advance miniredis' clock past the window; old entries fall out of the set.
	mr.FastForward(2 * time.Second)

	if res, _ := lim.Allow(ctx, "k", limit, window); !res.Allowed {
		t.Fatal("should be allowed again after window slides")
	}
}

// TestRemainingReported checks the Remaining counter decrements.
func TestRemainingReported(t *testing.T) {
	lim, _ := newTestLimiter(t)
	ctx := context.Background()
	res, _ := lim.Allow(ctx, "k", 5, time.Minute)
	if !res.Allowed || res.Remaining != 4 {
		t.Fatalf("want allowed with remaining 4, got %+v", res)
	}
}
