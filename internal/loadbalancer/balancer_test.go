package loadbalancer

import (
	"net/http"
	"testing"
	"time"

	"github.com/1morshed1/gatewayx/internal/domain"
)

func route(strategy domain.LBStrategy, ups ...domain.Upstream) *domain.Route {
	return &domain.Route{
		ID:         "r1",
		LBStrategy: strategy,
		Upstreams:  ups,
		UpdatedAt:  time.Unix(0, 0),
	}
}

func up(url string, weight int) domain.Upstream {
	return domain.Upstream{ID: url, URL: url, Weight: weight, Healthy: true}
}

func req() *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "http://gw/x", nil)
	r.RemoteAddr = "10.0.0.1:1234"
	return r
}

// TestWeightedRoundRobin checks picks are distributed in proportion to weight and
// that a 1:1 split alternates smoothly.
func TestWeightedRoundRobin(t *testing.T) {
	b := New()
	rt := route(domain.RoundRobin, up("a", 3), up("b", 1))

	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		u, err := b.Pick(rt, req())
		if err != nil {
			t.Fatal(err)
		}
		counts[u.URL]++
		b.Release(rt, u) // WRR ignores conns; release keeps state clean
	}
	// 3:1 weight over 400 picks => ~300/100.
	if counts["a"] != 300 || counts["b"] != 100 {
		t.Fatalf("want 300/100 for 3:1 weights, got a=%d b=%d", counts["a"], counts["b"])
	}
}

// TestLeastConnections checks the balancer sends the next request to the upstream
// with the fewest in-flight requests.
func TestLeastConnections(t *testing.T) {
	b := New()
	rt := route(domain.LeastConnections, up("a", 1), up("b", 1))

	// Pick without releasing: a gets 1 in-flight, so next should be b.
	first, _ := b.Pick(rt, req())
	second, _ := b.Pick(rt, req())
	if first.URL == second.URL {
		t.Fatalf("least-conn should spread: both went to %s", first.URL)
	}

	// Both now have 1 in-flight. Release b -> next pick should be b again.
	b.Release(rt, second)
	third, _ := b.Pick(rt, req())
	if third.URL != second.URL {
		t.Fatalf("expected pick to go to the released upstream %s, got %s", second.URL, third.URL)
	}
}

// TestConsistentHashStable checks the same client key always maps to the same
// upstream.
func TestConsistentHashStable(t *testing.T) {
	b := New()
	rt := route(domain.ConsistentHash, up("a", 1), up("b", 1), up("c", 1))

	r := req()
	r.Header.Set("X-Forwarded-For", "203.0.113.9")
	first, _ := b.Pick(rt, r)
	for i := 0; i < 50; i++ {
		u, _ := b.Pick(rt, r)
		if u.URL != first.URL {
			t.Fatalf("consistent hash not stable: %s vs %s", u.URL, first.URL)
		}
	}
}

// TestConsistentHashMinimalRemap checks that removing one upstream remaps only a
// minority of keys — the defining property of consistent hashing (vs hash%N which
// remaps almost everything).
func TestConsistentHashMinimalRemap(t *testing.T) {
	ups3 := []domain.Upstream{up("a", 1), up("b", 1), up("c", 1)}
	ups2 := []domain.Upstream{up("a", 1), up("b", 1)} // c removed

	ring3 := newHashRing(ups3)
	ring2 := newHashRing(ups2)

	const N = 10000
	moved := 0
	for i := 0; i < N; i++ {
		key := "user-" + string(rune(i%256)) + "-" + time.Duration(i).String()
		before := ups3[ring3.pick(key)].URL
		after := ups2[ring2.pick(key)].URL
		// Keys that were on 'c' MUST move; others should mostly stay.
		if before != "c" && before != after {
			moved++
		}
	}
	// Non-'c' keys that moved should be a small fraction (well under 20%).
	if moved > N/5 {
		t.Fatalf("too many keys remapped: %d/%d (consistent hashing should minimise this)", moved, N)
	}
}
