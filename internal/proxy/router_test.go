package proxy

import (
	"testing"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// TestRouterLongestPrefixWins verifies the core routing rule: the most specific
// (longest) matching path prefix should win, regardless of insertion order.
func TestRouterLongestPrefixWins(t *testing.T) {
	r := NewRouter()
	r.Update([]*domain.Route{
		{ID: "broad", PathPrefix: "/v2"},
		{ID: "specific", PathPrefix: "/v2/users"},
	})

	got := r.Match("", "/v2/users/42")
	if got == nil || got.ID != "specific" {
		t.Fatalf("want route 'specific', got %+v", got)
	}

	got = r.Match("", "/v2/orders")
	if got == nil || got.ID != "broad" {
		t.Fatalf("want route 'broad', got %+v", got)
	}
}

// TestRouterHostMatching verifies host-scoped routes and the empty-host wildcard.
func TestRouterHostMatching(t *testing.T) {
	r := NewRouter()
	r.Update([]*domain.Route{
		{ID: "api", Host: "api.example.com", PathPrefix: "/"},
		{ID: "any", Host: "", PathPrefix: "/"},
	})

	if got := r.Match("api.example.com", "/x"); got == nil || got.ID != "api" {
		t.Fatalf("host-scoped route not matched, got %+v", got)
	}
	// A different host falls through to the wildcard route.
	if got := r.Match("other.com", "/x"); got == nil || got.ID != "any" {
		t.Fatalf("wildcard route not matched, got %+v", got)
	}
}

// TestRouterNoMatch verifies an unmatched request returns nil (=> 404).
func TestRouterNoMatch(t *testing.T) {
	r := NewRouter()
	r.Update([]*domain.Route{{ID: "echo", PathPrefix: "/echo"}})
	if got := r.Match("", "/nope"); got != nil {
		t.Fatalf("expected no match, got %+v", got)
	}
}
