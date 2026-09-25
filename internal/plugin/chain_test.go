package plugin

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// fakePlugin records the order in which its hooks are called into a shared slice.
type fakePlugin struct {
	name     string
	priority int
	order    *[]string
	abort    bool
}

func (p *fakePlugin) Name() string  { return p.name }
func (p *fakePlugin) Priority() int { return p.priority }
func (p *fakePlugin) OnRequest(rc *domain.RequestContext) error {
	*p.order = append(*p.order, "req:"+p.name)
	if p.abort {
		rc.Abort(http.StatusTeapot, "nope")
	}
	return nil
}
func (p *fakePlugin) OnResponse(rc *domain.ResponseContext) error {
	*p.order = append(*p.order, "resp:"+p.name)
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestChainOrdering checks request phase runs low->high priority and response
// phase runs high->low (reverse), i.e. symmetric wrapping.
func TestChainOrdering(t *testing.T) {
	var order []string
	c := &Chain{
		log: testLogger(),
		plugins: []Plugin{
			&fakePlugin{name: "a", priority: 10, order: &order},
			&fakePlugin{name: "b", priority: 20, order: &order},
		},
	}

	rc := domain.NewRequestContext(&http.Request{Header: http.Header{}}, &domain.Route{})
	c.OnRequest(rc)
	_ = c.OnResponse(&domain.ResponseContext{Response: &http.Response{Header: http.Header{}}})

	want := []string{"req:a", "req:b", "resp:b", "resp:a"}
	if len(order) != len(want) {
		t.Fatalf("got %v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("at %d got %q want %q (full %v)", i, order[i], want[i], order)
		}
	}
}

// TestChainAbortStops checks that an aborting plugin prevents later request-phase
// plugins from running.
func TestChainAbortStops(t *testing.T) {
	var order []string
	c := &Chain{
		log: testLogger(),
		plugins: []Plugin{
			&fakePlugin{name: "a", priority: 10, order: &order, abort: true},
			&fakePlugin{name: "b", priority: 20, order: &order},
		},
	}

	rc := domain.NewRequestContext(&http.Request{Header: http.Header{}}, &domain.Route{})
	c.OnRequest(rc)

	if !rc.Aborted() {
		t.Fatal("expected request to be aborted")
	}
	if len(order) != 1 || order[0] != "req:a" {
		t.Fatalf("expected only plugin a to run, got %v", order)
	}
	status, _ := rc.AbortResult()
	if status != http.StatusTeapot {
		t.Fatalf("want status 418, got %d", status)
	}
}

// TestRegistryUnknownPlugin checks Build errors on an unregistered name.
func TestRegistryUnknownPlugin(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Build("ghost", nil); err == nil {
		t.Fatal("expected error for unknown plugin")
	}
}
