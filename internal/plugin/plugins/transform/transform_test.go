package transform

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/1morshed1/gatewayx/internal/domain"
)

func build(t *testing.T, cfg map[string]any) *Plugin {
	t.Helper()
	p, err := New()(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return p.(*Plugin)
}

// TestRequestHeadersAndPath checks header set/add/remove and path rewrite.
func TestRequestHeadersAndPath(t *testing.T) {
	p := build(t, map[string]any{
		"request": map[string]any{
			"headers": map[string]any{
				"set":    map[string]any{"X-Env": "prod"},
				"remove": []any{"X-Drop"},
			},
		},
		"rewrite_path": map[string]any{"from": "/v1", "to": "/v2"},
	})

	req := httptest.NewRequest(http.MethodGet, "http://x/v1/users", nil)
	req.Header.Set("X-Drop", "gone")
	rc := domain.NewRequestContext(req, &domain.Route{})

	if err := p.OnRequest(rc); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("X-Env"); got != "prod" {
		t.Fatalf("X-Env=%q want prod", got)
	}
	if req.Header.Get("X-Drop") != "" {
		t.Fatal("X-Drop should be removed")
	}
	if req.URL.Path != "/v2/users" {
		t.Fatalf("path=%q want /v2/users", req.URL.Path)
	}
}

// TestResponseBodyReplace checks response body find/replace and Content-Length fix.
func TestResponseBodyReplace(t *testing.T) {
	p := build(t, map[string]any{
		"response": map[string]any{
			"body_replace": []any{map[string]any{"from": "internal.svc", "to": "api.example.com"}},
		},
	})

	body := "link: http://internal.svc/x"
	resp := &http.Response{
		Header: http.Header{},
		Body:   io.NopCloser(strings.NewReader(body)),
	}
	rc := &domain.ResponseContext{Response: resp}

	if err := p.OnResponse(rc); err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	want := "link: http://api.example.com/x"
	if string(out) != want {
		t.Fatalf("body=%q want %q", out, want)
	}
	if resp.Header.Get("Content-Length") != "30" {
		t.Fatalf("content-length=%q want 30 (len of rewritten body)", resp.Header.Get("Content-Length"))
	}
}

// TestCompressedBodyUntouched checks we skip body replace on compressed responses.
func TestCompressedBodyUntouched(t *testing.T) {
	p := build(t, map[string]any{
		"response": map[string]any{
			"body_replace": []any{map[string]any{"from": "a", "to": "b"}},
		},
	})
	resp := &http.Response{
		Header: http.Header{"Content-Encoding": {"gzip"}},
		Body:   io.NopCloser(strings.NewReader("aaa")),
	}
	if err := p.OnResponse(&domain.ResponseContext{Response: resp}); err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	if string(out) != "aaa" {
		t.Fatalf("compressed body should be untouched, got %q", out)
	}
}
