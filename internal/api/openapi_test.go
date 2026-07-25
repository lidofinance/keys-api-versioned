package api

import (
	"context"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// The document is hand-written, so the thing that can rot is the list of endpoints.
// These tests make that a build failure rather than a consumer's problem: a route
// added without a spec entry fails, and a spec entry whose route was removed fails.
// Descriptions are still on the author.

func TestOpenAPICoversEveryRoute(t *testing.T) {
	server := New(stubStore{}, Config{})

	specPaths := specPathsWithMethods(t)

	for _, r := range server.routes() {
		if r.undocumented {
			continue
		}
		method, path, ok := strings.Cut(r.pattern, " ")
		if !ok {
			t.Fatalf("route pattern %q is not \"METHOD /path\"", r.pattern)
		}
		if !specPaths[path+" "+strings.ToLower(method)] {
			t.Errorf("route %s is not in openapi.yaml", r.pattern)
		}
	}
}

func TestOpenAPIHasNoPathsThatAreNotRoutes(t *testing.T) {
	server := New(stubStore{}, Config{})

	routed := map[string]bool{}
	for _, r := range server.routes() {
		method, path, _ := strings.Cut(r.pattern, " ")
		routed[path+" "+strings.ToLower(method)] = true
	}

	for entry := range specPathsWithMethods(t) {
		if !routed[entry] {
			t.Errorf("openapi.yaml documents %q, which the router does not serve", entry)
		}
	}
}

func TestDocsAndSpecAreServed(t *testing.T) {
	handler := New(stubStore{}, Config{}).Handler()

	for _, tc := range []struct {
		path        string
		contentType string
		contains    string
	}{
		{"/openapi.yaml", "application/yaml", "openapi: 3.0.3"},
		{"/docs", "text/html", "/openapi.yaml"},
	} {
		req := httptest.NewRequest("GET", tc.path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != 200 {
			t.Errorf("GET %s returned %d, want 200", tc.path, rec.Code)
			continue
		}
		if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, tc.contentType) {
			t.Errorf("GET %s served %q, want %s", tc.path, got, tc.contentType)
		}
		if !strings.Contains(rec.Body.String(), tc.contains) {
			t.Errorf("GET %s does not contain %q", tc.path, tc.contains)
		}
	}
}

// specPathsWithMethods extracts "path method" pairs from the embedded document.
//
// Parsed with a regexp rather than a YAML library, deliberately: pulling in a YAML
// dependency to check a list of strings in a test would be the tail wagging the dog.
// The shape being matched is fixed by this file's own formatting.
func specPathsWithMethods(t *testing.T) map[string]bool {
	t.Helper()

	var (
		pathLine   = regexp.MustCompile(`^  (/[^ :]*):\s*$`)
		methodLine = regexp.MustCompile(`^    (get|post|put|patch|delete):\s*$`)
		out        = map[string]bool{}
		current    string
	)

	for _, line := range strings.Split(string(openAPISpec), "\n") {
		if m := pathLine.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if m := methodLine.FindStringSubmatch(line); m != nil && current != "" {
			out[current+" "+m[1]] = true
		}
	}

	if len(out) == 0 {
		t.Fatal("no paths found in openapi.yaml: the parser or the file's shape changed")
	}

	// A rough floor, so a parser that silently matches almost nothing is caught.
	if len(out) < 10 {
		keys := make([]string, 0, len(out))
		for k := range out {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Fatalf("only %d paths found in openapi.yaml, which looks like a parsing failure: %v",
			len(out), keys)
	}
	return out
}

// stubStore is enough to build the router. No handler runs in these tests.
type stubStore struct{}

func (stubStore) Snapshot(context.Context, func(store.Snapshot) error) error { return nil }
func (stubStore) Close()                                                     {}

var _ store.Store = stubStore{}

// Keep the model import meaningful: this asserts the wire type the spec describes
// still has the fields it claims, so renaming one of them breaks here too.
var _ = model.Key{Key: "", DepositSignature: "", OperatorIndex: 0, Used: false,
	ModuleAddress: "", Index: 0, Vetted: false}
