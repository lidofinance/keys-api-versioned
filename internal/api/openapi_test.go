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
		// Mirrors are covered by the rule the document states and
		// TestEveryCanonicalRouteHasAMirror checks, not by an entry each. Fourteen
		// copies of a response body would rot faster than they would help.
		if r.mirror {
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

// TestEveryCanonicalRouteHasAMirror is what replaces documenting the extended surface
// path by path, and it checks something an enumeration could not: that the two surfaces
// stay in step. A canonical read added without a mirror fails here, which is the mistake
// that would actually happen — a consumer moving to /ext/v1 finding one endpoint missing.
//
// It also pins the shape of a mirrored path, since dropping the canonical version
// segment is the part that is easy to get wrong: /v2/modules/operators/keys has to land
// on /ext/v1/modules/operators/keys, not on /ext/v1/v2/... and not on a collision with
// /ext/v1/modules/{module_id}/operators/keys.
func TestEveryCanonicalRouteHasAMirror(t *testing.T) {
	server := New(stubStore{}, Config{})

	mirrors := map[string]bool{}
	var canonical []string
	for _, r := range server.routes() {
		switch {
		case r.mirror:
			mirrors[r.pattern] = true
		case r.undocumented, !strings.Contains(r.pattern, "/v1/"), strings.HasPrefix(r.pattern, "GET /openapi"):
			// Not part of the mirrored surface: probes, the document, and the ext-only
			// diff endpoints, which have no canonical twin by design.
		case strings.Contains(r.pattern, extPrefix):
		default:
			canonical = append(canonical, r.pattern)
		}
	}
	// The /v2 stream is canonical too and its pattern does not contain "/v1/".
	canonical = append(canonical, "GET /v2/modules/operators/keys")

	if len(canonical) < 10 {
		t.Fatalf("only %d canonical routes classified, which looks like the filter broke: %v",
			len(canonical), canonical)
	}

	for _, pattern := range canonical {
		method, path, _ := strings.Cut(pattern, " ")
		want := method + " " + mirrorPath(path)
		if !mirrors[want] {
			t.Errorf("%s has no mirror: %s is not routed", pattern, want)
		}
		delete(mirrors, want)
	}
	for leftover := range mirrors {
		t.Errorf("%s is a mirror of nothing", leftover)
	}
}

// TestTheCanonicalSurfaceCarriesNoInventedPaths states the compatibility rule as a test.
// Anything this service invented belongs under the prefix; a path of ours appearing on
// /v1 or /v2 is a path the canonical service could later claim with other semantics.
func TestTheCanonicalSurfaceCarriesNoInventedPaths(t *testing.T) {
	server := New(stubStore{}, Config{})

	// The canonical service's own surface, from its controllers: keys, modules,
	// operators, status, and the one endpoint it moved to /v2.
	canonicalPaths := map[string]bool{
		"/v1/keys": true, "/v1/keys/{pubkey}": true, "/v1/keys/find": true,
		"/v1/modules": true, "/v1/modules/keys": true, "/v1/modules/{module_id}": true,
		"/v1/modules/{module_id}/keys": true, "/v1/modules/{module_id}/keys/find": true,
		"/v1/operators": true, "/v1/modules/{module_id}/operators": true,
		"/v1/modules/{module_id}/operators/{operator_id}": true,
		"/v1/modules/{module_id}/operators/keys":          true,
		"/v2/modules/operators/keys":                      true,
		"/v1/status":                                      true,
	}

	for _, r := range server.routes() {
		if r.undocumented || r.mirror {
			continue
		}
		_, path, _ := strings.Cut(r.pattern, " ")
		if !strings.HasPrefix(path, "/v1/") && !strings.HasPrefix(path, "/v2/") {
			continue
		}
		if !canonicalPaths[path] {
			t.Errorf("%s is on the canonical surface but is not a canonical path: "+
				"anything this service invented belongs under %s", r.pattern, extPrefix)
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
