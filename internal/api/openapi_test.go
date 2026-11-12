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

func TestOpenAPICoversEveryRoute(t *testing.T) {
	server := New(stubStore{}, Config{})

	specPaths := specPathsWithMethods(t)

	for _, r := range server.routes() {
		if r.undocumented {
			continue
		}
		// Mirrors are covered by TestEveryCanonicalRouteHasAMirror, not an entry each.
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

// A canonical read added without a mirror fails here. /v2/modules/operators/keys must land on
// /ext/v1/modules/operators/keys, not /ext/v1/v2/... nor collide with {module_id}/operators/keys.
func TestEveryCanonicalRouteHasAMirror(t *testing.T) {
	server := New(stubStore{}, Config{})

	mirrors := map[string]bool{}
	var canonical []string
	for _, r := range server.routes() {
		switch {
		case r.mirror:
			mirrors[r.pattern] = true
		case r.undocumented, !strings.Contains(r.pattern, "/v1/"), strings.HasPrefix(r.pattern, "GET /openapi"):
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

// A path of ours on /v1 or /v2 is one the canonical service could later claim with other semantics.
func TestTheCanonicalSurfaceCarriesNoInventedPaths(t *testing.T) {
	server := New(stubStore{}, Config{})

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

// Regexp, not a YAML library: the matched shape is fixed by openapi.yaml's own formatting.
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

type stubStore struct{}

func (stubStore) Snapshot(context.Context, func(store.Snapshot) error) error { return nil }
func (stubStore) Close()                                                     {}

var _ store.Store = stubStore{}

// Renaming a field of the wire type the spec describes breaks here too.
var _ = model.Key{Key: "", DepositSignature: "", OperatorIndex: 0, Used: false,
	ModuleAddress: "", Index: 0, Vetted: false}

func TestTheDocsPageLoadsOnlyPinnedVerifiedAssets(t *testing.T) {
	tags := regexp.MustCompile(`<(?:script|link)\b[^>]*\b(?:src|href)="https?://[^"]+"[^>]*>`).FindAllString(docsPage, -1)
	if len(tags) == 0 {
		t.Fatal("found no external assets on the docs page")
	}
	for _, tag := range tags {
		if !regexp.MustCompile(`@\d+\.\d+\.\d+/`).MatchString(tag) {
			t.Errorf("not pinned to an exact version: %s", tag)
		}
		if !strings.Contains(tag, `integrity="sha384-`) || !strings.Contains(tag, `crossorigin="anonymous"`) {
			t.Errorf("no subresource integrity: %s", tag)
		}
	}
}
