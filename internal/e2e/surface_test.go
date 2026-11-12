package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// /v1 and /v2 must be indistinguishable from canonical, so /v1 silently ignores limit, cursor and
// snapshot as canonical does (no ValidationPipe whitelist); each negative has an /ext/v1 twin.

func TestPagingIsIgnoredOnTheCanonicalSurfaceAndHonouredOnTheExtendedOne(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	plain := srv.Get(t, "/v1/keys")
	plainBody := readBody(t, plain)
	if plain.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", plain.StatusCode)
	}

	paged := srv.Get(t, "/v1/keys?limit=3")
	pagedBody := readBody(t, paged)
	if paged.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys?limit=3: status %d, want 200", paged.StatusCode)
	}
	if pagedBody != plainBody {
		t.Errorf("?limit=3 changed the canonical response; it has to be ignored there")
	}
	if got := paged.Header.Get("X-Ignored-Parameters"); !strings.Contains(got, "limit") {
		t.Errorf("X-Ignored-Parameters is %q, want it to name limit", got)
	}
	if got := plain.Header.Get("X-Ignored-Parameters"); got != "" {
		t.Errorf("a request that sent no extension parameters carries X-Ignored-Parameters: %q", got)
	}

	// Positive twin: without it the test above passes on a service that lost paging.
	ext := srv.Get(t, "/ext/v1/keys?limit=3")
	extBody := readBody(t, ext)
	if ext.StatusCode != http.StatusOK {
		t.Fatalf("GET /ext/v1/keys?limit=3: status %d, want 200 — body %s", ext.StatusCode, extBody)
	}
	if got := ext.Header.Get("X-Ignored-Parameters"); got != "" {
		t.Errorf("the extended surface reported ignoring %q", got)
	}

	var page struct {
		Data       []json.RawMessage `json:"data"`
		Pagination *struct {
			Returned int    `json:"returned"`
			Cursor   string `json:"cursor"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal([]byte(extBody), &page); err != nil {
		t.Fatalf("decode the page: %v — body %s", err, extBody)
	}
	if page.Pagination == nil {
		t.Fatal("the extended surface returned no pagination block")
	}
	if len(page.Data) != 3 || page.Pagination.Returned != 3 {
		t.Errorf("the page held %d keys and reports %d, want 3", len(page.Data), page.Pagination.Returned)
	}
	if page.Pagination.Cursor == "" {
		t.Error("the page offered no cursor, so the walk cannot continue")
	}
	if strings.Contains(plainBody, `"pagination"`) {
		t.Error("the canonical response carries a pagination block, which it must never do")
	}
}

func TestAPinIsIgnoredOnTheCanonicalSurfaceAndHonouredOnTheExtendedOne(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 2, KeysPerOperator: 10}
	older := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, older)

	// Different content, so which version answered shows in the body, not only the meta.
	newer := spec.WithSalt(spec.Salt + 1)
	newer.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))
	pin := fmt.Sprintf("?snapshot=%d", older.BlockNumber)

	canonical := srv.Get(t, "/v1/keys"+pin)
	defer canonical.Body.Close()
	if canonical.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys%s: status %d, want 200", pin, canonical.StatusCode)
	}
	if _, err := testreg.NewVerifier(newer).V1Keys(canonical.Body); err != nil {
		t.Errorf("the canonical surface did not answer from latest: %v", err)
	}
	if got := canonical.Header.Get("X-Ignored-Parameters"); !strings.Contains(got, "snapshot") {
		t.Errorf("X-Ignored-Parameters is %q, want it to name snapshot", got)
	}

	extended := srv.Get(t, "/ext/v1/keys"+pin)
	defer extended.Body.Close()
	if extended.StatusCode != http.StatusOK {
		t.Fatalf("GET /ext/v1/keys%s: status %d, want 200", pin, extended.StatusCode)
	}
	verifier := testreg.NewVerifier(spec)
	if _, err := verifier.V1Keys(extended.Body); err != nil {
		t.Fatalf("the extended surface did not answer from the pinned version: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatalf("the pinned answer was not the older version: %v", err)
	}
}

// Pins canonical's error answers, settled by `diff -errors` against canonical 4.0.1 on Hoodi
// (17 of 21 cases agreed at first, 21 of 21 after).
func TestTheErrorSurfaceMatchesCanonical(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	wellFormedUnknown := "0x" + strings.Repeat("ab", 48)

	for _, c := range []struct {
		name     string
		method   string
		path     string
		body     string
		want     int
		contains string
		why      string
	}{
		{
			name: "a pubkey that could never be one", method: "GET", path: "/v1/keys/not-a-pubkey",
			want: http.StatusBadRequest, contains: "must be a valid 0x-prefixed 48-byte hex string",
			why: "answered 404 before, so a consumer that retries a 404 and gives up on a 400 retried forever",
		},
		{
			name: "a well-formed pubkey nobody holds", method: "GET", path: "/v1/keys/" + wellFormedUnknown,
			want: http.StatusNotFound,
			why:  "the twin of the case above: shape is fine, the key is simply absent",
		},
		{
			name: "find with an empty list", method: "POST", path: "/v1/keys/find", body: `{"pubkeys":[]}`,
			want: http.StatusBadRequest, contains: "at least 1 elements",
			why: "answered 200 with no keys before, which reads as none of yours exist",
		},
		{
			name: "find with the wrong field name", method: "POST", path: "/v1/keys/find", body: `{"keys":["0x00"]}`,
			want: http.StatusBadRequest, contains: "pubkeys must be an array",
			why: "also answered 200 with no keys, so a typo looked like an empty result",
		},
		{
			name: "the wrong method on a canonical path", method: "POST", path: "/v1/keys",
			want: http.StatusNotFound, contains: "Cannot POST /v1/keys",
			why: "Go answers 405 and Fastify answers 404; on their surface theirs wins",
		},
		{
			name: "the wrong method on the extended surface", method: "POST", path: "/ext/v1/keys",
			want: http.StatusMethodNotAllowed,
			why:  "our surface, our answer: 405 is the correct one and there is nothing to match",
		},
	} {
		resp := srv.Do(t, c.method, c.path, c.body)
		body := readBody(t, resp)

		if resp.StatusCode != c.want {
			t.Errorf("%s (%s %s): status %d, want %d — %s\n       body %s",
				c.name, c.method, c.path, resp.StatusCode, c.want, c.why, body)
			continue
		}
		if c.contains != "" && !strings.Contains(body, c.contains) {
			t.Errorf("%s: body does not contain %q — %s", c.name, c.contains, body)
		}
	}

	// `error` was declared and never set; canonical always sends it.
	resp := srv.Get(t, "/v1/modules/abc/keys")
	body := readBody(t, resp)
	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode the error body: %v — %s", err, body)
	}
	for _, field := range []string{"message", "error", "statusCode"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("the error body has no %q field; canonical sends all three — %s", field, body)
		}
	}
	if got := decoded["error"]; got != "Bad Request" {
		t.Errorf("error is %v, want the status text \"Bad Request\"", got)
	}
}

func TestTheInventedPathsAreOnlyOnTheExtendedSurface(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 5}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)
	spec.WithSalt(1).PublishModules(ctx, t, db.Writer, testreg.At(db.Block(1)), 1)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))
	since := fmt.Sprintf("?since=%d", from.BlockNumber)

	for _, c := range []struct {
		name string
		path string
		want int
	}{
		// Off /v1, since canonical could later claim those paths. The segment falls into the wildcard route
		// beside it: canonical's 400 for a bad pubkey or module id, 404 where no route matches.
		{"a key diff on /v1", "/v1/keys/diff" + since, http.StatusBadRequest},
		{"a module diff on /v1", "/v1/modules/diff" + since, http.StatusBadRequest},
		{"a per-module key diff on /v1", "/v1/modules/1/keys/diff" + since, http.StatusNotFound},

		{"a key diff on /ext/v1", "/ext/v1/keys/diff" + since, http.StatusOK},
		{"a module diff on /ext/v1", "/ext/v1/modules/diff" + since, http.StatusOK},
		{"a per-module key diff on /ext/v1", "/ext/v1/modules/1/keys/diff" + since, http.StatusOK},

		// The one canonical /v2 endpoint is mirrored without its version segment.
		{"keys on /v1", "/v1/keys", http.StatusOK},
		{"keys on /ext/v1", "/ext/v1/keys", http.StatusOK},
		{"the combined stream on /v2", "/v2/modules/operators/keys", http.StatusOK},
		{"the combined stream mirrored", "/ext/v1/modules/operators/keys", http.StatusOK},
		{"status on /ext/v1", "/ext/v1/status", http.StatusOK},
	} {
		resp := srv.Get(t, c.path)
		body := readBody(t, resp)
		if resp.StatusCode != c.want {
			t.Errorf("%s (GET %s): status %d, want %d — body %s",
				c.name, c.path, resp.StatusCode, c.want, body)
		}
	}
}
