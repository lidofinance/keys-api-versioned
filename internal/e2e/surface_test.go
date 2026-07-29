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

// The compatibility rule, as tests.
//
// `/v1` and `/v2` are the canonical lido-keys-api contract; `/ext/v1` is everything this
// service invented. The rule matters for one reason: an existing consumer must not be
// able to tell it was repointed here. So the interesting assertions are the negative
// ones — what /v1 does *not* do — and each has a positive twin on /ext/v1 proving the
// behaviour exists at all rather than having been dropped.
//
// One consequence is deliberate and worth being explicit about: /v1 ignores `limit`,
// `cursor` and `snapshot`. Silently ignoring an undeclared query parameter is what the
// canonical service does — its ValidationPipe is `{ transform: true }` with no
// `whitelist` (lido-keys-api src/main.ts) — so answering 400 here would be exactly the
// kind of difference these tests exist to prevent.

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

	// The same request with a paging parameter has to come back identical, byte for
	// byte. A canonical consumer never sends this; one of ours that does gets told.
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

	// And the positive twin: the same parameter on the extended surface pages. Without
	// this the test above would pass on a service that had simply lost paging.
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

	// A second version with different content, so "which version answered" is visible
	// in the body rather than only in the meta.
	newer := spec.WithSalt(spec.Salt + 1)
	newer.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))
	pin := fmt.Sprintf("?snapshot=%d", older.BlockNumber)

	// On the canonical surface the pin is ignored, so this is `latest` — the newer
	// version — and the verifier for the older one has to reject it.
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

	// On the extended surface the same pin resolves to the older version.
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

// TestTheErrorSurfaceMatchesCanonical pins the answers a run against the canonical
// service settled, so they survive without one running.
//
// The run is what produced them: `diff -errors` against canonical 4.0.1 on Hoodi
// (2026-07-28) started at 17 of 21 cases agreeing and ended at 21 of 21. Every
// expectation below is that service's own answer, and four of them were differences
// nothing in this repo could have caught — parity had only ever been checked on
// successful responses, which is parity of half the contract.
//
// Each case says what a consumer would have done wrong with the answer we used to give.
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

	// Every error body carries `error`, which the canonical service always sends and this
	// one silently omitted on every failure — the field was declared and never set.
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
		// Gone from the canonical surface: a path this service invented is a path the
		// canonical service could later claim with other semantics.
		//
		// What they answer instead is what a canonical consumer would get, because the
		// segment falls into the wildcard route beside it: `diff` reads as a pubkey that
		// is not one, or as a module id that is not one — both 400, from the same
		// validation the canonical service applies on its own `:pubkey` and `:module_id`
		// routes. The per-module path matches no route on either service, so it is a 404.
		//
		// So these do not 404 because they were deleted; they behave as any other bad
		// reference on that surface, which is the whole point of taking them off it.
		{"a key diff on /v1", "/v1/keys/diff" + since, http.StatusBadRequest},
		{"a module diff on /v1", "/v1/modules/diff" + since, http.StatusBadRequest},
		{"a per-module key diff on /v1", "/v1/modules/1/keys/diff" + since, http.StatusNotFound},

		// Present on the extended surface, which is what makes the three above a
		// statement about where they live rather than about them having been removed.
		{"a key diff on /ext/v1", "/ext/v1/keys/diff" + since, http.StatusOK},
		{"a module diff on /ext/v1", "/ext/v1/modules/diff" + since, http.StatusOK},
		{"a per-module key diff on /ext/v1", "/ext/v1/modules/1/keys/diff" + since, http.StatusOK},

		// The mirrored reads answer on both, including the one canonical endpoint that
		// lives on /v2 and is mirrored without its version segment.
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
