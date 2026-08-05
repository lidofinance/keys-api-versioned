package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// The default version is a property of the surface, not of the process.
//
// A deployment wants /ext/v1 to serve the version a reorg cannot take away, and it wants
// /v1 to keep serving what the canonical service serves — which is the head, read at
// `getBlock('latest')` in its keys-update job. One process-wide default cannot do both,
// and getting it wrong is invisible: same paths, same framing, older keys, and no way for
// a canonical consumer to opt out, because /v1 ignores `?snapshot=` by the same
// compatibility rule that makes it the canonical surface.

func TestTheExtendedSurfaceDefaultsToFinalizedWhileTheCanonicalOneStaysOnTheHead(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}

	final := testreg.At(db.Block(10))
	spec.PublishFinal(ctx, t, db.Writer, final, final.BlockNumber)

	// Two versions above finality, each with different keys, so which version answered is
	// visible in the body rather than only in the meta.
	head := spec.WithSalt(2)
	spec.WithSalt(1).PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(11)), final.BlockNumber)
	head.PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(12)), final.BlockNumber)

	srv := testpg.NewServerWithExtPin(t, db.Reader(t, "latest", 3), "finalized")

	// The extended surface, asked for nothing in particular, serves the finalized version.
	ext := srv.Get(t, "/ext/v1/keys")
	defer ext.Body.Close()
	if ext.StatusCode != http.StatusOK {
		t.Fatalf("GET /ext/v1/keys: status %d, want 200 — body %s", ext.StatusCode, readBody(t, ext))
	}
	finalized := testreg.NewVerifier(spec)
	if _, err := finalized.V1Keys(ext.Body); err != nil {
		t.Fatalf("the extended surface did not default to the finalized version: %v", err)
	}
	if err := finalized.Done(); err != nil {
		t.Fatalf("the extended default answered from a version above the finalized block: %v", err)
	}

	// The canonical one, same store and same request, still serves the head. This is the
	// assertion the whole split exists for: a consumer repointed here sees the content the
	// canonical service would have given it.
	canonical := srv.Get(t, "/v1/keys")
	defer canonical.Body.Close()
	if canonical.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", canonical.StatusCode)
	}
	if _, err := testreg.NewVerifier(head).V1Keys(canonical.Body); err != nil {
		t.Errorf("the extended default leaked onto the canonical surface: %v", err)
	}

	// And the way out, on the surface where parameters work at all: the head is one pin
	// away. Without this the extended surface would have traded a choice for a default.
	opted := srv.Get(t, "/ext/v1/keys?snapshot=latest")
	defer opted.Body.Close()
	if opted.StatusCode != http.StatusOK {
		t.Fatalf("GET /ext/v1/keys?snapshot=latest: status %d, want 200 — body %s",
			opted.StatusCode, readBody(t, opted))
	}
	if _, err := testreg.NewVerifier(head).V1Keys(opted.Body); err != nil {
		t.Errorf("?snapshot=latest did not override the extended default: %v", err)
	}
}

// Iterating by a label is the loop a consumer writes first: build
// `?snapshot=finalized&limit=1000` once, append `&cursor=` each page. Resolving the label
// again on every page would follow finality as it advances and hand back keys from two
// versions — no gap, no error, a set that never existed on the chain. So the combination is
// refused: a cursor names the version, and pinning one twice is not a request that has an
// answer.
//
// A refusal rather than "the cursor quietly wins" for the failure timing. This way the
// loop breaks on page two of its first run, on every run, with a message saying what to
// change — instead of on the day finality happens to move during a long walk.
func TestACursorAndALabelPinTogetherAreRefused(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}

	start := testreg.At(db.Block(10))
	spec.PublishFinal(ctx, t, db.Writer, start, start.BlockNumber)

	srv := testpg.NewServerWithExtPin(t, db.Reader(t, "latest", 3), "finalized")

	// Page one pins a label, which is exactly what it is for.
	first := srv.Get(t, "/ext/v1/keys?snapshot=finalized&limit=7")
	body, err := testreg.NewVerifier(spec).V1Keys(first.Body)
	first.Body.Close()
	if err != nil {
		t.Fatalf("page one: %v", err)
	}
	if body.Pagination == nil || body.Pagination.Cursor == "" {
		t.Fatal("page one did not return a cursor to continue from")
	}
	if body.Pagination.Snapshot != start.BlockNumber {
		t.Fatalf("page one is from version %d, want the finalized version %d",
			body.Pagination.Snapshot, start.BlockNumber)
	}

	// Page two the way a consumer writes it by hand: same query string plus the cursor.
	resp := srv.Get(t, "/ext/v1/keys?snapshot=finalized&limit=7&cursor="+body.Pagination.Cursor)
	refused := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("cursor together with snapshot: status %d, want 400 — body %s",
			resp.StatusCode, refused)
	}
	// The message has to name both parameters and say which one to drop; a 400 that only
	// says "bad request" turns a one-line client fix into an investigation.
	for _, want := range []string{"cursor", "snapshot", "first page"} {
		if !strings.Contains(refused, want) {
			t.Errorf("the refusal does not mention %q: %s", want, refused)
		}
	}

	// And the walk continues with the cursor alone, on its own version, while finality
	// advances onto a version with different keys.
	next := testreg.At(db.Block(11))
	spec.WithSalt(1).PublishFinal(ctx, t, db.Writer, next, next.BlockNumber)

	second := srv.Get(t, "/ext/v1/keys?limit=7&cursor="+body.Pagination.Cursor)
	page, err := testreg.NewVerifier(spec).V1Keys(second.Body)
	second.Body.Close()
	if err != nil {
		t.Fatalf("page two by cursor: %v", err)
	}
	if page.Pagination.Snapshot != start.BlockNumber {
		t.Errorf("page two is from version %d, the walk started on %d",
			page.Pagination.Snapshot, start.BlockNumber)
	}
}

// A paged walk follows its cursor, and the surface default must not overrule it. The
// ordering is the whole risk here: applying the default to every request would re-resolve
// finality on page two and hand the consumer a walk across two versions.
func TestTheExtendedDefaultStartsAPagedWalkAndThenGetsOutOfTheWay(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}

	final := testreg.At(db.Block(10))
	spec.PublishFinal(ctx, t, db.Writer, final, final.BlockNumber)
	spec.WithSalt(1).PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(11)), final.BlockNumber)

	srv := testpg.NewServerWithExtPin(t, db.Reader(t, "latest", 3), "finalized")

	verifier := testreg.NewVerifier(spec)
	path := "/ext/v1/keys?limit=7"
	pages := 0

	for {
		resp := srv.Get(t, path)
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("page %d: status %d, want 200", pages+1, resp.StatusCode)
		}
		body, err := verifier.V1Keys(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("page %d: %v", pages+1, err)
		}
		pages++

		if body.Pagination == nil {
			t.Fatalf("page %d carried no pagination block", pages)
		}
		// Page one lands on the finalized version because that is the surface default,
		// and every later page names it because the cursor does.
		if body.Pagination.Snapshot != final.BlockNumber {
			t.Fatalf("page %d is from version %d, want the finalized version %d",
				pages, body.Pagination.Snapshot, final.BlockNumber)
		}

		// A newer version between pages, which neither the cursor nor the default may
		// follow: finality has not moved.
		spec.WithSalt(uint64(pages)+1).PublishFinal(ctx, t, db.Writer,
			testreg.At(db.Block(12+int64(pages))), final.BlockNumber)

		if body.Pagination.Cursor == "" {
			break
		}
		path = "/ext/v1/keys?limit=7&cursor=" + body.Pagination.Cursor
	}

	if err := verifier.Done(); err != nil {
		t.Fatalf("the walk did not cover the finalized version exactly once: %v", err)
	}
	if pages < 2 {
		t.Fatalf("the walk took %d page(s); with a limit of 7 it has to take several", pages)
	}
}
