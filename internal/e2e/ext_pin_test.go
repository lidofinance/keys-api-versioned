package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// The default version is per surface: /ext/v1 serves finalized, /v1 stays on the head like canonical.
// /v1 ignores ?snapshot=, so a wrong default there could not be escaped.

func TestTheExtendedSurfaceDefaultsToFinalizedWhileTheCanonicalOneStaysOnTheHead(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 4}

	final := testreg.At(db.Block(10))
	spec.PublishFinal(ctx, t, db.Writer, final, final.BlockNumber)

	// Different keys per version, so which version answered shows in the body.
	head := spec.WithSalt(2)
	spec.WithSalt(1).PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(11)), final.BlockNumber)
	head.PublishFinal(ctx, t, db.Writer, testreg.At(db.Block(12)), final.BlockNumber)

	srv := testpg.NewServerWithExtPin(t, db.Reader(t, "latest", 3), "finalized")

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

	// The assertion the split exists for: a repointed consumer gets canonical's content.
	canonical := srv.Get(t, "/v1/keys")
	defer canonical.Body.Close()
	if canonical.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/keys: status %d, want 200", canonical.StatusCode)
	}
	if _, err := testreg.NewVerifier(head).V1Keys(canonical.Body); err != nil {
		t.Errorf("the extended default leaked onto the canonical surface: %v", err)
	}

	// The head is one pin away; otherwise the extended surface traded a choice for a default.
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

// Re-resolving a label per page would silently mix versions, so cursor plus label is refused: the
// loop breaks on page two of every run instead of on the day finality moves mid-walk.
func TestACursorAndALabelPinTogetherAreRefused(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 10}

	start := testreg.At(db.Block(10))
	spec.PublishFinal(ctx, t, db.Writer, start, start.BlockNumber)

	srv := testpg.NewServerWithExtPin(t, db.Reader(t, "latest", 3), "finalized")

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

	resp := srv.Get(t, "/ext/v1/keys?snapshot=finalized&limit=7&cursor="+body.Pagination.Cursor)
	refused := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("cursor together with snapshot: status %d, want 400 — body %s",
			resp.StatusCode, refused)
	}
	for _, want := range []string{"cursor", "snapshot", "first page"} {
		if !strings.Contains(refused, want) {
			t.Errorf("the refusal does not mention %q: %s", want, refused)
		}
	}

	// The cursor alone continues on its own version while finality advances.
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

// Applying the surface default to every page would re-resolve finality on page two.
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
		if body.Pagination.Snapshot != final.BlockNumber {
			t.Fatalf("page %d is from version %d, want the finalized version %d",
				pages, body.Pagination.Snapshot, final.BlockNumber)
		}

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
