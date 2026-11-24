package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Canonical refuses paging because page two may read a rewritten table. Here each page reads the
// version its cursor names, so publishing between pages must not change the walk.
func TestAPagedWalkStaysOnOneVersionAcrossPublishes(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 4, KeysPerOperator: 50}
	start := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, start)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	verifier := testreg.NewVerifier(spec)
	path := "/ext/v1/keys?limit=100"
	pages, snapshot := 0, int64(0)

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
		// Fails if a page re-resolved `latest` instead of following its cursor.
		if snapshot == 0 {
			snapshot = body.Pagination.Snapshot
			if snapshot != start.BlockNumber {
				t.Fatalf("the walk started on version %d, want %d", snapshot, start.BlockNumber)
			}
		} else if body.Pagination.Snapshot != snapshot {
			t.Fatalf("page %d is from version %d, the walk started on %d",
				pages, body.Pagination.Snapshot, snapshot)
		}

		next := spec.WithSalt(uint64(pages))
		next.Publish(ctx, t, db.Writer, testreg.At(db.Block(int64(pages))))

		if body.Pagination.Cursor == "" {
			break
		}
		path = "/ext/v1/keys?limit=100&cursor=" + body.Pagination.Cursor
	}

	if err := verifier.Done(); err != nil {
		t.Fatalf("the walk did not cover the version exactly once: %v", err)
	}
	if want := spec.TotalKeys() / 100; pages < want {
		t.Errorf("the walk took %d pages for %d keys at 100 a page, want at least %d",
			pages, spec.TotalKeys(), want)
	}
}

// No lease spans two pages, so a pruned walk answers 410 rather than a silent gap.
func TestAPagedWalkWhoseVersionIsPrunedAnswers410(t *testing.T) {
	testpg.Exclusive(t)

	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 50}
	start := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, start)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	first := srv.Get(t, "/ext/v1/keys?limit=10")
	body, err := testreg.NewVerifier(spec).Partial().V1Keys(first.Body)
	first.Body.Close()
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if body.Pagination == nil || body.Pagination.Cursor == "" {
		t.Fatal("the first page did not offer a cursor")
	}

	next := spec.WithSalt(1)
	next.Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))
	pruned, err := db.Writer.Prune(ctx, 1)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned.Versions == 0 {
		t.Fatalf("nothing was pruned, so the walk below is not resuming into a gone version")
	}

	resumed := srv.Get(t, "/ext/v1/keys?limit=10&cursor="+body.Pagination.Cursor)
	defer resumed.Body.Close()
	if resumed.StatusCode != http.StatusGone {
		t.Fatalf("resuming a walk into a pruned version: status %d, want 410", resumed.StatusCode)
	}
}

// Records today's answer: an unknown version must stay distinguishable from a dropped one.
func TestACursorNamingAnUnknownVersion(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 5}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))

	cursor := fmt.Sprintf("%d.1.0.0.%s", db.Block(999), testreg.HashAt(db.Block(999)))
	resp := srv.Get(t, "/ext/v1/keys?limit=10&cursor="+cursor)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Fatalf("a cursor naming a version that was never published answered 200")
	}
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusGone {
		t.Errorf("status %d for a cursor naming an unpublished version, want 404 or 410", resp.StatusCode)
	}
}
