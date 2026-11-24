package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// sameHeightReorg publishes a version at block 1, rolls it back, and publishes a replacement at the
// same height on another branch: what a one-block reorg followed by a rebuild at the head leaves.
func sameHeightReorg(t *testing.T, db *testpg.DB, spec testreg.Spec, between func()) (lost, replacement string) {
	t.Helper()
	ctx := context.Background()
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	at := testreg.At(db.Block(1))
	spec.WithSalt(1).Publish(ctx, t, db.Writer, at)
	if between != nil {
		between()
	}
	if _, err := db.Writer.SupersedeFrom(ctx, at.BlockNumber); err != nil {
		t.Fatalf("roll back: %v", err)
	}
	other := at
	other.BlockHash = fmt.Sprintf("0xb%063x", at.BlockNumber)
	spec.WithSalt(2).Publish(ctx, t, db.Writer, other)
	return at.BlockHash, other.BlockHash
}

// A walk whose version a reorg replaced at the same height answers 409, not pages from the other branch.
func TestAPagedWalkAcrossASameHeightReorgAnswers409(t *testing.T) {
	db := testpg.New(t)
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	var cursor string
	sameHeightReorg(t, db, spec, func() {
		resp := srv.Get(t, "/ext/v1/keys?snapshot=latest&limit=3")
		defer resp.Body.Close()
		var page struct {
			Pagination struct {
				Cursor string `json:"cursor"`
			} `json:"pagination"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("page one: status %d, err %v", resp.StatusCode, err)
		}
		cursor = page.Pagination.Cursor
	})

	resp := srv.Get(t, "/ext/v1/keys?cursor="+url.QueryEscape(cursor))
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("page two after its version was replaced at the same height: status %d, want 409", resp.StatusCode)
	}
}

// A numeric since at a height a reorg replaced answers 409; the replacement's hash still works.
func TestADiffFromAReplacedHeightAnswers409(t *testing.T) {
	db := testpg.New(t)
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	_, replacement := sameHeightReorg(t, db, spec, nil)
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	for _, path := range []string{"/ext/v1/keys/diff", "/ext/v1/modules/diff"} {
		resp := srv.Get(t, fmt.Sprintf("%s?since=%d", path, db.Block(1)))
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("%s from the replaced height: status %d, want 409", path, resp.StatusCode)
		}

		resp = srv.Get(t, path+"?since="+replacement)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s from the replacement's hash: status %d, want 200", path, resp.StatusCode)
		}
	}
}

// A rollback committed after the last read fails the response instead of a 200 mixing two versions.
func TestARollbackAfterTheLastReadFailsTheResponse(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	at := testreg.At(db.Block(1))
	spec.WithSalt(1).Publish(ctx, t, db.Writer, at)

	err := db.Reader(t, "latest", 2).Snapshot(ctx, func(snap store.Snapshot) error {
		if _, err := snap.KeysByPubkeys(ctx, testreg.ModuleAddress(2), []string{model.EncodeHex(testreg.Pubkey(2, 0, 0, 1))}); err != nil {
			return err
		}
		_, err := db.Writer.SupersedeFrom(ctx, at.BlockNumber)
		return err
	})
	var superseded store.ErrSuperseded
	if !errors.As(err, &superseded) {
		t.Fatalf("snapshot returned %v, want ErrSuperseded", err)
	}
}

// A schema change recreates the store and ids start over: the id a dump resolved now names a build at
// another block, which must not pass the re-check.
func TestAVersionIdTakenByAnotherVersionFailsTheResponse(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	err := db.Reader(t, "latest", 2).Snapshot(ctx, func(snap store.Snapshot) error {
		if _, err := snap.KeysByPubkeys(ctx, testreg.ModuleAddress(1), []string{model.EncodeHex(testreg.Pubkey(1, 0, 0, 0))}); err != nil {
			return err
		}
		_, err := db.Writer.Pool().Exec(ctx, `update versions
			set block_number = block_number + 10, block_hash = $1, status = 'building'`,
			fmt.Sprintf("0xc%063x", db.Block(10)))
		return err
	})
	var pruned store.ErrPruned
	if !errors.As(err, &pruned) {
		t.Fatalf("snapshot returned %v, want ErrPruned", err)
	}
}

// The same for a key diff: removals already read from the destination, additions from its survivor.
func TestARollbackDuringTheAdditionsFailsTheDiff(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	from := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, from)
	at := testreg.At(db.Block(1))
	spec.WithSalt(1).Publish(ctx, t, db.Writer, at)

	rolled := false
	_, err := db.Reader(t, "latest", 2).DiffKeys(ctx, from.BlockHash, "", "",
		func(kind store.DiffKind, _ *model.Key) error {
			if kind != store.DiffAdded || rolled {
				return nil
			}
			rolled = true
			_, err := db.Writer.SupersedeFrom(ctx, at.BlockNumber)
			return err
		})
	var superseded store.ErrSuperseded
	if !rolled || !errors.As(err, &superseded) {
		t.Fatalf("diff returned %v (rolled back: %v), want ErrSuperseded", err, rolled)
	}
}

// A client that hangs up after the last byte cancels the request mid re-check: the response failed
// and pgx dropped the pooled connection.
func TestAClientGoneAfterTheLastReadDoesNotFailTheResponse(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 2}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	reqCtx, hangUp := context.WithCancel(ctx)
	defer hangUp()
	err := db.Reader(t, "latest", 1).Snapshot(reqCtx, func(snap store.Snapshot) error {
		_, err := snap.KeysByPubkeys(reqCtx, testreg.ModuleAddress(1), []string{model.EncodeHex(testreg.Pubkey(1, 0, 0, 0))})
		hangUp()
		return err
	})
	if err != nil {
		t.Fatalf("snapshot returned %v after its last read, want nil", err)
	}
}

// A cursor without the version hash would resume by height, on the replacement branch after the reorg
// above: it answers 400 and tells the consumer to start again.
func TestAHashlessCursorAnswers400(t *testing.T) {
	db := testpg.New(t)
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 1, KeysPerOperator: 4}
	sameHeightReorg(t, db, spec, nil)
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	resp := srv.Get(t, fmt.Sprintf("/ext/v1/keys?cursor=%d.1.0.1", db.Block(1)))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "start the walk again") {
		t.Errorf("hashless cursor: status %d, body %s; want 400 telling the consumer to restart", resp.StatusCode, body)
	}
}
