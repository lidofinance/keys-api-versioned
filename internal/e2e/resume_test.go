package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

func TestAColdSyncRetryKeepsTheKeysAFailedAttemptFinished(t *testing.T) {
	db := testpg.New(t)
	src := newSpecSource(db.Block(0))
	src.failKeys[2] = 1
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)

	if r := syncer.Cycle(context.Background()); r.Err == nil {
		t.Fatal("the first cycle should fail on module 2")
	}
	if r := syncer.Cycle(context.Background()); r.Err != nil || !r.Rebuilt {
		t.Fatalf("the retry: rebuilt %t, err %v", r.Rebuilt, r.Err)
	}

	if got := src.reads(); got != "1:1 2:2 3:1" {
		t.Errorf("key reads per module = %s, want 1:1 2:2 3:1 (module 1 kept from the failed attempt)", got)
	}
	verifyContent(t, db, src.salts)
}

func TestAFailedAttemptNoLongerOnTheChainIsReadAgain(t *testing.T) {
	db := testpg.New(t)
	src := newSpecSource(db.Block(0))
	src.failKeys[2] = 1
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)

	if r := syncer.Cycle(context.Background()); r.Err == nil {
		t.Fatal("the first cycle should fail on module 2")
	}
	src.fork(src.block)
	if r := syncer.Cycle(context.Background()); r.Err != nil || !r.Rebuilt {
		t.Fatalf("the retry: rebuilt %t, err %v", r.Rebuilt, r.Err)
	}

	if got := src.reads(); got != "1:2 2:2 3:1" {
		t.Errorf("key reads per module = %s, want 1:2 2:2 3:1 (the attempt's block was reorged out)", got)
	}
	verifyContent(t, db, src.salts)
}

// A scoped rebuild inherits modules out of scope, which must not mean a dropped branch's rows.
func TestRowsOfAnAttemptOnADroppedBranchAreNotInherited(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)
	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}

	// Modules 2 and 3 change; module 2 is written, module 3 fails, and the branch is dropped.
	src.salts[2], src.salts[3] = 1, 1
	src.failKeys[3] = 1
	if r := syncer.Cycle(ctx); r.Err == nil {
		t.Fatal("the attempt should fail on module 3")
	}
	src.fork(src.block)
	src.salts[2], src.salts[3] = 0, 0

	src.salts[1] = 1
	if r := syncer.Cycle(ctx); r.Err != nil || !r.Rebuilt {
		t.Fatalf("rebuild of module 1: rebuilt %t, err %v", r.Rebuilt, r.Err)
	}
	verifyContent(t, db, src.salts)
}

// Failed attempts leave superseded rows above the published version. They must not use up the reorg
// search and leave a version of a dropped branch served.
func TestFailedAttemptsDoNotHideAnOrphanedVersion(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)
	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	orphan := src.hash(src.block)

	src.salts[1] = 1
	src.failKeys[1] = 21
	for i := 0; i < 21; i++ {
		if r := syncer.Cycle(ctx); r.Err == nil {
			t.Fatalf("attempt %d should fail on module 1", i)
		}
	}
	src.fork(src.block - 21)
	src.salts[1] = 0
	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cycle after the reorg: %v", r.Err)
	}

	var served int
	if err := db.Writer.Pool().QueryRow(ctx, `select count(*) from versions
		where block_hash = $1 and status in ('published', 'stale')`, orphan).Scan(&served); err != nil {
		t.Fatal(err)
	}
	if served != 0 {
		t.Error("the version on the dropped branch is still served")
	}
	verifyContent(t, db, src.salts)
}

// A module registered on a branch that is then dropped, after an attempt wrote its rows, must not be
// published with the next version.
func TestAModuleOfADroppedBranchIsNotPublished(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	src.shape.Modules = 2
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)
	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}

	// Module 3 appears and is written; module 1 changes and fails.
	src.shape.Modules = 3
	src.reversed = true
	src.salts[1] = 1
	src.failKeys[1] = 1
	if r := syncer.Cycle(ctx); r.Err == nil {
		t.Fatal("the attempt should fail on module 1")
	}
	var written int
	if err := db.Writer.Pool().QueryRow(ctx, `select count(*) from key_version where module_id = 3`).Scan(&written); err != nil || written == 0 {
		t.Fatalf("the attempt wrote %d keys of module 3 (err %v); the test needs some", written, err)
	}

	// The branch that registered module 3 is dropped; module 1 changes again on the other one.
	src.fork(src.block)
	src.shape.Modules = 2
	src.salts[1] = 2
	if r := syncer.Cycle(ctx); r.Err != nil || !r.Rebuilt {
		t.Fatalf("rebuild of module 1: rebuilt %t, err %v", r.Rebuilt, r.Err)
	}

	var open int
	if err := db.Writer.Pool().QueryRow(ctx, `select
		(select count(*) from module_version where module_id = 3 and to_block is null) +
		(select count(*) from key_version where module_id = 3 and to_block is null)`).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Errorf("%d rows of module 3 are open in the published version", open)
	}
}

type specSource struct {
	mu       sync.Mutex
	shape    testreg.Spec
	salts    map[int]uint64
	block    int64
	forked   map[int64]bool
	failKeys map[int]int
	keyReads map[int]int
	// reversed lists modules from the highest id, so a failure on module 1 comes after the others.
	reversed bool
}

func newSpecSource(block int64) *specSource {
	return &specSource{
		shape: testreg.Spec{Modules: 3, OperatorsPerModule: 2, KeysPerOperator: 3},
		salts: map[int]uint64{}, block: block, forked: map[int64]bool{},
		failKeys: map[int]int{}, keyReads: map[int]int{},
	}
}

func (s *specSource) spec(id int) testreg.Spec { return s.shape.WithSalt(s.salts[id]) }

func (s *specSource) fork(block int64) { s.forked[block] = true }

func (s *specSource) hash(block int64) string {
	if s.forked[block] {
		return fmt.Sprintf("0xf%063x", block)
	}
	return testreg.HashAt(block)
}

func (s *specSource) reads() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("1:%d 2:%d 3:%d", s.keyReads[1], s.keyReads[2], s.keyReads[3])
}

func (s *specSource) Name() string { return "spec" }

func (s *specSource) Head(context.Context) (model.ELBlockSnapshot, error) {
	s.block++
	return model.ELBlockSnapshot{BlockNumber: s.block, BlockHash: s.hash(s.block), Timestamp: s.block * 12}, nil
}

func (s *specSource) BlockHashAt(_ context.Context, block int64) (string, error) {
	return s.hash(block), nil
}

func (s *specSource) Modules(context.Context, source.Ref) ([]model.StakingModule, error) {
	out := make([]model.StakingModule, 0, s.shape.Modules)
	for _, id := range s.shape.ModuleIDs() {
		out = append(out, s.spec(id).Module(id))
	}
	if s.reversed {
		slices.Reverse(out)
	}
	return out, nil
}

func (s *specSource) Operators(_ context.Context, _ source.Ref, m model.StakingModule) ([]model.Operator, error) {
	return s.spec(int(m.ID)).Operators(int(m.ID)), nil
}

func (s *specSource) Keys(_ context.Context, _ source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	id := int(m.ID)
	s.mu.Lock()
	s.keyReads[id]++
	fail := s.failKeys[id] > 0
	if fail {
		s.failKeys[id]--
	}
	s.mu.Unlock()
	if fail {
		return errors.New("endpoint down")
	}
	spec := s.spec(id)
	for op := 0; op < spec.OperatorsPerModule; op++ {
		for i := 0; i < spec.KeysPerOperator; i++ {
			if err := fn(spec.Key(id, op, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyContent(t *testing.T, db *testpg.DB, salts map[int]uint64) {
	t.Helper()
	ctx := context.Background()
	shape := newSpecSource(0).shape
	err := db.Reader(t, "latest", 2).Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range shape.ModuleIDs() {
			want := map[string]bool{}
			for op := 0; op < shape.OperatorsPerModule; op++ {
				for i := 0; i < shape.KeysPerOperator; i++ {
					want[model.EncodeHex(testreg.Pubkey(id, op, i, salts[id]))] = true
				}
			}
			got := 0
			if err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{}, func(k *model.Key) error {
				got++
				if !want[model.EncodeHex(k.PubkeyBin)] {
					identity, _ := testreg.Decode(k.PubkeyBin)
					t.Errorf("module %d serves %s, want salt %d", id, identity, salts[id])
				}
				return nil
			}); err != nil {
				return err
			}
			if got != len(want) {
				t.Errorf("module %d serves %d keys, want %d", id, got, len(want))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read the latest version: %v", err)
	}
}

// A retried audit re-reads rows an unfinished attempt already opened, so its own diff is empty; the
// version's lastChangedBlockHash must still move.
func TestARetriedAuditMovesTheRefreshSignal(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	// Every cycle is an audit: reuse is off, as in the periodic full read.
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{MaxBlockGap: 1}, nil)
	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}

	src.salts[2] = 1
	src.failKeys[3] = 1
	if r := syncer.Cycle(ctx); r.Err == nil {
		t.Fatal("the audit should fail on module 3, after module 2 is written")
	}
	r := syncer.Cycle(ctx)
	if r.Err != nil || !r.Rebuilt {
		t.Fatalf("the retry: rebuilt %t, err %v", r.Rebuilt, r.Err)
	}
	verifyContent(t, db, src.salts)

	var block, hash, lastChanged string
	if err := db.Writer.Pool().QueryRow(ctx, `select block_number::text, block_hash, last_changed_block_hash
		  from versions where status = 'published' order by block_number desc limit 1`).
		Scan(&block, &hash, &lastChanged); err != nil {
		t.Fatalf("newest version: %v", err)
	}
	if lastChanged != hash {
		t.Errorf("version %s reports lastChangedBlockHash %s, want its own hash %s: module 2 changed", block, lastChanged, hash)
	}
}

// The audit's count survives a restart: counted from the version instead, restarts more often than
// the window put the audit off for good.
func TestRestartsDoNotPutTheAuditOff(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	cfg := ingest.SyncConfig{MaxBlockGap: 4}
	if r := ingest.NewSyncer(src, db.Writer, cfg, nil).Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	for i := 1; i <= 6; i++ {
		src.salts[2] = uint64(i)
		r := ingest.NewSyncer(src, db.Writer, cfg, nil).Cycle(ctx)
		if r.Err != nil {
			t.Fatalf("restart %d: %v", i, r.Err)
		}
		if r.Audit {
			return
		}
	}
	t.Fatal("no audit across 6 restarts with a gap limit of 4")
}

// A writer that comes up with the full read overdue may have been down for long: latest answers 425
// until the full read publishes. The pod stays ready, and a version pinned by number is still served.
func TestAnOverdueFullReadOnStartWithholdsLatest(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	cfg := ingest.SyncConfig{MaxBlockGap: 4}
	if r := ingest.NewSyncer(src, db.Writer, cfg, nil).Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	version := src.block
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	src.block += 10
	src.failKeys[1] = 1
	restarted := ingest.NewSyncer(src, db.Writer, cfg, nil)
	if r := restarted.Cycle(ctx); r.Err == nil {
		t.Fatal("the full read should fail on module 1")
	}
	for path, want := range map[string]int{
		"/v1/keys":      http.StatusTooEarly,
		"/health/ready": http.StatusOK,
		fmt.Sprintf("/ext/v1/keys?snapshot=%d&limit=1", version): http.StatusOK,
	} {
		if got := statusOf(t, srv, path); got != want {
			t.Errorf("%s while the full read is pending: status %d, want %d", path, got, want)
		}
	}

	if r := restarted.Cycle(ctx); r.Err != nil || !r.Audit {
		t.Fatalf("the retry: audit %t, err %v", r.Audit, r.Err)
	}
	if got := statusOf(t, srv, "/v1/keys"); got != http.StatusOK {
		t.Errorf("/v1/keys after the full read: status %d, want 200", got)
	}
}

// A start with the full read not due lifts a hold a previous writer left.
func TestAStartWithTheFullReadNotDueLiftsTheHold(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSpecSource(db.Block(0))
	if r := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{MaxBlockGap: 4}, nil).Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	src.block += 10
	src.failKeys[1] = 1
	if r := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{MaxBlockGap: 4}, nil).Cycle(ctx); r.Err == nil {
		t.Fatal("the full read should fail on module 1")
	}
	if r := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{MaxBlockGap: 1000}, nil).Cycle(ctx); r.Err != nil {
		t.Fatalf("start with a wider window: %v", r.Err)
	}
	if got := statusOf(t, srv, "/v1/keys"); got != http.StatusOK {
		t.Errorf("/v1/keys: status %d, want 200", got)
	}
}

func statusOf(t *testing.T, srv *testpg.Server, path string) int {
	t.Helper()
	resp := srv.Get(t, path)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}
