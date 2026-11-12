package e2e

import (
	"context"
	"errors"
	"fmt"
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

type specSource struct {
	mu       sync.Mutex
	shape    testreg.Spec
	salts    map[int]uint64
	block    int64
	forked   map[int64]bool
	failKeys map[int]int
	keyReads map[int]int
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
