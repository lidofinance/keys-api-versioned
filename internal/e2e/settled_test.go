package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Module 1 is 2 operators × 4 keys. Operator 0 has 2 deposited, operator 1 has 1.
func TestADepositReadsOnlyTheKeysAfterTheFinalizedDeposits(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newDepositSource(db.Block(0), map[[2]int]int{{1, 0}: 2, {1, 1}: 1})
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)

	if r := syncer.Cycle(ctx); r.Err != nil || r.KeysKept != 0 {
		t.Fatalf("cold sync: kept %d, err %v; want a full read", r.KeysKept, r.Err)
	}

	src.finalized = src.block
	src.deposit(1, 0)
	src.resetReads()
	r := syncer.Cycle(ctx)
	if r.Err != nil || !r.Rebuilt {
		t.Fatalf("deposit: rebuilt %t, err %v", r.Rebuilt, r.Err)
	}
	if r.KeysKept != 3 || r.KeysStaged != 5 {
		t.Errorf("kept %d, read %d; want 3 deposited keys kept and the other 5 of module 1 read", r.KeysKept, r.KeysStaged)
	}
	if got := src.readLog(); got != "1/0 from 2, 1/1 from 1" {
		t.Errorf("reads %q, want each operator of module 1 from its finalized deposited count", got)
	}
	// Key 2 of operator 0 flipped to used: one row closed, one opened.
	if r.KeysChanged != 2 {
		t.Errorf("%d rows changed, want 2", r.KeysChanged)
	}
	verifyDeposits(t, db, src)

	// The deposit above is not finalized yet, so it is read again; once it is, it is kept too.
	src.deposit(1, 1)
	src.resetReads()
	if r := syncer.Cycle(ctx); r.Err != nil || r.KeysKept != 3 {
		t.Fatalf("second deposit before finality: kept %d, err %v; want 3", r.KeysKept, r.Err)
	}
	src.finalized = src.block
	src.deposit(1, 1)
	src.resetReads()
	if r := syncer.Cycle(ctx); r.Err != nil || r.KeysKept != 5 {
		t.Fatalf("third deposit: kept %d, err %v; want 5 (3 for operator 0, 2 for operator 1)", r.KeysKept, r.Err)
	}
	verifyDeposits(t, db, src)
}

func TestDepositsAboveTheFinalizedBlockAreReadAgain(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newDepositSource(db.Block(0), map[[2]int]int{{1, 0}: 2})
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)

	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	// Finality short of the block the deposits were written at: a reorg could still undo them.
	src.finalized = src.block - 1
	src.deposit(1, 0)
	r := syncer.Cycle(ctx)
	if r.Err != nil || r.KeysKept != 0 || r.KeysStaged != 8 {
		t.Errorf("kept %d, read %d, err %v; want module 1 read in full", r.KeysKept, r.KeysStaged, r.Err)
	}
	verifyDeposits(t, db, src)
}

// The audit exists to check the rows a prefix would trust, so it trusts none.
func TestTheAuditReadsEveryKey(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newDepositSource(db.Block(0), map[[2]int]int{{1, 0}: 4, {2, 1}: 3})
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{MaxBlockGap: 3}, nil)

	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	src.finalized = src.block
	for i := 0; i < 5; i++ {
		r := syncer.Cycle(ctx)
		if r.Err != nil {
			t.Fatalf("cycle %d: %v", i, r.Err)
		}
		if !r.Audit {
			continue
		}
		if r.KeysKept != 0 || r.KeysStaged != 16 {
			t.Errorf("audit kept %d, read %d; want all 16 keys read", r.KeysKept, r.KeysStaged)
		}
		return
	}
	t.Fatal("no audit within 5 cycles of a gap limit of 3")
}

// One wrong answer for a deposited key is kept unread by every rebuild after it, so only the audit can
// correct it, and it has to come due while deposits keep moving the nonce.
func TestAWrongDepositedKeyIsCorrectedByTheAuditWhileNoncesMove(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := &lyingOnce{depositSource: newDepositSource(db.Block(0), map[[2]int]int{{1, 0}: 1}), lie: true}
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{MaxBlockGap: 3}, nil)

	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	src.lie = false

	audited := false
	for i := 0; i < 6 && !audited; i++ {
		src.finalized = src.block
		src.deposit(1, i%2)
		r := syncer.Cycle(ctx)
		if r.Err != nil {
			t.Fatalf("cycle %d: %v", i, r.Err)
		}
		if !r.Rebuilt {
			t.Fatalf("cycle %d: a deposit did not rebuild: %s", i, r.Reason)
		}
		audited = r.Audit
	}
	if !audited {
		t.Fatal("no audit in 6 deposit rebuilds with a gap limit of 3")
	}
	verifyDeposits(t, db, src.depositSource)

	// The gap now counts from the audit, so the next deposit keeps the prefix again.
	src.finalized = src.block
	src.deposit(1, 1)
	if r := syncer.Cycle(ctx); r.Err != nil || r.Audit || r.KeysKept == 0 {
		t.Errorf("the deposit after the audit: audit %t, kept %d, err %v; want a prefix-keeping rebuild", r.Audit, r.KeysKept, r.Err)
	}
}

// lyingOnce answers one deposited key wrong while lie is set.
type lyingOnce struct {
	*depositSource
	lie bool
}

func (s *lyingOnce) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	_, err := s.KeysFrom(ctx, at, m, nil, fn)
	return err
}

func (s *lyingOnce) KeysFrom(ctx context.Context, at source.Ref, m model.StakingModule, from map[int32]int32, fn func(model.Key) error) (map[int32]int32, error) {
	return s.depositSource.KeysFrom(ctx, at, m, from, func(k model.Key) error {
		if s.lie && m.ID == 1 && k.OperatorIndex == 0 && k.Index == 0 {
			wrong := append([]byte(nil), k.PubkeyBin...)
			wrong[len(wrong)-1] ^= 0xff
			k.PubkeyBin = wrong
		}
		return fn(k)
	})
}

// The store holds 3 deposited keys for operator 0; the chain at the read block has 2 (keys removed
// on a testnet, or a store from another branch). That operator is read and rewritten in full.
func TestAPrefixTheChainNoLongerHasIsReadInFull(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newDepositSource(db.Block(0), map[[2]int]int{{1, 0}: 3, {1, 1}: 1})
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)

	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	src.finalized = src.block
	src.deposited[[2]int{1, 0}] = 2
	src.resetReads()
	r := syncer.Cycle(ctx)
	if r.Err != nil {
		t.Fatalf("rebuild: %v", r.Err)
	}
	if got := src.readLog(); got != "1/0 from 0, 1/1 from 1" {
		t.Errorf("reads %q, want operator 0 in full and operator 1 after its deposited key", got)
	}
	if r.KeysKept != 1 {
		t.Errorf("kept %d, want 1 (operator 1 only)", r.KeysKept)
	}
	verifyDeposits(t, db, src)
}

// depositSource: one salt, deposits per operator. A module's nonce moves with its deposits, as
// obtainDepositData moves it on chain.
type depositSource struct {
	shape     testreg.Spec
	deposited map[[2]int]int
	block     int64
	finalized int64
	reads     []string
}

func newDepositSource(block int64, deposited map[[2]int]int) *depositSource {
	return &depositSource{
		shape:     testreg.Spec{Modules: 2, OperatorsPerModule: 2, KeysPerOperator: 4},
		deposited: deposited,
		block:     block,
	}
}

func (s *depositSource) spec() testreg.Spec {
	return s.shape.WithUsed(func(module, operator, index int) bool {
		return index < s.deposited[[2]int{module, operator}]
	})
}

func (s *depositSource) deposit(module, operator int) { s.deposited[[2]int{module, operator}]++ }

func (s *depositSource) resetReads() { s.reads = nil }

func (s *depositSource) readLog() string {
	sort.Strings(s.reads)
	return strings.Join(s.reads, ", ")
}

func (s *depositSource) Name() string { return "deposits" }

func (s *depositSource) Head(context.Context) (model.ELBlockSnapshot, error) {
	s.block++
	return model.ELBlockSnapshot{BlockNumber: s.block, BlockHash: testreg.HashAt(s.block), Timestamp: s.block * 12}, nil
}

func (s *depositSource) FinalizedBlock(context.Context) (model.ELBlockSnapshot, error) {
	if s.finalized == 0 {
		return model.ELBlockSnapshot{}, nil
	}
	return model.ELBlockSnapshot{BlockNumber: s.finalized, BlockHash: testreg.HashAt(s.finalized), Timestamp: s.finalized * 12}, nil
}

func (s *depositSource) BlockHashAt(_ context.Context, block int64) (string, error) {
	return testreg.HashAt(block), nil
}

func (s *depositSource) Modules(context.Context, source.Ref) ([]model.StakingModule, error) {
	spec := s.spec()
	out := make([]model.StakingModule, 0, spec.Modules)
	for _, id := range spec.ModuleIDs() {
		m := spec.Module(id)
		for op := 0; op < spec.OperatorsPerModule; op++ {
			m.Nonce += int32(s.deposited[[2]int{id, op}])
		}
		out = append(out, m)
	}
	return out, nil
}

func (s *depositSource) Operators(_ context.Context, _ source.Ref, m model.StakingModule) ([]model.Operator, error) {
	return s.spec().Operators(int(m.ID)), nil
}

func (s *depositSource) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	_, err := s.KeysFrom(ctx, at, m, nil, fn)
	return err
}

// KeysFrom trusts an offset only up to the deposited count, as the chain reader does.
func (s *depositSource) KeysFrom(_ context.Context, _ source.Ref, m model.StakingModule, from map[int32]int32, fn func(model.Key) error) (map[int32]int32, error) {
	id := int(m.ID)
	spec := s.spec()
	applied := map[int32]int32{}
	for op := 0; op < spec.OperatorsPerModule; op++ {
		start := int(from[int32(op)])
		if start > s.deposited[[2]int{id, op}] || start < 0 {
			start = 0
		}
		if start > 0 {
			applied[int32(op)] = int32(start)
		}
		s.reads = append(s.reads, fmt.Sprintf("%d/%d from %d", id, op, start))
		for i := start; i < spec.KeysPerOperator; i++ {
			if err := fn(spec.Key(id, op, i)); err != nil {
				return nil, err
			}
		}
	}
	return applied, nil
}

var (
	_ source.TailKeyReader    = (*depositSource)(nil)
	_ source.FinalityReporter = (*depositSource)(nil)
)

// verifyDeposits: every key served, under its own identity, with the chain's used flag.
func verifyDeposits(t *testing.T, db *testpg.DB, src *depositSource) {
	t.Helper()
	ctx := context.Background()
	spec := src.spec()
	err := db.Reader(t, "latest", 2).Snapshot(ctx, func(snap store.Snapshot) error {
		for _, id := range spec.ModuleIDs() {
			got := 0
			if err := snap.StreamKeys(ctx, testreg.ModuleAddress(id), model.KeysFilter{}, func(k *model.Key) error {
				got++
				want := spec.Key(id, int(k.OperatorIndex), int(k.Index))
				if !equalKeys(k.PubkeyBin, want.PubkeyBin) {
					t.Errorf("module %d operator %d key %d: wrong pubkey", id, k.OperatorIndex, k.Index)
				}
				if k.Used != want.Used {
					t.Errorf("module %d operator %d key %d: used %t, want %t", id, k.OperatorIndex, k.Index, k.Used, want.Used)
				}
				return nil
			}); err != nil {
				return err
			}
			if want := spec.KeysPerModule(); got != want {
				t.Errorf("module %d serves %d keys, want %d", id, got, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read the latest version: %v", err)
	}
}

func equalKeys(a, b []byte) bool { return string(a) == string(b) }
