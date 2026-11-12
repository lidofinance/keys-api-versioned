package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// A naive store that records calls: the loop's decisions are under test, not SQL.

func TestCycleColdStartBuildsEveryModule(t *testing.T) {
	src := newFakeSource(2)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if !result.Rebuilt {
		t.Fatal("cold start should publish a version")
	}
	if got := len(store.published); got != 1 {
		t.Fatalf("published %d versions, want 1", got)
	}
	if got := store.keyWrites; got != 2 {
		t.Fatalf("read keys of %d modules, want both", got)
	}
}

func TestCycleUnchangedExtendsWithoutReadingKeys(t *testing.T) {
	src := newFakeSource(2)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	before := store.keyWrites
	src.advance()

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if result.Action != ActionExtend {
		t.Fatalf("action %q, want extend: %s", result.Action, result.Reason)
	}
	if store.keyWrites != before {
		t.Fatalf("extend read keys of %d modules, want none", store.keyWrites-before)
	}
	if store.extends != 1 {
		t.Fatalf("extends = %d, want 1", store.extends)
	}
}

func TestCycleNonceChangeRebuildsOnlyThatModule(t *testing.T) {
	src := newFakeSource(3)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	store.keyWrites = 0
	store.keyWriteModules = nil

	src.advance()
	src.bumpNonce(2)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if result.Action != ActionRebuild {
		t.Fatalf("action %q, want rebuild: %s", result.Action, result.Reason)
	}
	if store.keyWrites != 1 {
		t.Fatalf("read keys of %d modules, want only the changed one", store.keyWrites)
	}
	if got := store.keyWriteModules[0]; got != 2 {
		t.Fatalf("rebuilt module %d, want 2", got)
	}
	if store.staled != 1 {
		t.Fatalf("staled %d versions, want 1", store.staled)
	}
}

func TestCycleReorgRollsBackToForkPoint(t *testing.T) {
	src := newFakeSource(2)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	published := store.latest.BlockNumber

	src.advance()
	src.forkAt(published)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if result.Action != ActionSupersede {
		t.Fatalf("action %q, want supersede: %s", result.Action, result.Reason)
	}
	if store.supersededFrom != published {
		t.Fatalf("rolled back from block %d, want %d", store.supersededFrom, published)
	}
	if !result.Rebuilt {
		t.Fatal("a reorg should be followed by a rebuild")
	}
}

// A fork that changed nothing must cost no key reads (it used to be a 512 000-key mainnet read).
func TestCycleReorgWithNoChangeReadsNoKeys(t *testing.T) {
	src := newFakeSource(3)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	src.advance()
	src.bumpNonce(1)
	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("second version: %v", result.Err)
	}
	survivor := store.published[0].BlockNumber
	reorged := store.latest.BlockNumber
	keyWritesBefore := store.keyWrites

	// The nonce bump was in the dropped block, so the chain matches the surviving version again.
	src.advance()
	src.forkAt(reorged)
	src.dropNonce(1)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if result.ReorgFrom != reorged {
		t.Errorf("ReorgFrom %d, want %d", result.ReorgFrom, reorged)
	}
	if store.supersededFrom != reorged {
		t.Errorf("rolled back from %d, want %d", store.supersededFrom, reorged)
	}
	if store.latest == nil || store.latest.BlockNumber != survivor {
		t.Fatalf("latest version is %v, want the survivor at %d", store.latest, survivor)
	}
	if got := store.keyWrites - keyWritesBefore; got != 0 {
		t.Errorf("re-read keys of %d modules after a reorg that changed nothing, want 0", got)
	}
	if result.Action != ActionExtend {
		t.Errorf("action %q, want extend: %s", result.Action, result.Reason)
	}
	if !strings.Contains(result.Reason, "reorg rollback") {
		t.Errorf("reason does not mention the rollback: %s", result.Reason)
	}
}

func TestCycleReorgWithOneChangedModuleScopesTheRebuild(t *testing.T) {
	src := newFakeSource(3)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	src.advance()
	src.bumpNonce(1)
	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("second version: %v", result.Err)
	}
	reorged := store.latest.BlockNumber
	store.keyWriteModules = nil

	// Module 1's bump is gone with its block; only module 2 differs from the survivor.
	src.advance()
	src.forkAt(reorged)
	src.dropNonce(1)
	src.bumpNonce(2)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if !result.Rebuilt {
		t.Fatal("a changed module should have been rebuilt")
	}
	if len(store.keyWriteModules) != 1 || store.keyWriteModules[0] != 2 {
		t.Errorf("re-read keys of modules %v, want only module 2", store.keyWriteModules)
	}
}

// A fork below everything held must be a defined full rebuild, not an error.
func TestCycleReorgBelowRetainedWindowRebuildsFromScratch(t *testing.T) {
	src := newFakeSource(2)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	only := store.latest.BlockNumber

	src.advance()
	src.forkAt(only)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle returned an error instead of rebuilding: %v", result.Err)
	}
	if result.Action != ActionSupersede || !result.Rebuilt {
		t.Fatalf("action %q rebuilt=%t, want supersede with a rebuild: %s",
			result.Action, result.Rebuilt, result.Reason)
	}
	if len(store.keyWriteModules) < 2 {
		t.Errorf("rebuilt modules %v, want every module", store.keyWriteModules)
	}
	if store.latest == nil || store.latest.BlockNumber != src.block {
		t.Errorf("latest version is %v, want one at the new head %d", store.latest, src.block)
	}
}

func TestCycleBlockGapForcesRebuild(t *testing.T) {
	src := newFakeSource(1)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{MaxBlockGap: 10}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	for i := 0; i < 11; i++ {
		src.advance()
	}

	result := s.Cycle(context.Background())
	if result.Action != ActionRebuild {
		t.Fatalf("action %q, want rebuild past the gap: %s", result.Action, result.Reason)
	}
}

// In the combined mode the API shares the process with ingest, so a malformed source answer that
// panics must fail one cycle, not exit.
func TestCyclePanicFailsTheCycleAndTheNextOneRecovers(t *testing.T) {
	src := &panickingSource{fakeSource: newFakeSource(2), panics: 1}
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	result := s.Cycle(context.Background())
	if result.Err == nil || !result.Panicked {
		t.Fatalf("got err %v, panicked %t, want a failed cycle marked as panicked", result.Err, result.Panicked)
	}
	if len(store.published) != 0 {
		t.Fatalf("published %d versions from a panicked build, want none", len(store.published))
	}

	result = s.Cycle(context.Background())
	if result.Err != nil || result.Panicked {
		t.Fatalf("next cycle: err %v, panicked %t, want a clean rebuild", result.Err, result.Panicked)
	}
	if len(store.published) != 1 {
		t.Fatalf("published %d versions after recovering, want 1", len(store.published))
	}
}

type panickingSource struct {
	*fakeSource
	panics int
}

func (p *panickingSource) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	if p.panics > 0 {
		p.panics--
		var operators []model.Operator
		_ = operators[m.ID] // index out of range, as a malformed count would cause
	}
	return p.fakeSource.Keys(ctx, at, m, fn)
}

type fakeSource struct {
	block   int64
	branch  string
	modules []model.StakingModule
	// forks records heights whose hash no longer matches what was published.
	forks map[int64]bool
}

func newFakeSource(modules int) *fakeSource {
	f := &fakeSource{block: 1000, branch: "a", forks: map[int64]bool{}}
	for i := 1; i <= modules; i++ {
		m := model.StakingModule{
			ID:                        int32(i),
			Type:                      "curated-onchain-v1",
			Name:                      fmt.Sprintf("module-%d", i),
			Nonce:                     int32(100 + i),
			Active:                    true,
			WithdrawalCredentialsType: 1,
		}
		m.SetRawAddress(fmt.Sprintf("0x%040x", i))
		f.modules = append(f.modules, m)
	}
	return f
}

func (f *fakeSource) Name() string { return "fake" }

func (f *fakeSource) advance() { f.block++ }

func (f *fakeSource) bumpNonce(id int32) {
	for i := range f.modules {
		if f.modules[i].ID == id {
			f.modules[i].Nonce++
		}
	}
}

// dropNonce undoes a bump: the transaction was in the orphaned block.
func (f *fakeSource) dropNonce(id int32) {
	for i := range f.modules {
		if f.modules[i].ID == id {
			f.modules[i].Nonce--
		}
	}
}

func (f *fakeSource) forkAt(block int64) { f.forks[block] = true }

func (f *fakeSource) hashAt(block int64) string {
	branch := f.branch
	if f.forks[block] {
		branch = "b"
	}
	return fmt.Sprintf("0x%s%063x", branch, block)
}

func (f *fakeSource) Head(context.Context) (model.ELBlockSnapshot, error) {
	return model.ELBlockSnapshot{
		BlockNumber:          f.block,
		BlockHash:            f.hashAt(f.block),
		Timestamp:            f.block * 12,
		LastChangedBlockHash: f.hashAt(f.block),
	}, nil
}

func (f *fakeSource) BlockHashAt(_ context.Context, block int64) (string, error) {
	return f.hashAt(block), nil
}

func (f *fakeSource) Modules(context.Context, source.Ref) ([]model.StakingModule, error) {
	out := make([]model.StakingModule, len(f.modules))
	copy(out, f.modules)
	return out, nil
}

func (f *fakeSource) Operators(_ context.Context, _ source.Ref, m model.StakingModule) ([]model.Operator, error) {
	return []model.Operator{{
		Name:          "operator-0",
		RewardAddress: fmt.Sprintf("0x%040x", m.ID),
		Index:         0,
		Active:        true,
		ModuleAddress: m.RawAddress(),
	}}, nil
}

func (f *fakeSource) Keys(_ context.Context, _ source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	return fn(model.Key{
		PubkeyBin:     make([]byte, 48),
		SignatureBin:  make([]byte, 96),
		OperatorIndex: 0,
		ModuleAddress: m.RawAddress(),
		Index:         0,
	})
}

var _ source.Provider = (*fakeSource)(nil)

type fakeStore struct {
	latest    *StoredVersion
	published []StoredVersion
	nextID    int64

	keyWrites       int
	keyWriteModules []int32
	operatorWrites  []int32
	moduleWrites    []int32
	// failOperators gives a rebuild that starts and does not finish.
	failOperators  bool
	extends        int
	staled         int
	supersededFrom int64
}

func newFakeStore() *fakeStore { return &fakeStore{} }

func (s *fakeStore) Migrate(context.Context) error { return nil }
func (s *fakeStore) Close()                        {}

func (s *fakeStore) Latest(context.Context) (*StoredVersion, error) { return s.latest, nil }

func (s *fakeStore) Recent(_ context.Context, limit int) ([]StoredVersion, error) {
	out := make([]StoredVersion, 0, len(s.published))
	for i := len(s.published) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, s.published[i])
	}
	return out, nil
}

func (s *fakeStore) Begin(_ context.Context, at model.ELBlockSnapshot) (Build, error) {
	s.nextID++
	return &fakeBuild{store: s, id: s.nextID, at: at}, nil
}

// Extend moves only the confirmed-at fields, like the real store; otherwise the reorg walk probes
// the head instead of the version's own height.
func (s *fakeStore) Extend(_ context.Context, v StoredVersion, at model.ELBlockSnapshot, nonces map[string]int64, finalized int64) error {
	s.extends++
	v.HeadBlockNumber = at.BlockNumber
	v.HeadBlockHash = at.BlockHash
	v.HeadTimestamp = at.Timestamp
	v.ModuleNonces = nonces
	for i := range s.published {
		if s.published[i].ID == v.ID {
			s.published[i] = v
		}
	}
	s.latest = &v
	return nil
}

func (s *fakeStore) MarkStale(_ context.Context, v StoredVersion) error {
	s.staled++
	return nil
}

func (s *fakeStore) SupersedeFrom(_ context.Context, block int64) (Superseded, error) {
	s.supersededFrom = block

	kept := make([]StoredVersion, 0, len(s.published))
	for _, v := range s.published {
		if v.BlockNumber < block {
			kept = append(kept, v)
		}
	}
	dropped := len(s.published) - len(kept)
	s.published = kept
	if len(kept) == 0 {
		s.latest = nil
	} else {
		newest := kept[len(kept)-1]
		s.latest = &newest
	}
	return Superseded{Versions: int64(dropped)}, nil
}

func (s *fakeStore) Prune(context.Context, int) (Pruned, error) { return Pruned{}, nil }

type fakeBuild struct {
	store  *fakeStore
	id     int64
	at     model.ELBlockSnapshot
	nonces map[string]int64
}

func (b *fakeBuild) Block() int64      { return b.at.BlockNumber }
func (b *fakeBuild) BlockHash() string { return b.at.BlockHash }

func (b *fakeBuild) Keys(_ context.Context, m model.StakingModule) (ModuleKeys, error) {
	b.store.keyWrites++
	b.store.keyWriteModules = append(b.store.keyWriteModules, m.ID)
	return &fakeModuleKeys{}, nil
}

func (b *fakeBuild) Operators(_ context.Context, m model.StakingModule, _ []model.Operator) (int, error) {
	if b.store.failOperators {
		return 0, errors.New("operator write failed")
	}
	b.store.operatorWrites = append(b.store.operatorWrites, m.ID)
	return 0, nil
}

func (b *fakeBuild) Module(_ context.Context, m model.StakingModule, _ bool) error {
	if b.nonces == nil {
		b.nonces = map[string]int64{}
	}
	b.nonces[m.RawAddress()] = int64(m.Nonce)
	b.store.moduleWrites = append(b.store.moduleWrites, m.ID)
	return nil
}

func (b *fakeBuild) Publish(_ context.Context, label string, finalized int64) error {
	// Head fields start equal to the version's block, as the real INSERT does: the unchanged-head
	// short-circuit compares against HeadBlockHash.
	v := StoredVersion{
		ID:                   b.id,
		BlockNumber:          b.at.BlockNumber,
		BlockHash:            b.at.BlockHash,
		HeadBlockNumber:      b.at.BlockNumber,
		HeadBlockHash:        b.at.BlockHash,
		HeadTimestamp:        b.at.Timestamp,
		Timestamp:            b.at.Timestamp,
		LastChangedBlockHash: b.at.BlockHash,
		Label:                label,
		Status:               StatusPublished,
		FinalizedBlockNumber: finalized,
		ModuleNonces:         b.nonces,
	}
	b.store.published = append(b.store.published, v)
	b.store.latest = &v
	return nil
}

func (b *fakeBuild) Abort(context.Context) {}

type fakeModuleKeys struct{ staged int64 }

func (k *fakeModuleKeys) Add(context.Context, model.Key) error { k.staged++; return nil }
func (k *fakeModuleKeys) Commit(context.Context) (KeyDiff, error) {
	return KeyDiff{Staged: k.staged, Opened: k.staged}, nil
}
func (k *fakeModuleKeys) Abort(context.Context) {}

var _ Store = (*fakeStore)(nil)

// StatusPublished mirrors the store package's constant; importing it would be a cycle.
const StatusPublished = "published"

// A mainnet node on a Hoodi database, or a devnet reusing chain id 1, looks like a reorg. A module
// set sharing no address must refuse rather than roll back.
func TestCycleRefusesToRollBackAForeignModuleSet(t *testing.T) {
	src := newFakeSource(2)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	published := store.latest.BlockNumber

	// A different deployment: same chain id, same heights, entirely different contracts.
	for i := range src.modules {
		src.modules[i].SetRawAddress(fmt.Sprintf("0x%040x", 900+i))
	}
	src.advance()
	src.forkAt(published)

	result := s.Cycle(context.Background())
	if result.Err == nil {
		t.Fatal("the cycle rolled back a store belonging to another deployment instead of refusing")
	}
	if store.supersededFrom != 0 {
		t.Errorf("SupersedeFrom was called with %d; the rows must not be touched", store.supersededFrom)
	}
	if result.Rebuilt {
		t.Error("the cycle rebuilt over another deployment's store")
	}
	for _, want := range []string{"different chain", "PROVIDERS_URLS"} {
		if !strings.Contains(result.Err.Error(), want) {
			t.Errorf("the error does not mention %q, so an operator cannot act on it: %v", want, result.Err)
		}
	}
}

// Control: an unchanged module set still rolls back, or the guard above proves nothing.
func TestCycleStillRollsBackWhenTheModuleSetIsTheSame(t *testing.T) {
	src := newFakeSource(2)
	store := newFakeStore()
	s := NewSyncer(src, store, SyncConfig{}, nil)

	if result := s.Cycle(context.Background()); result.Err != nil {
		t.Fatalf("cold start: %v", result.Err)
	}
	published := store.latest.BlockNumber

	src.advance()
	src.forkAt(published)

	result := s.Cycle(context.Background())
	if result.Err != nil {
		t.Fatalf("cycle: %v", result.Err)
	}
	if store.supersededFrom != published {
		t.Errorf("rolled back from %d, want %d", store.supersededFrom, published)
	}
}
