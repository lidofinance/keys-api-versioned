package ingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// These tests run the loop against a fake source and an in-memory store, so the
// three transitions and the scoping rule are checked without a database or a chain.
// The store here is deliberately naive — it records calls rather than implementing
// ranges — because what is under test is the loop's decisions, not SQL.

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
	// The version being replaced is marked stale before the rebuild starts, so a
	// reader holding it knows what it has while the new one is being written.
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

	// The chain replaces the block the version was built on.
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

// fakeSource is a source.Provider with a controllable head, nonces and branch.
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

// forkAt makes the hash at a height differ from what was published there.
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

// fakeStore records what the loop asked of it.
type fakeStore struct {
	latest    *StoredVersion
	published []StoredVersion
	nextID    int64

	keyWrites       int
	keyWriteModules []int32
	extends         int
	staled          int
	supersededFrom  int64
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

func (s *fakeStore) Extend(_ context.Context, v StoredVersion, at model.ELBlockSnapshot, nonces map[string]int64) error {
	s.extends++
	v.BlockNumber = at.BlockNumber
	v.BlockHash = at.BlockHash
	v.ModuleNonces = nonces
	s.latest = &v
	return nil
}

func (s *fakeStore) MarkStale(_ context.Context, v StoredVersion) error {
	s.staled++
	return nil
}

func (s *fakeStore) SupersedeFrom(_ context.Context, block int64) (Superseded, error) {
	s.supersededFrom = block
	return Superseded{Versions: 1}, nil
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

func (b *fakeBuild) Operators(context.Context, model.StakingModule, []model.Operator) (int, error) {
	return 0, nil
}

func (b *fakeBuild) Module(_ context.Context, m model.StakingModule, _ bool) error {
	if b.nonces == nil {
		b.nonces = map[string]int64{}
	}
	b.nonces[m.RawAddress()] = int64(m.Nonce)
	return nil
}

func (b *fakeBuild) Publish(_ context.Context, label string) error {
	v := StoredVersion{
		ID:           b.id,
		BlockNumber:  b.at.BlockNumber,
		BlockHash:    b.at.BlockHash,
		Label:        label,
		Status:       StatusPublished,
		ModuleNonces: b.nonces,
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

// StatusPublished mirrors the store package's constant without importing it, which
// would be a cycle. Keeping it here also documents that the loop only ever writes
// two of the four statuses; the others are the store's business.
const StatusPublished = "published"
