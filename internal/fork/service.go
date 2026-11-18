//go:build fork

package fork

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
	"github.com/lidofinance/keys-api-versioned/internal/store"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
)

type Harness struct {
	Anvil    *Anvil
	DB       *testpg.DB
	Client   *eth.Client
	Provider source.Provider
	Syncer   *ingest.Syncer
	Sandbox  *Registry

	tb  testing.TB
	app *store.App
}

type HarnessOpts struct {
	// Empty means the Sandbox module alone; AllModules means every module.
	Modules []int32
	Anvil   AnvilOpts
	// Extra endpoints follow the harness node, for lagging/failing-endpoint tests.
	Extra []string
	Sync  ingest.SyncConfig
	Debug bool
	// NoOperatorWatcher is the control: without the scan, a change that moves no nonce must go unnoticed.
	NoOperatorWatcher bool
}

// AllModules is slow: module 1 holds 67 242 keys at the pinned block, read slot by slot from upstream.
var AllModules = []int32{}

func NewHarness(tb testing.TB, o HarnessOpts) *Harness {
	tb.Helper()

	// Leases and retention are per-database, and fork block numbers overlap internal/e2e's.
	testpg.Exclusive(tb)

	anvil := StartAnvil(tb, o.Anvil)
	db := testpg.New(tb)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if o.Debug {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	urls := anvil.URL
	for _, extra := range o.Extra {
		urls += "," + extra
	}

	client := eth.NewClient(urls, eth.Options{
		Concurrency: 4,
		BatchSize:   20,
		// A cold fork read pulls storage slot by slot: tens of seconds, past the 60 s default.
		Timeout: 180 * time.Second,
		// Multicall3 is inherited from Hoodi, so aggregation is under test as in production.
		Multicall: true,
		Log:       log,
	})

	core := lidocore.NewProvider(client, LocatorAddress, lidocore.Options{
		KeysBatch:       200,
		CallsPerRequest: 20,
		KeysPerRequest:  400,
		Concurrency:     4,
		Log:             log,
	})

	var provider source.Provider = core
	if modules := o.Modules; len(modules) > 0 || modules == nil {
		if modules == nil {
			modules = []int32{SandboxModuleID}
		}
		provider = scopeModules(core, modules)
	}
	if o.NoOperatorWatcher {
		provider = hideOperatorWatcher{provider}
	}

	cfg := o.Sync
	if cfg.RetainVersions == 0 {
		cfg.RetainVersions = 20
	}

	h := &Harness{
		Anvil:    anvil,
		DB:       db,
		Client:   client,
		Provider: provider,
		Syncer:   ingest.NewSyncer(provider, db.Writer, cfg, log),
		Sandbox:  NewRegistry(tb, anvil, SandboxAddress),
		tb:       tb,
	}
	return h
}

func (h *Harness) Cycle() ingest.CycleResult {
	h.tb.Helper()

	result := h.Syncer.Cycle(context.Background())
	if result.Err != nil {
		h.tb.Fatalf("cycle at head %d: %v", result.HeadBlock, result.Err)
	}
	return result
}

func (h *Harness) TryCycle() ingest.CycleResult {
	h.tb.Helper()
	return h.Syncer.Cycle(context.Background())
}

func (h *Harness) Server() *testpg.Server {
	h.tb.Helper()
	return testpg.NewServer(h.tb, h.DB.Reader(h.tb, "latest", 4))
}

func (h *Harness) reader() *store.App {
	h.tb.Helper()
	if h.app == nil {
		h.app = h.DB.Reader(h.tb, "latest", 2)
	}
	return h.app
}

// Keys reads what the writer recorded; the served bytes are internal/e2e's concern.
func (h *Harness) Keys(moduleAddress string) []model.Key {
	h.tb.Helper()

	var keys []model.Key
	err := h.reader().Snapshot(context.Background(), func(s store.Snapshot) error {
		return s.StreamKeys(context.Background(), moduleAddress, model.KeysFilter{},
			func(k *model.Key) error {
				// StreamKeys reuses the Key between calls.
				copied := *k
				copied.EnsureHex()
				keys = append(keys, copied)
				return nil
			})
	})
	if err != nil {
		h.tb.Fatalf("read keys of %s: %v", moduleAddress, err)
	}
	return keys
}

func (h *Harness) Operators(moduleAddress string) []model.Operator {
	h.tb.Helper()

	var operators []model.Operator
	err := h.reader().Snapshot(context.Background(), func(s store.Snapshot) error {
		var err error
		operators, err = s.Operators(context.Background(), moduleAddress, model.OperatorsFilter{})
		return err
	})
	if err != nil {
		h.tb.Fatalf("read operators of %s: %v", moduleAddress, err)
	}
	return operators
}

func (h *Harness) Module(id int32) model.StakingModule {
	h.tb.Helper()

	var found *model.StakingModule
	err := h.reader().Snapshot(context.Background(), func(s store.Snapshot) error {
		modules, err := s.Modules(context.Background(), nil)
		if err != nil {
			return err
		}
		for i := range modules {
			if modules[i].ID == id {
				found = &modules[i]
			}
		}
		return nil
	})
	if err != nil {
		h.tb.Fatalf("read modules: %v", err)
	}
	if found == nil {
		h.tb.Fatalf("module %d is not in the published version", id)
	}
	return *found
}

// scopedProvider is test-only: Hoodi's module 1 (67 242 keys) and CSM (467 operators) cost
// hundreds of thousands of upstream slot reads; Sandbox reaches the same semantics.
//
// It embeds the concrete provider, not the interface, so every optional capability the loop
// type-asserts (finality, tail reads, the grouped event scan, source changes) stays visible: one
// lost silently runs the fork suite on a code path production does not take.
type scopedProvider struct {
	*lidocore.Provider
	keep map[int32]bool
}

var (
	_ source.TailKeyReader          = scopedProvider{}
	_ source.FinalityReporter       = scopedProvider{}
	_ source.OperatorWatcher        = scopedProvider{}
	_ source.GroupedOperatorWatcher = scopedProvider{}
	_ source.SourceChangeReporter   = scopedProvider{}
	_ source.ChainIdentifier        = scopedProvider{}
	_ source.ChainVerifier          = scopedProvider{}
)

func scopeModules(p *lidocore.Provider, ids []int32) source.Provider {
	keep := make(map[int32]bool, len(ids))
	for _, id := range ids {
		keep[id] = true
	}
	return scopedProvider{Provider: p, keep: keep}
}

// hideOperatorWatcher embeds by field, not interface, which drops the optional methods.
type hideOperatorWatcher struct{ inner source.Provider }

func (h hideOperatorWatcher) Name() string { return h.inner.Name() }

func (h hideOperatorWatcher) Head(ctx context.Context) (model.ELBlockSnapshot, error) {
	return h.inner.Head(ctx)
}

func (h hideOperatorWatcher) BlockHashAt(ctx context.Context, block int64) (string, error) {
	return h.inner.BlockHashAt(ctx, block)
}

func (h hideOperatorWatcher) Modules(ctx context.Context, at source.Ref) ([]model.StakingModule, error) {
	return h.inner.Modules(ctx, at)
}

func (h hideOperatorWatcher) Operators(ctx context.Context, at source.Ref, m model.StakingModule) ([]model.Operator, error) {
	return h.inner.Operators(ctx, at, m)
}

func (h hideOperatorWatcher) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	return h.inner.Keys(ctx, at, m, fn)
}

func (s scopedProvider) Modules(ctx context.Context, at source.Ref) ([]model.StakingModule, error) {
	all, err := s.Provider.Modules(ctx, at)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, m := range all {
		if s.keep[m.ID] {
			out = append(out, m)
		}
	}
	return out, nil
}
