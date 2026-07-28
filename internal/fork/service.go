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

// Harness is the whole service over a fork: the real chain reader, the real writer,
// a real Postgres schema, and the HTTP surface on top of it.
//
// The loop's ticker is never started. Tests call Cycle when they want one, which is
// what makes "the rename landed in the block after the version" a statement a test can
// make rather than a race it has to hope for.
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

// HarnessOpts configure the service under test.
type HarnessOpts struct {
	// Modules bounds which staking modules the reader sees. Empty means the Sandbox
	// module alone; nil-but-explicit through AllModules means every one of them.
	Modules []int32
	// Anvil configures the node. The zero value forks the pinned block.
	Anvil AnvilOpts
	// Extra endpoints are appended to the provider's endpoint list after the
	// harness's own node, for the tests about a lagging or failing endpoint.
	Extra []string
	// Sync overrides the loop's configuration.
	Sync ingest.SyncConfig
	// Debug turns on the service's own debug logging, which is the only way to see
	// what a cycle decided and what it decided it from.
	Debug bool
	// NoOperatorWatcher hides source.OperatorWatcher from the loop, which is how the
	// operator-event tests get a control: with the scan gone, a change that moves no
	// nonce has to go unnoticed. Without that half, a passing event test would also
	// pass if the rebuild had been triggered by something else entirely.
	NoOperatorWatcher bool
}

// AllModules lifts the module bound, for the scenarios that are about the module set
// itself. Expect it to be slow: module 1 alone holds 67 242 keys at the pinned block,
// and a fork reads them out of upstream one storage slot at a time.
var AllModules = []int32{}

// NewHarness boots everything and tears it down with the test.
func NewHarness(tb testing.TB, o HarnessOpts) *Harness {
	tb.Helper()

	// Leases and retention are per-database rather than per-schema, and a fork's block
	// numbers land in the same space as the synthetic ones internal/e2e hands out.
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
		// A fork answers a cold read by pulling storage from upstream a slot at a
		// time, so a call that is milliseconds against a node can be tens of seconds
		// here. The default 60 s turns that into a test failure that looks like a bug.
		Timeout: 180 * time.Second,
		// Multicall3 is deployed on Hoodi and inherited by the fork, so the
		// aggregation path is the one under test, as in production.
		Multicall: true,
		Log:       log,
	})

	var provider source.Provider = lidocore.NewProvider(client, LocatorAddress, lidocore.Options{
		// Small enough that a cold span read stays inside the timeout above.
		KeysBatch:       200,
		CallsPerRequest: 20,
		KeysPerRequest:  400,
		Concurrency:     4,
		Log:             log,
	})

	if modules := o.Modules; len(modules) > 0 || modules == nil {
		if modules == nil {
			modules = []int32{SandboxModuleID}
		}
		provider = scopeModules(provider, modules)
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

// Cycle runs one polling cycle and fails the test if it errored.
func (h *Harness) Cycle() ingest.CycleResult {
	h.tb.Helper()

	result := h.Syncer.Cycle(context.Background())
	if result.Err != nil {
		h.tb.Fatalf("cycle at head %d: %v", result.HeadBlock, result.Err)
	}
	return result
}

// TryCycle runs one cycle and hands back whatever happened, for the tests where the
// failure is the finding.
func (h *Harness) TryCycle() ingest.CycleResult {
	h.tb.Helper()
	return h.Syncer.Cycle(context.Background())
}

// Server is the HTTP surface over the published versions, on its own reader pool.
func (h *Harness) Server() *testpg.Server {
	h.tb.Helper()
	return testpg.NewServer(h.tb, h.DB.Reader(h.tb, "latest", 4))
}

// reader is a store.App on its own pool, reused across a test's reads so a scenario
// does not open a pool per assertion.
func (h *Harness) reader() *store.App {
	h.tb.Helper()
	if h.app == nil {
		h.app = h.DB.Reader(h.tb, "latest", 2)
	}
	return h.app
}

// Keys reads the published latest version's keys for one module, straight out of the
// store — the served bytes are the API's business and internal/e2e already covers
// them; what a fork test needs is what the writer recorded.
func (h *Harness) Keys(moduleAddress string) []model.Key {
	h.tb.Helper()

	var keys []model.Key
	err := h.reader().Snapshot(context.Background(), func(s store.Snapshot) error {
		return s.StreamKeys(context.Background(), moduleAddress, model.KeysFilter{},
			func(k *model.Key) error {
				// StreamKeys reuses the Key between calls, so it has to be copied
				// before it is kept.
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

// Operators reads the published latest version's operators for one module.
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

// Module reads one module row of the published latest version.
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

// scopedProvider bounds which modules the service sees.
//
// This is a test-only decorator rather than a service option on purpose. The read it
// removes is enormous and entirely uninteresting to these scenarios: Hoodi's module 1
// holds 67 242 keys and CSM has 467 operators, and a fork serves the first read of
// either by pulling hundreds of thousands of storage slots out of the upstream node —
// slow, and hard enough on a shared node to draw 502s. Sandbox is 36 keys in the same
// contract family, so every semantic under test is reachable for a thousandth of the
// traffic.
//
// What it costs: these tests say nothing about a cycle handling several modules at
// once. A scenario that is about the module set widens the filter and pays for it.
type scopedProvider struct {
	source.Provider
	keep map[int32]bool
}

// scopeModules wraps a provider, keeping the optional interfaces intact — the sync
// loop type-asserts for OperatorWatcher, and a decorator that swallowed it would turn
// the operator-event scan off without saying so, which is the one path these tests
// exist to exercise.
func scopeModules(p source.Provider, ids []int32) source.Provider {
	keep := make(map[int32]bool, len(ids))
	for _, id := range ids {
		keep[id] = true
	}
	scoped := scopedProvider{Provider: p, keep: keep}

	watcher, isWatcher := p.(source.OperatorWatcher)
	chain, isChain := p.(source.ChainIdentifier)
	switch {
	case isWatcher && isChain:
		return struct {
			scopedProvider
			source.OperatorWatcher
			source.ChainIdentifier
		}{scoped, watcher, chain}
	case isWatcher:
		return struct {
			scopedProvider
			source.OperatorWatcher
		}{scoped, watcher}
	case isChain:
		return struct {
			scopedProvider
			source.ChainIdentifier
		}{scoped, chain}
	default:
		return scoped
	}
}

// hideOperatorWatcher is a provider that deliberately does not implement
// source.OperatorWatcher, so operatorEvents short-circuits at sync.go:490 and the
// cycle has only the nonce to go on. Embedding by field rather than by interface is
// what drops the optional methods.
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
