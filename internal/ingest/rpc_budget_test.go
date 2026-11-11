package ingest

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
)

// What a cycle costs in requests is the number that decides the bill on a metered
// endpoint, and until it was a counter it was arithmetic on the source — which is how
// a provider's daily quota ran out without anyone being able to say from what.
//
// The numbers below are exact, not a budget with headroom: a cycle makes whole
// requests, so there is no tolerance band that is both meaningful and quiet. Any new
// round trip fails this test, and the fix is either to remove it or to change one
// number here — which is then a line in a diff that a reviewer sees and can argue
// with. That is the entire point; it is not meant to be a hoop.
//
// Counted per method rather than as a total, because "eth_getLogs went from 2 to 4"
// says what happened and "9 != 8" does not.
//
// This is the tight guard: it fails on any new round trip, whether or not the bill still
// fits. The consequence — whether a day of it fits the provider's daily budget — is
// TestADayOfPollingFitsTheProviderQuota in rpc_quota_test.go, which projects the measured
// profile over a day and fails when it does not. Both are worth having: one added call
// often still fits, and the day it stops fitting is not the day it was added.

func TestQuietCycleRequestBudget(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)

	// The cold start reads every module's keys, so the budget it spends is a function
	// of the registry rather than of the loop. What is asserted here is the steady
	// state: the cycle after a version exists and nothing has changed.
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	if store.latest == nil {
		t.Fatal("cold start published nothing, so there is no quiet cycle to measure")
	}

	// A new head, same nonces, no operator events: the cycle the service spends
	// almost all of its life in.
	node.advance()
	node.reset()

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("quiet cycle: %v", r.Err)
	}
	if r.Action != ActionExtend {
		t.Fatalf("action %q (%s), want extend — a quiet cycle must not rebuild", r.Action, r.Reason)
	}

	want := map[string]int{
		// The head, and the finality pin — the latter only because this test leaves the
		// epoch pacing off. There is no third header read: the reorg probe is skipped
		// while the version's own block is finalized, which is where it sits almost
		// always, and no eth_getBlockByHash because the head's timestamp arrives in the
		// head header itself.
		"eth_getBlockByNumber": 2,
		// One request resolving the locator, the contract version and the module set
		// together, one carrying every getType and getNonce.
		"eth_call": 2,
		// One filter per module family, not one per module: four modules, two families.
		// Also only because the pacing is off here.
		"eth_getLogs": 2,
	}
	assertRequests(t, node, want, 6)
}

// A cycle where a module's nonce moved re-reads that module's keys, and the operator
// scan is skipped: the rebuild is already decided, so asking whether operators moved
// buys nothing. Worth pinning because the saving is invisible — nothing fails if a
// future change asks anyway.
func TestNonceChangeSkipsTheOperatorScan(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, _ := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}

	node.advance()
	node.bumpNonce(budgetModules[0].address)
	node.reset()

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle: %v", r.Err)
	}
	if r.Action != ActionRebuild {
		t.Fatalf("action %q (%s), want rebuild", r.Action, r.Reason)
	}
	if got := node.count("eth_getLogs"); got != 0 {
		t.Errorf("eth_getLogs %d, want 0: a nonce change already decided the rebuild", got)
	}
}

// Watching the locator and the contract version is free while nothing changes: both
// reads ride in the module-set request. If either ever becomes a request of its own,
// the quiet cycle grows and this says so.
func TestWatchingTheContractsCostsNothingWhileTheyHoldStill(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, _ := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}

	for i := 0; i < 3; i++ {
		node.advance()
		node.reset()
		r := syncer.Cycle(context.Background())
		if r.Err != nil {
			t.Fatalf("quiet cycle %d: %v", i, r.Err)
		}
		if r.Action != ActionExtend {
			t.Fatalf("quiet cycle %d: action %q (%s), want extend", i, r.Action, r.Reason)
		}
		if got := node.count("eth_call"); got != 2 {
			t.Fatalf("quiet cycle %d: eth_call %d, want 2 — the locator and the version travel with the module set", i, got)
		}
	}
}

// Noticing that the router moved is only half of it: the module rows in the published
// version were read from a contract the protocol no longer uses, so they have to be
// written again. The case that makes this a rule rather than a nicety is the one below —
// the replacement reports the same modules with the same nonces, so every other signal
// says nothing changed.
//
// What it does *not* do is re-read keys. Keys and operators come from the module
// contracts, whose addresses are unchanged here; only the module tuple came from the
// router. If a replacement reports different module addresses, the nonce rule notices
// that and scopes a key read to those modules — which is the same mechanism as always,
// not a special case.
func TestRouterChangeRewritesModulesWithoutRereadingKeys(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("quiet cycle: %v", r.Err)
	}
	keysBefore, modulesBefore := store.keyWrites, len(store.moduleWrites)

	node.advance()
	node.setRouter("0x00000000000000000000000000000000000000ff")

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle after the router moved: %v", r.Err)
	}
	if r.Action != ActionRebuild {
		t.Fatalf("action %q (%s), want rebuild: the module rows came from a router nobody uses", r.Action, r.Reason)
	}
	if !strings.Contains(r.Reason, "locator") {
		t.Errorf("reason %q should say the locator moved", r.Reason)
	}
	assertEveryModuleWritten(t, store, modulesBefore)
	if store.keyWrites != keysBefore {
		t.Errorf("%d modules had their keys re-read, want none: the keys are in the module contracts and did not move",
			store.keyWrites-keysBefore)
	}

	// And the reason is consumed, so one deploy is one rebuild rather than a rebuild
	// every cycle from here on.
	modulesBefore = len(store.moduleWrites)
	node.advance()
	next := syncer.Cycle(context.Background())
	if next.Err != nil {
		t.Fatalf("cycle after the rebuild: %v", next.Err)
	}
	if next.Action != ActionExtend {
		t.Fatalf("action %q (%s), want extend once the new router has been read", next.Action, next.Reason)
	}
	if len(store.moduleWrites) != modulesBefore {
		t.Errorf("%d module rows written after the rebuild, want none", len(store.moduleWrites)-modulesBefore)
	}
}

// A contract version bump changes what the same tuple means — below v4 there is no
// withdrawal credentials type — so the module rows were decoded under an assumption
// that no longer holds, and no nonce moves to say so. Rewriting them is the whole fix:
// the version says nothing about keys.
func TestContractVersionChangeRewritesModulesWithoutRereadingKeys(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	var changes [][2]int64
	client := eth.NewClient(server.URL, eth.Options{Concurrency: 4, BatchSize: 100, Multicall: false})
	provider := lidocore.NewProvider(client, budgetLocator, lidocore.Options{
		OnContractVersion: func(previous, current int64) {
			changes = append(changes, [2]int64{previous, current})
		},
	})
	store := newFakeStore()
	syncer := NewSyncer(provider, store, SyncConfig{MaxBlockGap: 1000}, nil)

	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	if len(changes) != 1 || changes[0] != [2]int64{0, routerContractVersion} {
		t.Fatalf("version reports %v, want one first read of %d", changes, routerContractVersion)
	}
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("quiet cycle: %v", r.Err)
	}
	if len(changes) != 1 {
		t.Fatalf("version reports %v, want no report while the version holds still", changes)
	}
	keysBefore, modulesBefore := store.keyWrites, len(store.moduleWrites)

	// An upgrade under a running service, with every nonce untouched.
	node.advance()
	node.setVersion(routerContractVersion+1, 1)

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle after the upgrade: %v", r.Err)
	}
	if r.Action != ActionRebuild {
		t.Fatalf("action %q (%s), want rebuild: the tuple is decoded differently now", r.Action, r.Reason)
	}
	if !strings.Contains(r.Reason, "contract version") {
		t.Errorf("reason %q should say the contract version changed", r.Reason)
	}
	if len(changes) != 2 || changes[1] != [2]int64{routerContractVersion, routerContractVersion + 1} {
		t.Fatalf("version reports %v, want the upgrade reported with its previous value", changes)
	}
	assertEveryModuleWritten(t, store, modulesBefore)
	if store.keyWrites != keysBefore {
		t.Errorf("%d modules had their keys re-read, want none: a contract version says nothing about keys",
			store.keyWrites-keysBefore)
	}

	modulesBefore = len(store.moduleWrites)
	node.advance()
	next := syncer.Cycle(context.Background())
	if next.Err != nil {
		t.Fatalf("cycle after the rebuild: %v", next.Err)
	}
	if next.Action != ActionExtend {
		t.Fatalf("action %q (%s), want extend once the new version has been read", next.Action, next.Reason)
	}
	if len(store.moduleWrites) != modulesBefore {
		t.Errorf("%d module rows written after the rebuild, want none", len(store.moduleWrites)-modulesBefore)
	}
}

// Operator events are the third depth, and the one that used to be wrong: the code
// widened the scope to every module and then read every key, against a comment on the
// same line saying keys were left alone.
func TestOperatorEventsRereadOperatorsAndNotKeys(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("quiet cycle: %v", r.Err)
	}
	keysBefore, operatorsBefore := store.keyWrites, len(store.operatorWrites)

	// A rename on a curated module: no nonce moves, the log filter sees the event.
	node.advance()
	node.emitOperatorEvent(budgetModules[0].address)

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle with operator events: %v", r.Err)
	}
	if r.Action != ActionRebuild {
		t.Fatalf("action %q (%s), want rebuild", r.Action, r.Reason)
	}
	if store.keyWrites != keysBefore {
		t.Errorf("%d modules had their keys re-read, want none: a rename is not a key change",
			store.keyWrites-keysBefore)
	}
	if got := len(store.operatorWrites) - operatorsBefore; got != len(budgetModules) {
		t.Errorf("%d modules had their operators re-read, want all %d: names live off the nonce, so which module changed is unknown",
			got, len(budgetModules))
	}
}

// A CMv2 rename is an event at the module's MetaRegistry, not at the module. The scan
// carries the registry in the community filter, so it costs no extra request and is
// picked up within a cycle, as in the canonical service.
func TestACMv2RenameAtTheMetaRegistryRereadsOperators(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	operatorsBefore := len(store.operatorWrites)

	node.advance()
	node.emitOperatorEvent(budgetMetaRegistry)
	node.reset()

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle with a MetaRegistry event: %v", r.Err)
	}
	if r.Action != ActionRebuild {
		t.Fatalf("action %q (%s), want rebuild", r.Action, r.Reason)
	}
	if got := len(store.operatorWrites) - operatorsBefore; got != len(budgetModules) {
		t.Errorf("%d modules had their operators re-read, want %d", got, len(budgetModules))
	}
	if got := node.count("eth_getLogs"); got != 2 {
		t.Errorf("eth_getLogs %d, want 2: the registry rides in the community filter", got)
	}
}

// The scan range used to start at the published version's own block, and a version
// that nothing changed is never rewritten — so the window grew by a block every cycle
// and the same blocks were re-read five times a minute until the periodic audit reset
// it. With a mark, each cycle asks only about the blocks that arrived since the last
// one said there was nothing.
func TestOperatorScanOnlyCoversNewBlocks(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, _ := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}

	// Four quiet cycles, each one block further along. Every scan should cover exactly
	// the block that arrived, and the window must not widen.
	for i := 0; i < 4; i++ {
		node.advance()
		node.reset()
		r := syncer.Cycle(context.Background())
		if r.Err != nil {
			t.Fatalf("cycle %d: %v", i, r.Err)
		}
		if r.Action != ActionExtend {
			t.Fatalf("cycle %d: action %q (%s), want extend", i, r.Action, r.Reason)
		}

		want := [2]int64{r.HeadBlock, r.HeadBlock}
		for _, scan := range node.scanned() {
			if scan != want {
				t.Fatalf("cycle %d scanned blocks %d..%d, want just the new block %d",
					i, scan[0], scan[1], r.HeadBlock)
			}
		}
	}
}

// A scan that found something must not move the mark. The signal is discharged by a
// rebuild that re-reads operators, and a rebuild can fail — advancing the mark first
// would answer "no events" next cycle to a question nothing had acted on, and the
// rename would surface hours later in the periodic audit.
func TestAFoundEventSurvivesAFailedRebuild(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("quiet cycle: %v", r.Err)
	}
	versionBlock := store.latest.BlockNumber

	// An event arrives, and the rebuild it triggers fails.
	store.failOperators = true
	node.advance()
	node.emitOperatorEvent(budgetModules[0].address)

	r := syncer.Cycle(context.Background())
	if r.Err == nil {
		t.Fatal("want the rebuild to fail, so that the next cycle has to rescan")
	}
	failedAt := r.HeadBlock

	// The event is gone from the chain's answer, so the only way the next cycle can
	// still see it is by asking about the blocks it was in — which is what the mark
	// staying put is for.
	store.failOperators = false
	node.clearOperatorEvents()
	node.advance()
	node.reset()

	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cycle after the failure: %v", r.Err)
	}
	scans := node.scanned()
	if len(scans) == 0 {
		t.Fatal("no scan at all after a failed rebuild")
	}
	for _, scan := range scans {
		if scan[0] > failedAt {
			t.Errorf("scanned %d..%d, want the range to still start at or below %d — the block the failed rebuild was for",
				scan[0], scan[1], failedAt)
		}
		if scan[0] > versionBlock+1 && scan[0] > failedAt {
			t.Errorf("scanned %d..%d, want it to reach back to the published version at %d",
				scan[0], scan[1], versionBlock)
		}
	}
}

// The number a metered provider bills is the JSON-RPC call, not the round trip, and a
// quiet cycle sends eleven of the former inside two of the latter. Aggregating them
// takes it to two — which is the difference between 2 000 000 compute units lasting five
// hours and lasting days.
//
// Asserted as billed calls rather than as compute units on purpose: CU per method is the
// provider's pricing table and not ours to encode. The call count is what we control and
// what any provider's table multiplies.
func TestMeteredCycleBillsTwoCallsInsteadOfEleven(t *testing.T) {
	for _, tc := range []struct {
		name    string
		metered bool
		want    int
	}{
		{"own node: one eth_call per read, packed into batches", false, 11},
		{"metered: one aggregate per dependent step", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newBudgetNode()
			node.multicall = true
			server := httptest.NewServer(node)
			defer server.Close()

			client := eth.NewClient(server.URL, eth.Options{
				Concurrency: 4, BatchSize: 100,
				Multicall: true, Metered: tc.metered,
			})
			provider := lidocore.NewProvider(client, budgetLocator, lidocore.Options{})
			store := newFakeStore()
			syncer := NewSyncer(provider, store, SyncConfig{MaxBlockGap: 1000}, nil)

			if r := syncer.Cycle(context.Background()); r.Err != nil {
				t.Fatalf("cold start: %v", r.Err)
			}
			node.advance()
			node.reset()

			r := syncer.Cycle(context.Background())
			if r.Err != nil {
				t.Fatalf("quiet cycle: %v", r.Err)
			}
			if r.Action != ActionExtend {
				t.Fatalf("action %q (%s), want extend", r.Action, r.Reason)
			}

			// Two dependent steps, and no way to make it one: the module addresses the
			// second reads from come out of the first.
			if got := node.billedCalls("eth_call"); got != tc.want {
				t.Errorf("a quiet cycle billed %d eth_call, want %d", got, tc.want)
			}
		})
	}
}

// A poll that finds the same head it found last time is the common case whenever the
// interval is at or below the slot time, and it can be answered from one request: no new
// block means no new state, so every signal below would be read at a point already
// accounted for. This is what makes a 12-second interval — one slot — affordable, and it
// is the canonical service's own trick, which is how it affords a 5-second default.
func TestAnUnchangedHeadCostsOneRequest(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncerOver(t, server.URL, newFakeStore())
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	extendsBefore := store.extends

	// The chain has not produced a block: same head, three polls.
	node.reset()
	for i := 0; i < 3; i++ {
		r := syncer.Cycle(context.Background())
		if r.Err != nil {
			t.Fatalf("poll %d: %v", i, r.Err)
		}
		if r.Action != ActionExtend {
			t.Fatalf("poll %d: action %q (%s), want extend", i, r.Action, r.Reason)
		}
		if !strings.Contains(r.Reason, "still") {
			t.Errorf("poll %d: reason %q should say the head has not moved", i, r.Reason)
		}
	}

	counts := node.counts()
	if counts["eth_getBlockByNumber"] != 3 {
		t.Errorf("%d head reads for three polls, want 3", counts["eth_getBlockByNumber"])
	}
	for method, n := range counts {
		if method != "eth_getBlockByNumber" {
			t.Errorf("%s was called %d times; an unchanged head should read nothing else", method, n)
		}
	}
	// And nothing is written: extending a version to a head it is already recorded at
	// would be a write that says nothing.
	if store.extends != extendsBefore {
		t.Errorf("%d version writes on an unchanged head, want none", store.extends-extendsBefore)
	}

	// A new block, and the full set of cheap signals comes back.
	node.advance()
	node.reset()
	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle after a new block: %v", r.Err)
	}
	if got := node.counts()["eth_call"]; got == 0 {
		t.Error("a moved head should read the module set and nonces")
	}
	if store.extends == extendsBefore {
		t.Error("a moved head should extend the version")
	}
}

// The operator scan and the finalized read are paced by the epoch, because that is how
// fast their subjects move: finality advances once per epoch by definition, and an
// operator rename is a months-apart event. Running them on the cycle's clock — five times
// a minute — spent most of a metered budget re-asking questions whose answer could not
// have changed.
//
// Because a compute-unit provider bills the call and not the block range, scanning once
// per epoch over a range 32 slots wide costs exactly what scanning every cycle over one
// slot did, per scan — so this is a straight division of the bill, not a trade.
func TestEpochPacedReadsHappenOncePerEpoch(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	epoch := 384 * time.Second // mainnet: 12 s × 32
	syncer, _ := budgetSyncerOver(t, server.URL, newFakeStore())
	syncer.cfg.EpochDuration = epoch

	clock := time.Unix(1_700_000_000, 0)
	syncer.now = func() time.Time { return clock }

	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}

	// One cycle to take the first scan, which a fresh process always does: it knows
	// nothing yet and waiting an epoch to find out would be worse than the cost.
	clock = clock.Add(12 * time.Second)
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("first cycle: %v", r.Err)
	}
	if len(node.scanned()) == 0 {
		t.Fatal("a fresh process should scan on its first cycle rather than wait an epoch")
	}

	// Thirty further cycles inside the same epoch. None of them should scan, and none
	// should re-read the finalized block.
	node.reset()
	for i := 0; i < 30; i++ {
		clock = clock.Add(12 * time.Second)
		node.advance()
		if r := syncer.Cycle(context.Background()); r.Err != nil {
			t.Fatalf("cycle %d: %v", i, r.Err)
		}
	}
	if got := len(node.scanned()); got != 0 {
		t.Errorf("%d operator scans inside one epoch, want none", got)
	}
	if got := node.finalizedReads(); got != 0 {
		t.Errorf("%d finalized reads inside one epoch, want none", got)
	}

	// Crossing the epoch boundary: one scan of each family, over everything that
	// accumulated rather than only the newest block.
	node.reset()
	clock = clock.Add(2 * 12 * time.Second)
	node.advance()
	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle at the epoch boundary: %v", r.Err)
	}
	if r.Action != ActionExtend {
		t.Fatalf("action %q (%s), want extend", r.Action, r.Reason)
	}
	scans := node.scanned()
	if len(scans) != 2 {
		t.Fatalf("%d log filters after a full epoch, want 2 — one per module family", len(scans))
	}
	for _, scan := range scans {
		if width := scan[1] - scan[0]; width < 30 {
			t.Errorf("scanned %d..%d — %d blocks; want the whole epoch's worth accumulated",
				scan[0], scan[1], width+1)
		}
	}
	if got := node.finalizedReads(); got != 1 {
		t.Errorf("%d finalized reads at the epoch boundary, want 1", got)
	}
}

// With the pacing off — a devnet that sets either constant to zero — both reads happen
// every cycle, which is what they did before the pacing existed.
func TestZeroEpochKeepsThePerCycleReads(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, _ := budgetSyncerOver(t, server.URL, newFakeStore())
	syncer.cfg.EpochDuration = 0

	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	node.advance()
	node.reset()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("quiet cycle: %v", r.Err)
	}
	if len(node.scanned()) == 0 {
		t.Error("no operator scan with the pacing disabled")
	}
	if node.finalizedReads() != 1 {
		t.Errorf("%d finalized reads, want 1 per cycle with the pacing disabled", node.finalizedReads())
	}
}

// A restart is not a cold start. The question is asked of the database — one query for
// the newest published version, whose row carries the per-module nonces the last cycle
// observed — so a process that comes back up compares those against the chain and reads
// no keys at all unless something actually moved. Worth pinning because the alternative
// is invisible: re-reading 500 000 keys on every deploy would look like a slow start
// rather than like a bug.
func TestRestartWithAPopulatedStoreReadsNoKeys(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	if store.keyWrites != len(budgetModules) {
		t.Fatalf("cold start read %d modules' keys, want all %d", store.keyWrites, len(budgetModules))
	}
	keysBefore := store.keyWrites

	// The restart: a new Syncer and a new provider — no in-memory state survives — over
	// the same store. Everything this cycle knows about the past comes from the database.
	node.advance()
	node.reset()
	restarted, _ := budgetSyncerOver(t, server.URL, store)

	r := restarted.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("first cycle after the restart: %v", r.Err)
	}
	if r.Action != ActionExtend {
		t.Fatalf("action %q (%s), want extend: nothing changed while the process was down", r.Action, r.Reason)
	}
	if store.keyWrites != keysBefore {
		t.Errorf("%d modules had their keys re-read after a restart, want none", store.keyWrites-keysBefore)
	}

	// What a restart does cost, and it is worth stating rather than leaving to be
	// discovered: one extra eth_call, because the staking router address is resolved
	// before it can be verified against anything; one more for CMv2's MetaRegistry,
	// which the event scan watches and no operator read has resolved yet; and a wide
	// event scan, because the scan mark is in memory and starts empty again.
	if got := node.count("eth_call"); got != 4 {
		t.Errorf("eth_call %d, want 4: a quiet cycle's 2 plus the router and the MetaRegistry resolved cold", got)
	}
	for _, scan := range node.scanned() {
		if scan[0] != store.latest.BlockNumber+1 {
			t.Errorf("scanned %d..%d, want the range to restart at the published version's block %d",
				scan[0], scan[1], store.latest.BlockNumber+1)
		}
	}

	// And the cycle after that is an ordinary quiet cycle again.
	node.advance()
	node.reset()
	if r := restarted.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("second cycle after the restart: %v", r.Err)
	}
	if got := node.count("eth_call"); got != 2 {
		t.Errorf("eth_call %d, want 2: the router is cached from here on", got)
	}
}

// A crash *during* the cold sync is the case that costs real money, and it is not
// resumable: an unfinished version stays `building`, Latest only considers published and
// stale ones, so the next start finds nothing and reads the whole registry again. A pod
// that keeps dying before its first version lands therefore re-reads 500 000 keys every
// time it comes up, which is how a provider's daily quota goes in an afternoon.
//
// Pinned rather than fixed. Resuming means deciding what a half-read version at a block
// the chain has moved past is worth, and the answer is probably nothing — but the cost
// of not resuming should be a measured statement rather than a surprise.
func TestACrashDuringTheColdSyncStartsOver(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	store := newFakeStore()
	syncer, _ := budgetSyncerOver(t, server.URL, store)

	// The first cold sync dies partway through.
	store.failOperators = true
	r := syncer.Cycle(context.Background())
	if r.Err == nil {
		t.Fatal("want the cold sync to fail")
	}
	if store.latest != nil {
		t.Fatal("an unfinished version must not be served")
	}
	keysAfterFailure := store.keyWrites
	if keysAfterFailure == 0 {
		t.Fatal("the failed sync should have read at least one module before dying")
	}

	// It comes back up. Nothing is inherited: every module's keys are read again.
	store.failOperators = false
	node.advance()
	restarted, _ := budgetSyncerOver(t, server.URL, store)

	next := restarted.Cycle(context.Background())
	if next.Err != nil {
		t.Fatalf("cold sync after the crash: %v", next.Err)
	}
	if next.Reason != "no published version" {
		t.Errorf("reason %q, want a cold start: a building version is not resumed", next.Reason)
	}
	if got := store.keyWrites - keysAfterFailure; got != len(budgetModules) {
		t.Errorf("%d modules had their keys read, want all %d read from scratch", got, len(budgetModules))
	}
}

// The one case where a restart does re-read everything, and it is the periodic audit
// rather than the restart: a published version whose own block is MAX_BLOCKS_OVERLAP
// behind head. Downtime longer than that window puts any deployment here, which is the
// thing to know before setting the window narrow.
func TestRestartAfterLongDowntimeRunsTheAudit(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncerOverWithGap(t, server.URL, newFakeStore(), 100)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	keysBefore := store.keyWrites

	// Down for longer than the window.
	for i := 0; i < 100; i++ {
		node.advance()
	}
	restarted, _ := budgetSyncerOverWithGap(t, server.URL, store, 100)

	r := restarted.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("first cycle after a long gap: %v", r.Err)
	}
	if r.Action != ActionRebuild {
		t.Fatalf("action %q (%s), want rebuild", r.Action, r.Reason)
	}
	if !strings.Contains(r.Reason, "block gap") {
		t.Errorf("reason %q should name the block gap, not something else", r.Reason)
	}
	// Flagged as an audit, which is what lets the metric separate "re-read everything
	// on a timer" from "re-read everything because a signal fired". Paired with
	// KeysChanged it is the only check on the nonce assumption.
	if !r.Audit {
		t.Error("a block-gap rebuild should be marked as the audit")
	}
	if got := store.keyWrites - keysBefore; got != len(budgetModules) {
		t.Errorf("%d modules had their keys re-read, want all %d: the audit trusts no signal", got, len(budgetModules))
	}
}

// assertEveryModuleWritten checks that the rebuild was not scoped to a subset: a
// provenance change invalidates every module row, and a scope would leave the modules
// whose nonce happened not to move holding rows decoded the old way.
func assertEveryModuleWritten(t *testing.T, store *fakeStore, before int) {
	t.Helper()

	written := map[int32]bool{}
	for _, id := range store.moduleWrites[before:] {
		written[id] = true
	}
	for _, m := range budgetModules {
		if !written[int32(m.id)] {
			t.Errorf("module %d was not written again", m.id)
		}
	}
}

// Below v4 the module tuple has no withdrawal credentials type, and 1 is implied. The
// decision follows the version the router reported rather than whether the ABI this
// binary was built with declares the field: the ABI is a property of the build, the
// tuple is a property of the chain.
func TestWithdrawalCredentialsTypeFollowsTheContractVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int64
		onChain uint8
		want    int32
	}{
		{"pre-v4 ignores whatever the field holds", 3, 2, 1},
		{"v4 with the field unset implies 1", 4, 0, 1},
		{"v4 reports what the contract says", 4, 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newBudgetNode()
			node.setVersion(tc.version, tc.onChain)
			server := httptest.NewServer(node)
			defer server.Close()

			client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 100, Multicall: false})
			provider := lidocore.NewProvider(client, budgetLocator, lidocore.Options{})

			modules, err := provider.Modules(context.Background(), source.Ref{BlockNumber: 1000})
			if err != nil {
				t.Fatalf("Modules: %v", err)
			}
			if len(modules) == 0 {
				t.Fatal("no modules read")
			}
			for _, m := range modules {
				if m.WithdrawalCredentialsType != tc.want {
					t.Errorf("module %d: withdrawalCredentialsType %d, want %d",
						m.ID, m.WithdrawalCredentialsType, tc.want)
				}
			}
		})
	}
}

func budgetSyncer(t *testing.T, url string) (*Syncer, *fakeStore) {
	t.Helper()
	return budgetSyncerOver(t, url, newFakeStore())
}

// budgetSyncerOver builds a syncer over an existing store, which is what a restart is:
// the process keeps nothing, the database keeps everything.
func budgetSyncerOver(t *testing.T, url string, store *fakeStore) (*Syncer, *fakeStore) {
	t.Helper()
	return budgetSyncerOverWithGap(t, url, store, 1000)
}

func budgetSyncerOverWithGap(t *testing.T, url string, store *fakeStore, gap int64) (*Syncer, *fakeStore) {
	t.Helper()

	client := eth.NewClient(url, eth.Options{
		Concurrency: 4, BatchSize: 100,
		// Aggregation off: it probes for the contract once and then routes reads
		// through it, which is a different request shape and a separate question. The
		// budget being pinned here is the plain path.
		Multicall: false,
	})
	provider := lidocore.NewProvider(client, budgetLocator, lidocore.Options{})
	return NewSyncer(provider, store, SyncConfig{MaxBlockGap: gap}, nil), store
}

func assertRequests(t *testing.T, node *budgetNode, want map[string]int, total int) {
	t.Helper()

	got := node.counts()
	for method, n := range want {
		if got[method] != n {
			t.Errorf("%s: %d requests, want %d", method, got[method], n)
		}
	}
	for method, n := range got {
		if _, expected := want[method]; !expected {
			t.Errorf("%s: %d requests, and a quiet cycle is not supposed to call it at all", method, n)
		}
	}

	sum := 0
	for _, n := range got {
		sum += n
	}
	if sum != total {
		t.Errorf("a quiet cycle costs %d requests, want %d:\n%s", sum, total, node.breakdown())
	}
}

// budgetNode is an execution node that answers the reads one cycle makes and counts
// the requests by method.
type budgetNode struct {
	mu       sync.Mutex
	requests map[string]int
	calls    map[string]int
	// multicall makes the fake answer eth_getCode for the Multicall3 address and
	// execute aggregate3, which is what the client probes for before aggregating.
	multicall bool
	block     int64
	router    string
	version   int64
	wcType    uint8
	nonces    map[string]int64
	logHits   map[string]bool
	scans     [][2]int64
	finReads  int
}

// finalizedReads counts eth_getBlockByNumber("finalized") since the last reset.
func (n *budgetNode) finalizedReads() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.finReads
}

// scanned returns the block ranges the node was asked about since the last reset.
func (n *budgetNode) scanned() [][2]int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([][2]int64(nil), n.scans...)
}

type budgetModule struct {
	id      int64
	address string
	kind    string
}

// The four mainnet modules: two curated-shaped, two CSM-shaped, which is what makes
// the operator scan two requests rather than one or four.
var budgetModules = []budgetModule{
	{1, "0x0000000000000000000000000000000000000001", "curated-onchain-v1"},
	{2, "0x0000000000000000000000000000000000000002", "curated-onchain-v1"},
	{3, "0x0000000000000000000000000000000000000003", "community-onchain-v1"},
	{4, "0x0000000000000000000000000000000000000004", "curated-onchain-v2"},
}

// routerContractVersion is what the fake router reports. 4 is the version from which
// the module tuple carries a withdrawal credentials type, which is the shape the ABI in
// this repo declares.
const routerContractVersion = 4

const (
	budgetLocator = "0x000000000000000000000000000000000000010c"
	budgetRouter  = "0x00000000000000000000000000000000000001a0"
	// budgetMetaRegistry is module 4's MetaRegistry.
	budgetMetaRegistry = "0x00000000000000000000000000000000000004e7"
)

func newBudgetNode() *budgetNode {
	n := &budgetNode{
		requests: map[string]int{},
		calls:    map[string]int{},
		block:    1000,
		router:   budgetRouter,
		version:  routerContractVersion,
		wcType:   1,
		nonces:   map[string]int64{},
	}
	for _, m := range budgetModules {
		n.nonces[m.address] = 7
	}
	return n
}

func (n *budgetNode) advance() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.block++
}

func (n *budgetNode) setVersion(version int64, wcType uint8) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.version, n.wcType = version, wcType
}

// emitOperatorEvent makes the next log filter covering this address report a hit,
// which is what a rename looks like to the cycle.
func (n *budgetNode) emitOperatorEvent(address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.logHits == nil {
		n.logHits = map[string]bool{}
	}
	n.logHits[strings.ToLower(address)] = true
}

func (n *budgetNode) setRouter(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.router = addr
}

func (n *budgetNode) bumpNonce(address string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nonces[address]++
}

func (n *budgetNode) reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.requests = map[string]int{}
	n.calls = map[string]int{}
	n.scans = nil
	n.finReads = 0
}

// profile is every JSON-RPC call sent since the last reset, by method — the shape the
// quota projection is built from.
func (n *budgetNode) profile() callProfile {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := callProfile{calls: make(map[string]int, len(n.calls))}
	for method, count := range n.calls {
		out.calls[method] = count
	}
	return out
}

// billedCalls is the number of JSON-RPC objects sent, which is what a compute-unit
// provider charges for. An aggregate counts as the one eth_call it is.
func (n *budgetNode) billedCalls(method string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls[method]
}

func (n *budgetNode) clearOperatorEvents() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.logHits = nil
}

func (n *budgetNode) counts() map[string]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]int, len(n.requests))
	for k, v := range n.requests {
		out[k] = v
	}
	return out
}

func (n *budgetNode) count(method string) int { return n.counts()[method] }

func (n *budgetNode) breakdown() string {
	counts := n.counts()
	methods := make([]string, 0, len(counts))
	for m := range counts {
		methods = append(methods, m)
	}
	sort.Strings(methods)

	var b strings.Builder
	for _, m := range methods {
		b.WriteString("  ")
		b.WriteString(m)
		b.WriteString(" ")
		b.WriteString(big.NewInt(int64(counts[m])).String())
		b.WriteString("\n")
	}
	return b.String()
}

type budgetRequest struct {
	ID     int               `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

func (n *budgetNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqs []budgetRequest
	if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Two different counts, because two different things get billed. One HTTP round
	// trip is one request — what a provider charging per request bills, and what the
	// client's own counter reports. The JSON-RPC objects inside it are what a provider
	// charging compute units bills, and on dRPC that is the number that matters.
	n.mu.Lock()
	if len(reqs) > 0 {
		n.requests[reqs[0].Method]++
	}
	for _, req := range reqs {
		n.calls[req.Method]++
	}
	n.mu.Unlock()

	out := make([]json.RawMessage, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, n.answer(req))
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte("[" + strings.Join(rawStrings(out), ",") + "]"))
}

func rawStrings(raw []json.RawMessage) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		out = append(out, string(r))
	}
	return out
}

func (n *budgetNode) answer(req budgetRequest) json.RawMessage {
	switch req.Method {
	case "eth_getBlockByNumber":
		// The requested height matters: the reorg check asks for the version's own
		// block, and a node that answered every height with the head would make every
		// cycle look like a fork.
		n.mu.Lock()
		head := n.block
		n.mu.Unlock()

		var tag string
		_ = json.Unmarshal(req.Params[0], &tag)
		if tag == "finalized" {
			n.mu.Lock()
			n.finReads++
			n.mu.Unlock()
		}
		block := head
		if strings.HasPrefix(tag, "0x") {
			asked, ok := new(big.Int).SetString(strings.TrimPrefix(tag, "0x"), 16)
			if !ok {
				return budgetError(req.ID, "bad block number "+tag)
			}
			if asked.Int64() > head {
				return budgetResult(req.ID, nil)
			}
			block = asked.Int64()
		}
		return budgetResult(req.ID, budgetHeader(block))

	case "eth_getBlockByHash":
		var hash string
		_ = json.Unmarshal(req.Params[0], &hash)
		return budgetResult(req.ID, budgetHeader(blockOfHash(hash)))

	case "eth_getCode":
		n.mu.Lock()
		on := n.multicall
		n.mu.Unlock()
		if !on {
			return budgetResult(req.ID, "0x")
		}
		return budgetResult(req.ID, "0x6000")

	case "eth_getLogs":
		var filter struct {
			Address   []string `json:"address"`
			FromBlock string   `json:"fromBlock"`
			ToBlock   string   `json:"toBlock"`
		}
		_ = json.Unmarshal(req.Params[0], &filter)

		n.mu.Lock()
		hits := n.logHits
		n.scans = append(n.scans, [2]int64{hexToInt(filter.FromBlock), hexToInt(filter.ToBlock)})
		n.mu.Unlock()
		for _, address := range filter.Address {
			if hits[strings.ToLower(address)] {
				return budgetResult(req.ID, []any{map[string]any{"address": address}})
			}
		}
		return budgetResult(req.ID, []any{})

	case "eth_call":
		var call struct {
			To   string `json:"to"`
			Data string `json:"data"`
		}
		_ = json.Unmarshal(req.Params[0], &call)
		return n.answerCall(req.ID, strings.ToLower(call.To), common.FromHex(call.Data))

	default:
		return budgetError(req.ID, "unexpected method "+req.Method)
	}
}

// answerCall dispatches on the selector rather than on the address, which is how a
// node behaves: the router the locator no longer points at still answers, and that is
// exactly the case the locator re-check has to survive.
func (n *budgetNode) answerCall(id int, to string, data []byte) json.RawMessage {
	n.mu.Lock()
	router, nonces, version, wcType := n.router, n.nonces, n.version, n.wcType
	n.mu.Unlock()

	if len(data) < 4 {
		return budgetError(id, "call with no selector to "+to)
	}
	sel := string(data[:4])

	if to == strings.ToLower(eth.MulticallAddress) {
		return n.answerAggregate(id, data)
	}

	switch {
	case to == budgetLocator && sel == selector(&eth.LocatorABI, "stakingRouter"):
		return budgetResult(id, word(common.FromHex(router)))

	case sel == selector(&eth.StakingRouterABI, "getContractVersion"):
		return budgetResult(id, word(big.NewInt(version).Bytes()))

	case sel == selector(&eth.StakingRouterABI, "getStakingModules"):
		packed, err := packStakingModules(wcType)
		if err != nil {
			return budgetError(id, err.Error())
		}
		return budgetResult(id, "0x"+common.Bytes2Hex(packed))

	// Zero operators, which is what keeps this fake to the reads the loop makes on its
	// own: with none, there are no per-operator key reads, so a rebuild here exercises
	// the decision path rather than the key path.
	case sel == selector(&eth.RegistryABI, "getNodeOperatorsCount"):
		return budgetResult(id, word(big.NewInt(0).Bytes()))

	case sel == selector(&eth.StakingModuleABI, "getType"):
		for _, m := range budgetModules {
			if m.address == to {
				var padded [32]byte
				copy(padded[:], m.kind)
				return budgetResult(id, "0x"+common.Bytes2Hex(padded[:]))
			}
		}
		return budgetError(id, "getType on an address that is not a module: "+to)

	case sel == selector(&eth.RegistryABI, "getNonce"):
		return budgetResult(id, word(big.NewInt(nonces[to]).Bytes()))

	// CMv2 names live in its MetaRegistry; the operator read resolves it, and the
	// event scan watches it.
	case sel == selector(&eth.CMv2ABI, "META_REGISTRY"):
		return budgetResult(id, word(common.FromHex(budgetMetaRegistry)))

	default:
		return budgetError(id, "unexpected call to "+to)
	}
}

// answerAggregate executes an aggregate3 by dispatching each inner call through the
// same answerCall the plain path uses, so an aggregated cycle and a plain one are
// answered identically and only the billed-call count differs.
func (n *budgetNode) answerAggregate(id int, data []byte) json.RawMessage {
	type call3 struct {
		Target       common.Address
		AllowFailure bool
		CallData     []byte
	}
	type call3Result struct {
		Success    bool
		ReturnData []byte
	}

	values, err := eth.MulticallABI.Methods["aggregate3"].Inputs.Unpack(data[4:])
	if err != nil {
		return budgetError(id, "bad aggregate3 input: "+err.Error())
	}
	inner := *abi.ConvertType(values[0], new([]call3)).(*[]call3)

	results := make([]call3Result, 0, len(inner))
	for _, c := range inner {
		raw := n.answerCall(0, strings.ToLower(c.Target.Hex()), c.CallData)
		var decoded struct {
			Result string `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &decoded)
		if decoded.Error != nil {
			return budgetError(id, "inner call failed: "+decoded.Error.Message)
		}
		results = append(results, call3Result{Success: true, ReturnData: common.FromHex(decoded.Result)})
	}

	packed, err := eth.MulticallABI.Methods["aggregate3"].Outputs.Pack(results)
	if err != nil {
		return budgetError(id, err.Error())
	}
	return budgetResult(id, "0x"+common.Bytes2Hex(packed))
}

func word(b []byte) string {
	return "0x" + common.Bytes2Hex(common.LeftPadBytes(b, 32))
}

func budgetHeader(block int64) map[string]any {
	return map[string]any{
		"number":    hexInt(block),
		"hash":      blockHash(block),
		"timestamp": hexInt(1_700_000_000 + block*12),
	}
}

func selector(parsed *abi.ABI, method string) string {
	return string(parsed.Methods[method].ID)
}

// packStakingModules encodes the getStakingModules return, which is the one answer
// the fake cannot fake: the decoder reads it by reflection over the real ABI.
func packStakingModules(wcType uint8) ([]byte, error) {
	type module struct {
		Id                         *big.Int
		StakingModuleAddress       common.Address
		StakingModuleFee           uint16
		TreasuryFee                uint16
		StakeShareLimit            uint16
		Status                     uint8
		Name                       string
		LastDepositAt              uint64
		LastDepositBlock           *big.Int
		ExitedValidatorsCount      *big.Int
		PriorityExitShareThreshold uint16
		MaxDepositsPerBlock        uint64
		MinDepositBlockDistance    uint64
		WithdrawalCredentialsType  uint8
	}

	modules := make([]module, 0, len(budgetModules))
	for _, m := range budgetModules {
		modules = append(modules, module{
			Id:                        big.NewInt(m.id),
			StakingModuleAddress:      common.HexToAddress(m.address),
			Name:                      m.kind,
			LastDepositBlock:          big.NewInt(0),
			ExitedValidatorsCount:     big.NewInt(0),
			WithdrawalCredentialsType: wcType,
		})
	}
	return eth.StakingRouterABI.Methods["getStakingModules"].Outputs.Pack(modules)
}

func budgetResult(id int, value any) json.RawMessage {
	body, _ := json.Marshal(map[string]any{"id": id, "result": value})
	return body
}

func budgetError(id int, message string) json.RawMessage {
	body, _ := json.Marshal(map[string]any{
		"id":    id,
		"error": map[string]any{"code": -32000, "message": message},
	})
	return body
}

func hexInt(n int64) string { return "0x" + big.NewInt(n).Text(16) }

func hexToInt(hex string) int64 {
	n, _ := new(big.Int).SetString(strings.TrimPrefix(hex, "0x"), 16)
	if n == nil {
		return -1
	}
	return n.Int64()
}

func blockHash(block int64) string {
	return "0x" + common.Bytes2Hex(common.LeftPadBytes(big.NewInt(block).Bytes(), 32))
}

// blockOfHash is blockHash inverted: the fake's hashes are the height, so a header
// read by hash answers about the same block a read by number would.
func blockOfHash(hash string) int64 {
	return new(big.Int).SetBytes(common.FromHex(hash)).Int64()
}
