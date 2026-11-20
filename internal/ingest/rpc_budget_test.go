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

// Exact per-method request counts for a quiet cycle: any new round trip fails here and changes a
// number a reviewer sees. Whether a day still fits the quota: TestADayOfPollingFitsTheProviderQuota.

func TestQuietCycleRequestBudget(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)

	// The cold start's cost depends on the registry; the steady state is what is asserted.
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	if store.latest == nil {
		t.Fatal("cold start published nothing, so there is no quiet cycle to measure")
	}

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
		// Head plus finality pin (pacing is off here). The reorg probe is skipped while the version's
		// block is finalized, and the head header carries the timestamp.
		"eth_getBlockByNumber": 2,
		// One request for locator, contract version and module set; one for every getType and getNonce.
		"eth_call": 2,
		// One filter per module family, not per module: four modules, two families.
		"eth_getLogs": 2,
	}
	assertRequests(t, node, want, 6)
}

// The rebuild is already decided, so the operator scan is skipped. Pinned because nothing else
// fails if a change starts asking anyway.
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

// Locator and contract version reads ride in the module-set request.
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

// A replacement router can report the same modules and nonces; module rows must still be rewritten.
// Keys come from the module contracts, whose addresses are unchanged, so they are not re-read.
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

	// The reason is consumed: one deploy, one rebuild.
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

// Below v4 the tuple has no withdrawal credentials type, and no nonce moves on the upgrade.
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

// A CMv2 rename is an event at its MetaRegistry; the community filter carries it at no extra request.
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

// The DAO sets status, fees and share limits on the router, and no module nonce moves. Only the module
// rows change, and the rebuild reuses the cycle's module read: the cycle costs what a quiet one does.
func TestARouterFieldChangeRewritesModuleRowsAtNoExtraRequests(t *testing.T) {
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
	keysBefore, operatorsBefore, modulesBefore := store.keyWrites, len(store.operatorWrites), len(store.moduleWrites)

	node.advance()
	node.setStatus(1)
	node.reset()

	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle after the status change: %v", r.Err)
	}
	if r.Action != ActionRebuild || !strings.Contains(r.Reason, "router fields") {
		t.Fatalf("action %q (%s), want a rebuild for the router fields", r.Action, r.Reason)
	}
	assertEveryModuleWritten(t, store, modulesBefore)
	if store.keyWrites != keysBefore || len(store.operatorWrites) != operatorsBefore {
		t.Errorf("%d key and %d operator re-reads, want none: only the router's tuple moved",
			store.keyWrites-keysBefore, len(store.operatorWrites)-operatorsBefore)
	}
	assertRequests(t, node, map[string]int{"eth_getBlockByNumber": 2, "eth_call": 2}, 4)

	node.advance()
	if next := syncer.Cycle(context.Background()); next.Err != nil || next.Action != ActionExtend {
		t.Fatalf("next cycle: action %q (%s), err %v; want extend once the rows are rewritten", next.Action, next.Reason, next.Err)
	}
}

// The source reports a contract version change once. A rebuild that fails must not use it up: with
// the same nonces and the same decoded fields, nothing else would ever rewrite the module rows.
func TestASourceChangeSurvivesAFailedRebuild(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	node.advance()
	node.setVersion(routerContractVersion+1, 1)
	store.failModules = true
	if r := syncer.Cycle(context.Background()); r.Err == nil {
		t.Fatal("want the rebuild to fail")
	}

	store.failModules = false
	node.advance()
	r := syncer.Cycle(context.Background())
	if r.Err != nil {
		t.Fatalf("cycle after the failure: %v", r.Err)
	}
	if r.Action != ActionRebuild || !strings.Contains(r.Reason, "contract version") {
		t.Fatalf("action %q (%s), want the rebuild retried for the contract version", r.Action, r.Reason)
	}

	node.advance()
	if next := syncer.Cycle(context.Background()); next.Action != ActionExtend {
		t.Fatalf("action %q (%s), want extend once a rebuild has published", next.Action, next.Reason)
	}
}

// A rebuild scoped to one module's keys moves the version but reads no other module's operators, so
// the next scan must start where the last one ended, not at the new version's block. After a rebuild
// that read every module's operators, it starts there.
func TestAScopedRebuildLeavesItsBlocksToTheNextScan(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, _ := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	node.advance()
	quiet := syncer.Cycle(context.Background())
	if quiet.Err != nil || quiet.Action != ActionExtend {
		t.Fatalf("quiet cycle: action %q, err %v", quiet.Action, quiet.Err)
	}

	node.advance()
	node.bumpNonce(budgetModules[2].address)
	if r := syncer.Cycle(context.Background()); r.Err != nil || r.Action != ActionRebuild {
		t.Fatalf("nonce cycle: action %q (%s), err %v; want a rebuild", r.Action, r.Reason, r.Err)
	}

	node.advance()
	node.reset()
	if r := syncer.Cycle(context.Background()); r.Err != nil || r.Action != ActionExtend {
		t.Fatalf("cycle after the rebuild: action %q (%s), err %v", r.Action, r.Reason, r.Err)
	}
	for _, scan := range node.scanned() {
		if scan[0] != quiet.HeadBlock+1 {
			t.Errorf("scanned %d..%d, want it to start at %d, the block after the last scan",
				scan[0], scan[1], quiet.HeadBlock+1)
		}
	}

	// An operator rebuild reads every module's operators: the next scan starts after it.
	node.advance()
	node.emitOperatorEvent(budgetModules[0].address)
	rebuilt := syncer.Cycle(context.Background())
	if rebuilt.Err != nil || rebuilt.Action != ActionRebuild {
		t.Fatalf("event cycle: action %q (%s), err %v; want a rebuild", rebuilt.Action, rebuilt.Reason, rebuilt.Err)
	}
	node.clearOperatorEvents()
	node.advance()
	node.reset()
	if r := syncer.Cycle(context.Background()); r.Err != nil || r.Action != ActionExtend {
		t.Fatalf("cycle after the operator rebuild: action %q (%s), err %v", r.Action, r.Reason, r.Err)
	}
	for _, scan := range node.scanned() {
		if scan[0] != rebuilt.HeadBlock+1 {
			t.Errorf("scanned %d..%d, want it to start at %d, after the operator rebuild",
				scan[0], scan[1], rebuilt.HeadBlock+1)
		}
	}
}

// A restart has no mark, so the published version's block is the floor. A rebuild on the first cycle
// that reads no operators moves the version; the floor must stay where the process found it.
func TestARebuildRightAfterARestartKeepsTheScanFloor(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	first, store := budgetSyncer(t, server.URL)
	if r := first.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	published := store.latest.BlockNumber

	restarted, _ := budgetSyncerOver(t, server.URL, store)
	node.advance()
	node.bumpNonce(budgetModules[2].address)
	if r := restarted.Cycle(context.Background()); r.Err != nil || r.Action != ActionRebuild {
		t.Fatalf("first cycle after the restart: action %q (%s), err %v; want a rebuild", r.Action, r.Reason, r.Err)
	}

	node.advance()
	node.reset()
	if r := restarted.Cycle(context.Background()); r.Err != nil || r.Action != ActionExtend {
		t.Fatalf("next cycle: action %q (%s), err %v", r.Action, r.Reason, r.Err)
	}
	scans := node.scanned()
	if len(scans) == 0 {
		t.Fatal("no scan after the rebuild")
	}
	for _, scan := range scans {
		if scan[0] != published+1 {
			t.Errorf("scanned %d..%d, want it to start at %d, after the version the process started on",
				scan[0], scan[1], published+1)
		}
	}
}

// With a scan mark each cycle scans only the blocks since the last empty scan; without it the
// window grew by a block every cycle until the audit.
func TestOperatorScanOnlyCoversNewBlocks(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, _ := budgetSyncer(t, server.URL)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}

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

// A found event must not move the mark: the rebuild can fail, and the rename would then surface
// only in the periodic audit.
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

	store.failOperators = true
	node.advance()
	node.emitOperatorEvent(budgetModules[0].address)

	r := syncer.Cycle(context.Background())
	if r.Err == nil {
		t.Fatal("want the rebuild to fail, so that the next cycle has to rescan")
	}
	failedAt := r.HeadBlock

	// The event is gone from the chain's answer; only rescanning its blocks finds it again.
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

// A quiet cycle sends eleven JSON-RPC calls in two round trips; aggregation bills two. Asserted as
// calls, not compute units: the provider's price table is not ours to encode.
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

			// Two dependent steps: the second reads from module addresses the first returns.
			if got := node.billedCalls("eth_call"); got != tc.want {
				t.Errorf("a quiet cycle billed %d eth_call, want %d", got, tc.want)
			}
		})
	}
}

func TestAnUnchangedHeadCostsOneRequest(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncerOver(t, server.URL, newFakeStore())
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	extendsBefore := store.extends

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
	if store.extends != extendsBefore {
		t.Errorf("%d version writes on an unchanged head, want none", store.extends-extendsBefore)
	}

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

// A compute-unit provider bills the call, not the block range, so one scan per epoch over 32 slots
// divides the bill rather than trading it.
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

	// A fresh process always takes the first scan.
	clock = clock.Add(12 * time.Second)
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("first cycle: %v", r.Err)
	}
	if len(node.scanned()) == 0 {
		t.Fatal("a fresh process should scan on its first cycle rather than wait an epoch")
	}

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

	// Crossing the epoch: one scan per family over everything accumulated.
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

// A restart compares the stored nonces against the chain and reads no keys. Pinned: re-reading
// 500 000 keys per deploy would look like a slow start, not a bug.
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

	// No in-memory state survives the restart; only the store does.
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

	// A restart costs one extra eth_call to resolve the router, one for CMv2's MetaRegistry, and a
	// wide event scan because the scan mark lives in memory.
	if got := node.count("eth_call"); got != 4 {
		t.Errorf("eth_call %d, want 4: a quiet cycle's 2 plus the router and the MetaRegistry resolved cold", got)
	}
	for _, scan := range node.scanned() {
		if scan[0] != store.latest.BlockNumber+1 {
			t.Errorf("scanned %d..%d, want the range to restart at the published version's block %d",
				scan[0], scan[1], store.latest.BlockNumber+1)
		}
	}

	node.advance()
	node.reset()
	if r := restarted.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("second cycle after the restart: %v", r.Err)
	}
	if got := node.count("eth_call"); got != 2 {
		t.Errorf("eth_call %d, want 2: the router is cached from here on", got)
	}
}

// Pinned, not fixed: an unfinished cold sync stays building and Latest ignores it, so a pod dying
// before its first version re-reads all 500 000 keys on every start.
func TestACrashDuringTheColdSyncStartsOver(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	store := newFakeStore()
	syncer, _ := budgetSyncerOver(t, server.URL, store)

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

// Downtime longer than MAX_BLOCKS_OVERLAP triggers the audit: know this before narrowing the window.
func TestRestartAfterLongDowntimeRunsTheAudit(t *testing.T) {
	node := newBudgetNode()
	server := httptest.NewServer(node)
	defer server.Close()

	syncer, store := budgetSyncerOverWithGap(t, server.URL, newFakeStore(), 100)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	keysBefore := store.keyWrites

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
	// Flagged as an audit, so metrics separate timer re-reads from signal-driven ones.
	if !r.Audit {
		t.Error("a block-gap rebuild should be marked as the audit")
	}
	if got := store.keyWrites - keysBefore; got != len(budgetModules) {
		t.Errorf("%d modules had their keys re-read, want all %d: the audit trusts no signal", got, len(budgetModules))
	}
}

// A provenance change invalidates every module row, so the rebuild must not be scoped.
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

// Follows the router's reported version, not the build's ABI: the tuple belongs to the chain.
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

func budgetSyncerOver(t *testing.T, url string, store *fakeStore) (*Syncer, *fakeStore) {
	t.Helper()
	return budgetSyncerOverWithGap(t, url, store, 1000)
}

func budgetSyncerOverWithGap(t *testing.T, url string, store *fakeStore, gap int64) (*Syncer, *fakeStore) {
	t.Helper()

	client := eth.NewClient(url, eth.Options{
		Concurrency: 4, BatchSize: 100,
		// Aggregation off: the plain request shape is what is pinned here.
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

type budgetNode struct {
	mu       sync.Mutex
	requests map[string]int
	calls    map[string]int
	// multicall makes the fake answer the Multicall3 probe and execute aggregate3.
	multicall bool
	block     int64
	router    string
	version   int64
	wcType    uint8
	// status is every module's router status (0 = active).
	status   uint8
	nonces   map[string]int64
	logHits  map[string]bool
	scans    [][2]int64
	finReads int
}

func (n *budgetNode) finalizedReads() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.finReads
}

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

// Two curated-shaped and two CSM-shaped modules, so the operator scan is two requests.
var budgetModules = []budgetModule{
	{1, "0x0000000000000000000000000000000000000001", "curated-onchain-v1"},
	{2, "0x0000000000000000000000000000000000000002", "curated-onchain-v1"},
	{3, "0x0000000000000000000000000000000000000003", "community-onchain-v1"},
	{4, "0x0000000000000000000000000000000000000004", "curated-onchain-v2"},
}

// From v4 the module tuple carries a withdrawal credentials type, as this repo's ABI declares.
const routerContractVersion = 4

const (
	budgetLocator      = "0x000000000000000000000000000000000000010c"
	budgetRouter       = "0x00000000000000000000000000000000000001a0"
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

func (n *budgetNode) setStatus(status uint8) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status = status
}

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

func (n *budgetNode) profile() callProfile {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := callProfile{calls: make(map[string]int, len(n.calls))}
	for method, count := range n.calls {
		out.calls[method] = count
	}
	return out
}

// billedCalls counts JSON-RPC objects; an aggregate counts as the one eth_call it is.
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

	// Round trips are what per-request billing counts; the JSON-RPC objects inside are what
	// compute-unit billing counts.
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
		// The requested height matters: answering every height with the head would look like a fork.
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

// Dispatch on selector, not address, as a node does: the router the locator left still answers.
func (n *budgetNode) answerCall(id int, to string, data []byte) json.RawMessage {
	n.mu.Lock()
	router, nonces, version, wcType, status := n.router, n.nonces, n.version, n.wcType, n.status
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
		packed, err := packStakingModules(wcType, status)
		if err != nil {
			return budgetError(id, err.Error())
		}
		return budgetResult(id, "0x"+common.Bytes2Hex(packed))

	// Zero operators keep the fake to the loop's own reads: no per-operator key reads.
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

	// CMv2 names live in its MetaRegistry.
	case sel == selector(&eth.CMv2ABI, "META_REGISTRY"):
		return budgetResult(id, word(common.FromHex(budgetMetaRegistry)))

	default:
		return budgetError(id, "unexpected call to "+to)
	}
}

// answerAggregate reuses answerCall, so only the billed-call count differs from the plain path.
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

// packStakingModules is real ABI encoding: the decoder reads it by reflection.
func packStakingModules(wcType, status uint8) ([]byte, error) {
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
			Status:                    status,
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

// The fake's hashes encode the height.
func blockOfHash(hash string) int64 {
	return new(big.Int).SetBytes(common.FromHex(hash)).Int64()
}
