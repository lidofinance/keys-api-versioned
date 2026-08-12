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
		// Head, the finality pin, and the reorg check reading the version's own height.
		"eth_getBlockByNumber": 3,
		// The head's timestamp. Removable: it is in the header the head read already
		// returned, and this line is here so that removing it shows up as a win.
		"eth_getBlockByHash": 1,
		// One request resolving the locator and reading the module set together, one
		// carrying every getType and getNonce.
		"eth_call": 2,
		// One filter per module family, not one per module: four modules, two families.
		"eth_getLogs": 2,
	}
	assertRequests(t, node, want, 8)
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

	client := eth.NewClient(url, eth.Options{
		Concurrency: 4, BatchSize: 100,
		// Aggregation off: it probes for the contract once and then routes reads
		// through it, which is a different request shape and a separate question. The
		// budget being pinned here is the plain path.
		Multicall: false,
	})
	provider := lidocore.NewProvider(client, budgetLocator, lidocore.Options{})
	store := newFakeStore()
	return NewSyncer(provider, store, SyncConfig{MaxBlockGap: 1000}, nil), store
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
	block    int64
	router   string
	version  int64
	wcType   uint8
	nonces   map[string]int64
	logHits  map[string]bool
	scans    [][2]int64
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
)

func newBudgetNode() *budgetNode {
	n := &budgetNode{
		requests: map[string]int{},
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
	n.scans = nil
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

	// One HTTP round trip is one request, whatever it carries: that is what an
	// endpoint meters and what the client's own counter counts. A JSON-RPC batch
	// mixing methods is recorded under the first one, the same label the client uses.
	if len(reqs) > 0 {
		n.mu.Lock()
		n.requests[reqs[0].Method]++
		n.mu.Unlock()
	}

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

	default:
		return budgetError(id, "unexpected call to "+to)
	}
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
