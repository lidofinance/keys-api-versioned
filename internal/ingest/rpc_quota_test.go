package ingest

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
)

// This test answers one question: at the interval this service is deployed with, does a
// day of polling fit inside the provider's daily budget?
//
// It exists because the answer was once no, and nothing said so. Two contours polling
// every twelve seconds spent about 6.5M compute units a day against a 2 000 000 daily
// key limit; dRPC cut them off five hours into every day, and both writers sat dead
// while a healthy fallback endpoint went unasked. Nobody had done the multiplication,
// and there was nothing to do it against.
//
// The profile is measured rather than asserted: a real Syncer runs real cycles against a
// fake node that counts the JSON-RPC calls it receives. So a change that adds a call
// anywhere under Cycle shows up here without anyone remembering to update a number, and
// the failure names the method that grew and what it costs per day.
//
// What this does NOT cover, so that the number is not read as more than it is: the cold
// sync, which happens once per database, and the key reads a real nonce change triggers,
// whose cost is a function of how many keys moved. Those are the auditAllowance below.

// The deployment being checked. Change these together with the values file — that is the
// point of them being here.
const (
	// quotaCUPerDay is the dRPC key's daily limit, taken from the error it returns:
	// "Your key daily limit exceeded (Key day limit 2000000)".
	quotaCUPerDay = 2_000_000
	// contours is how many writers share the key. Mainnet and Hoodi both point at the
	// same dRPC account today.
	contours = 2
	// pollInterval must match UPDATE_KEYS_INTERVAL_MS in
	// helm-charts-vroom/vroom-evm-kapi-writer/values-k8s-dev.yaml.
	pollInterval = 5 * time.Second
	// slotTime and epochTime are the chain's, and they decide how many blocks a day has
	// to be observed and how often the epoch-paced reads come due.
	slotTime  = 12 * time.Second
	epochTime = 384 * time.Second // 12 s × 32 slots
	// auditAllowance covers what this test cannot measure: the periodic full audits and
	// the scoped re-reads a real nonce change triggers. Measured on mainnet at ~4 900 CU
	// per full read once aggregation collapses the key calls, seven audits a day per
	// contour, plus headroom for a handful of real changes.
	auditAllowance = 100_000
)

// computeUnits is what the provider charges per method.
//
// Not ours to define, which is exactly why it is a table with a source rather than a
// number folded into an expression. The anchor is dRPC's own rate-limit documentation —
// 120 000 CU per minute is "approximately 100 eth_call per second", so an eth_call is
// 20 — and eth_getLogs is documented at 75 or more.
//
// Where the published figure is unclear the value here is deliberately the pessimistic
// one: this test should fail early rather than pass wrongly, and a header read billed at
// 24 rather than 15 is the difference between 26 % and 42 % of headroom.
var computeUnits = map[string]int{
	"eth_call":             20,
	"eth_getLogs":          75,
	"eth_getBlockByNumber": 24,
	"eth_getBlockByHash":   24,
	"eth_getCode":          24,
	"eth_chainId":          10,
}

func TestADayOfPollingFitsTheProviderQuota(t *testing.T) {
	idle, moved, epoch := measureCycleProfiles(t)

	blocksPerDay := int(24 * time.Hour / slotTime)
	pollsPerDay := int(24 * time.Hour / pollInterval)
	epochsPerDay := int(24 * time.Hour / epochTime)

	// Below the slot time every block is still observed exactly once — whichever poll
	// sees it first — so the expensive cycles are a function of the chain and only the
	// empty polls scale with the interval. Above it, polls are the binding number and
	// blocks get skipped, which costs nothing extra.
	movedCycles := blocksPerDay
	if pollsPerDay < blocksPerDay {
		movedCycles = pollsPerDay
	}
	idleCycles := pollsPerDay - movedCycles

	// The epoch-paced reads are the difference between a cycle that took them and one
	// that did not, charged once per epoch rather than once per cycle.
	epochExtra := epoch.minus(moved)

	perContour := moved.scale(movedCycles).
		plus(idle.scale(idleCycles)).
		plus(epochExtra.scale(epochsPerDay))
	total := perContour.cu()*contours + auditAllowance

	t.Logf("projected %d CU/day for %d contour(s) at a %s interval, quota %d (%.0f%% used)\n%s",
		total, contours, pollInterval, quotaCUPerDay,
		100*float64(total)/quotaCUPerDay, perContour.table())

	if total > quotaCUPerDay {
		t.Fatalf(`a day of polling no longer fits the provider quota.

  projected  %d CU/day  (%d contours at a %s interval, plus %d for audits)
  quota      %d CU/day
  over by    %d

per contour, per day:
%s
Something under Cycle started making more calls. Either take the call back out, or — if
it has to be there — say so by raising the interval in
helm-charts-vroom/vroom-evm-kapi-writer/values-k8s-dev.yaml and pollInterval here, and
work out whether the freshness it costs is acceptable. Do not raise the quota constant:
it is what the provider grants, not a number we choose.`,
			total, contours, pollInterval, auditAllowance,
			quotaCUPerDay, total-quotaCUPerDay, perContour.table())
	}
}

// Below the slot time the expensive part of the bill is constant, which is the property
// that makes a five-second interval cost only a third more than a twelve-second one.
// Worth pinning: it is the reason the interval can be chosen for freshness rather than
// for cost, and it stops holding the moment the short-circuit stops working.
func TestLoweringTheIntervalBelowASlotOnlyAddsEmptyPolls(t *testing.T) {
	idle, moved, _ := measureCycleProfiles(t)

	if idle.cu() == 0 {
		t.Fatal("an idle poll should still cost one header read")
	}
	if idle.cu() >= moved.cu() {
		t.Fatalf("an idle poll costs %d CU and a cycle that saw a block costs %d; the "+
			"short-circuit is not saving anything", idle.cu(), moved.cu())
	}
	// The whole point: an idle poll must not read the module set or scan logs.
	for _, method := range []string{"eth_call", "eth_getLogs"} {
		if idle.calls[method] != 0 {
			t.Errorf("an idle poll made %d %s; it should stop after the head read",
				idle.calls[method], method)
		}
	}
}

// callProfile is what one cycle sent, by method.
type callProfile struct{ calls map[string]int }

func (p callProfile) cu() int {
	total := 0
	for method, n := range p.calls {
		weight, known := computeUnits[method]
		if !known {
			// An unpriced method is a hole in the table rather than a free call. Charged
			// at the most expensive thing we know about so that adding one is visible.
			weight = 75
		}
		total += n * weight
	}
	return total
}

func (p callProfile) scale(n int) callProfile {
	out := callProfile{calls: map[string]int{}}
	for method, count := range p.calls {
		out.calls[method] = count * n
	}
	return out
}

func (p callProfile) plus(other callProfile) callProfile {
	out := callProfile{calls: map[string]int{}}
	for method, count := range p.calls {
		out.calls[method] = count
	}
	for method, count := range other.calls {
		out.calls[method] += count
	}
	return out
}

// minus is the extra work one profile did over another, floored at zero: it compares a
// cycle that took the epoch-paced reads against one that did not.
func (p callProfile) minus(other callProfile) callProfile {
	out := callProfile{calls: map[string]int{}}
	for method, count := range p.calls {
		if diff := count - other.calls[method]; diff > 0 {
			out.calls[method] = diff
		}
	}
	return out
}

func (p callProfile) table() string {
	methods := make([]string, 0, len(p.calls))
	for method := range p.calls {
		methods = append(methods, method)
	}
	sort.Strings(methods)

	var b strings.Builder
	for _, method := range methods {
		if p.calls[method] == 0 {
			continue
		}
		weight, known := computeUnits[method]
		note := ""
		if !known {
			weight, note = 75, "  <- not in the CU table, charged at the worst known rate"
		}
		b.WriteString(fmt.Sprintf("  %-22s %9d calls  %10d CU%s\n",
			method, p.calls[method], p.calls[method]*weight, note))
	}
	b.WriteString(fmt.Sprintf("  %-22s %9s  %10d CU\n", "total", "", p.cu()))
	return b.String()
}

// measureCycleProfiles runs real cycles and returns what each kind of cycle sent: one
// where the head had not moved, one where it had, and one where the epoch-paced reads
// also came due.
//
// Configured the way the deployment is — metered, aggregating — because the profile of
// the thing being budgeted is the profile that matters.
func measureCycleProfiles(t *testing.T) (idle, moved, epoch callProfile) {
	t.Helper()

	node := newBudgetNode()
	node.multicall = true
	server := httptest.NewServer(node)
	defer server.Close()

	client := eth.NewClient(server.URL, eth.Options{
		Concurrency: 4, BatchSize: 100, Multicall: true, Metered: true,
	})
	provider := lidocore.NewProvider(client, budgetLocator, lidocore.Options{})
	syncer := NewSyncer(provider, newFakeStore(), SyncConfig{
		MaxBlockGap:   1000,
		EpochDuration: epochTime,
	}, nil)

	clock := time.Unix(1_700_000_000, 0)
	syncer.now = func() time.Time { return clock }

	// Cold start, then one cycle to arm the epoch marks so that the next cycles are
	// inside an epoch rather than at its boundary.
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cold start: %v", r.Err)
	}
	advance := func(d time.Duration) {
		clock = clock.Add(d)
		node.advance()
	}
	advance(slotTime)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("arming cycle: %v", r.Err)
	}

	// A poll that finds the same head.
	node.reset()
	clock = clock.Add(pollInterval)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("idle poll: %v", r.Err)
	}
	idle = node.profile()

	// A poll that finds a new block, still inside the epoch.
	node.reset()
	advance(slotTime)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cycle after a new block: %v", r.Err)
	}
	moved = node.profile()

	// A poll that also takes the epoch-paced reads.
	node.reset()
	clock = clock.Add(epochTime)
	node.advance()
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cycle at the epoch boundary: %v", r.Err)
	}
	epoch = node.profile()

	if moved.cu() == 0 {
		t.Fatal("measured nothing for a cycle that saw a new block")
	}
	return idle, moved, epoch
}
