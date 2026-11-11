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

// A day of polling at the deployed interval must fit the provider's daily compute-unit budget,
// measured by counting the calls a real Syncer sends. Cold sync and nonce-driven reads: auditAllowance.

// The deployment being checked; change together with its configuration.
const (
	// quotaCUPerDay is the key's daily limit, as quoted in the provider's error.
	quotaCUPerDay = 2_000_000
	// contours is how many writers share the key.
	contours = 2
	// pollInterval must match the writer's configured UPDATE_KEYS_INTERVAL_MS.
	pollInterval = 5 * time.Second
	slotTime     = 12 * time.Second
	epochTime    = 384 * time.Second // 12 s × 32 slots
	// auditAllowance covers full audits and scoped re-reads: ~4 900 CU per full mainnet read with
	// aggregation, seven audits a day per writer, plus headroom.
	auditAllowance = 100_000
)

// computeUnits per method, from the provider's rate-limit docs (eth_call 20, eth_getLogs 75+).
// Unclear figures take the pessimistic value: the test should fail early rather than pass wrongly.
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

	// Below the slot time each block is observed once, so only empty polls scale with the interval.
	movedCycles := blocksPerDay
	if pollsPerDay < blocksPerDay {
		movedCycles = pollsPerDay
	}
	idleCycles := pollsPerDay - movedCycles

	// Epoch-paced reads are charged once per epoch, not per cycle.
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

// Below the slot time the expensive part is constant: 5 s costs only a third more than 12 s.
// It stops holding the moment the unchanged-head short-circuit breaks.
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

type callProfile struct{ calls map[string]int }

func (p callProfile) cu() int {
	total := 0
	for method, n := range p.calls {
		weight, known := computeUnits[method]
		if !known {
			// An unpriced method is charged at the most expensive known rate, so adding one is visible.
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

// measureCycleProfiles runs real cycles, configured metered and aggregating as deployed.
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

	// Cold start, then one cycle to arm the epoch marks, so the next cycles fall inside an epoch.
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

	node.reset()
	clock = clock.Add(pollInterval)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("idle poll: %v", r.Err)
	}
	idle = node.profile()

	node.reset()
	advance(slotTime)
	if r := syncer.Cycle(context.Background()); r.Err != nil {
		t.Fatalf("cycle after a new block: %v", r.Err)
	}
	moved = node.profile()

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
