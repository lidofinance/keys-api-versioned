package lidocore

import (
	"context"
	"math"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// Counts from the provider size allocations and loops; a wrapped or absurd one used to panic in
// make or plan ~1e16 reads.

func TestAnOutOfRangeOperatorCountFailsTheReadWithoutPanicking(t *testing.T) {
	for name, count := range map[string]*big.Int{
		"uint256 max":         new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)),
		"wraps negative":      new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 63), big.NewInt(1)),
		"2^40":                new(big.Int).Lsh(big.NewInt(1), 40),
		"one over the limit":  big.NewInt(maxOperatorsPerModule + 1),
		"fits int32, too big": big.NewInt(math.MaxInt32),
	} {
		t.Run(name, func(t *testing.T) {
			node := &operatorsNode{count: count}
			server := httptest.NewServer(node)
			defer server.Close()
			client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})

			for _, reader := range []ModuleReader{&CuratedReader{}, &CommunityReader{}} {
				operators, err := reader.Operators(context.Background(), client, eth.BlockRef{Tag: "latest"}, csmUnderTest)
				if err == nil {
					t.Fatalf("%T: got %d operators and no error, want the count rejected", reader, len(operators))
				}
				if !strings.Contains(err.Error(), "getNodeOperatorsCount") {
					t.Errorf("%T: error %q does not name the count", reader, err)
				}
			}
		})
	}
}

func TestAnOperatorCountAtTheLimitIsAccepted(t *testing.T) {
	if _, err := operatorCount([]any{big.NewInt(maxOperatorsPerModule)}); err != nil {
		t.Fatalf("count at the limit: %v", err)
	}
	if _, err := operatorCount(nil); err == nil {
		t.Fatal("an empty result was accepted as a count")
	}
}

func TestCommunityOperatorWithAnAbsurdKeyCountFailsTheRead(t *testing.T) {
	// uint32 in the ABI: 0x80000001 used to become a negative int32, then 1.8e19 spans.
	node := &operatorsNode{operators: 2, addedKeys: 0x80000001}
	server := httptest.NewServer(node)
	defer server.Close()
	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})

	operators, err := (&CommunityReader{}).Operators(context.Background(), client, eth.BlockRef{Tag: "latest"}, csmUnderTest)
	if err == nil {
		t.Fatalf("got %+v and no error, want TotalAddedKeys rejected", operators)
	}
	if !strings.Contains(err.Error(), "TotalAddedKeys") {
		t.Errorf("error %q does not name the field", err)
	}
}

func TestCuratedOperatorCountersAreRangeChecked(t *testing.T) {
	valid := func() []any {
		return []any{true, "op", common.Address{}, uint64(10), uint64(1), uint64(12), uint64(8)}
	}
	if op, err := decodeCuratedOperator(0, moduleUnderTest, valid()); err != nil || op.TotalSigningKeys != 12 {
		t.Fatalf("valid operator: got %+v, %v", op, err)
	}

	for i, field := range []string{"totalVetted", "totalExited", "totalAdded", "totalDeposited"} {
		for _, bad := range []uint64{0x80000001, 1 << 40, math.MaxUint64, maxKeysPerOperator + 1} {
			values := valid()
			values[3+i] = bad
			_, err := decodeCuratedOperator(0, moduleUnderTest, values)
			if err == nil {
				t.Errorf("%s = %d was accepted", field, bad)
				continue
			}
			if !strings.Contains(err.Error(), field) {
				t.Errorf("%s = %d: error %q does not name the field", field, bad, err)
			}
		}
	}
}

func TestDepositableCountIsRangeChecked(t *testing.T) {
	huge := new(big.Int).Lsh(big.NewInt(1), 64)
	if _, err := depositableFromSummary(0, []any{big.NewInt(0), huge}); err == nil {
		t.Error("depositableValidatorsCount 2^64 was accepted")
	}
	if _, err := depositableFromSummary(0, nil); err == nil {
		t.Error("an empty summary was accepted")
	}
	if n, err := depositableFromSummary(0, []any{huge, big.NewInt(7)}); err != nil || n != 7 {
		t.Errorf("got %d, %v, want 7 (only the last output is the count)", n, err)
	}
}

func TestKeyReadsWithImpossibleTotalsAreRefusedBeforeAnyRequest(t *testing.T) {
	for name, operators := range map[string][]model.Operator{
		"negative total": {{Index: 0, TotalSigningKeys: -2147483647}},
		"module over the ceiling": {
			{Index: 0, TotalSigningKeys: maxKeysPerOperator},
			{Index: 1, TotalSigningKeys: maxKeysPerOperator},
			{Index: 2, TotalSigningKeys: maxKeysPerOperator},
			{Index: 3, TotalSigningKeys: maxKeysPerOperator},
			{Index: 4, TotalSigningKeys: maxKeysPerOperator},
			{Index: 5, TotalSigningKeys: 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			node := &keysNode{}
			server := httptest.NewServer(node)
			defer server.Close()
			client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})

			err := (&CuratedReader{}).Keys(context.Background(), client, eth.BlockRef{Tag: "latest"},
				moduleUnderTest, operators, nil, func(model.Key) error { return nil })
			if err == nil {
				t.Fatal("got no error, want the totals refused")
			}
			if got := client.Stats().Requests; got != 0 {
				t.Errorf("%d requests went out, want none", got)
			}
		})
	}
}

// A provider that truncates return data used to publish a version with keys silently missing.
func TestAKeyReadMustReturnExactlyWhatWasAskedFor(t *testing.T) {
	operators := []model.Operator{
		{Index: 0, TotalSigningKeys: 1500, StakingLimit: 1500},
		{Index: 1, TotalSigningKeys: 3, StakingLimit: 3},
	}
	for name, delta := range map[string]int{"fewer": -1, "more": 1} {
		t.Run(name, func(t *testing.T) {
			node := &keysNode{delta: map[int32]int{1: delta}}
			server := httptest.NewServer(node)
			defer server.Close()
			client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})

			var emitted int
			err := (&CuratedReader{KeysBatch: 1000}).Keys(context.Background(), client, eth.BlockRef{Tag: "latest"},
				moduleUnderTest, operators, nil, func(model.Key) error { emitted++; return nil })
			if err == nil {
				t.Fatalf("got %d keys and no error, want the short read refused", emitted)
			}
			if !strings.Contains(err.Error(), "asked for 3") {
				t.Errorf("error %q does not say what was asked for", err)
			}
		})
	}
}

func TestAPanicInAKeyReadGoroutineBecomesAnError(t *testing.T) {
	node := &keysNode{}
	server := httptest.NewServer(node)
	defer server.Close()
	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})

	spec := keysSpec{abi: &eth.RegistryABI, method: "getSigningKeys",
		decode: func([]any, string, model.Operator, int32) ([]model.Key, error) { panic("decoder bug") }}
	err := streamKeys(context.Background(), client, eth.BlockRef{Tag: "latest"}, moduleUnderTest,
		[]model.Operator{{Index: 0, TotalSigningKeys: 2}}, nil, spec, keysPacing{}, nil,
		func(model.Key) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "decoder bug") {
		t.Fatalf("got %v, want the panic as an error", err)
	}
}

func TestEveryOperatorMustYieldItsTotalSigningKeys(t *testing.T) {
	// Operator 1 is listed twice, so its keys are read twice and its sum is off.
	node := &keysNode{}
	server := httptest.NewServer(node)
	defer server.Close()
	client := eth.NewClient(server.URL, eth.Options{Concurrency: 2, BatchSize: 8})

	operators := []model.Operator{{Index: 1, TotalSigningKeys: 2}, {Index: 1, TotalSigningKeys: 2}}
	err := (&CuratedReader{}).Keys(context.Background(), client, eth.BlockRef{Tag: "latest"},
		moduleUnderTest, operators, nil, func(model.Key) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "totalSigningKeys is 2") {
		t.Fatalf("got %v, want the per-operator sum refused", err)
	}
}

func TestCountOfRejectsWhatIntOfWouldWrap(t *testing.T) {
	for _, tc := range []struct {
		v  any
		ok bool
	}{
		{big.NewInt(0), true},
		{big.NewInt(100), true},
		{big.NewInt(-1), false},
		{new(big.Int).Lsh(big.NewInt(1), 64), false},
		{(*big.Int)(nil), false},
		{uint64(math.MaxUint64), false},
		{uint32(math.MaxUint32), false},
		{uint8(7), true},
		{int64(-5), false},
		{"7", false},
	} {
		_, err := countOf(tc.v, 1000)
		if (err == nil) != tc.ok {
			t.Errorf("countOf(%v) err = %v, want ok %t", tc.v, err, tc.ok)
		}
	}
}
