package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Canonical streams keys by (operatorIndex, index). Without an ORDER BY they came in heap order, and
// a changed key's new row sits at the end of the heap: module 5 on mainnet came out reshuffled.
func TestKeysStreamInCanonicalOrderAfterARewrite(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 3, KeysPerOperator: 4}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	spec.WithUsed(func(_, operator, index int) bool { return operator == 0 && index == 0 }).
		Publish(ctx, t, db.Writer, testreg.At(db.Block(1)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))
	for _, path := range []string{"/v1/keys", "/v1/modules/1/keys", "/v1/modules/1/operators/keys"} {
		resp := srv.Get(t, path)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var doc struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		var keys []struct {
			OperatorIndex int `json:"operatorIndex"`
			Index         int `json:"index"`
		}
		var nested struct {
			Keys json.RawMessage `json:"keys"`
		}
		raw := doc.Data
		if json.Unmarshal(raw, &nested) == nil && nested.Keys != nil {
			raw = nested.Keys
		}
		if err := json.Unmarshal(raw, &keys); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if len(keys) != spec.TotalKeys() {
			t.Fatalf("%s: %d keys, want %d", path, len(keys), spec.TotalKeys())
		}
		for i := 1; i < len(keys); i++ {
			a, b := keys[i-1], keys[i]
			if a.OperatorIndex > b.OperatorIndex || (a.OperatorIndex == b.OperatorIndex && a.Index >= b.Index) {
				t.Errorf("%s: key %d.%d comes after %d.%d", path, b.OperatorIndex, b.Index, a.OperatorIndex, a.Index)
				break
			}
		}
	}
}

// int32(4294967296) is 0: the filter served operator 0's keys, the path served operator 0.
func TestAnOperatorIndexBeyondInt32IsNotOperatorZero(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	testreg.Spec{Modules: 1, OperatorsPerModule: 2, KeysPerOperator: 2}.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))

	for path, want := range map[string]int{
		"/v1/modules/1/operators/4294967296":                                       http.StatusNotFound,
		"/v1/keys?operatorIndex=4294967296":                                        http.StatusBadRequest,
		"/v1/modules/1/keys?operatorIndex=4294967296":                              http.StatusBadRequest,
		fmt.Sprintf("/ext/v1/keys?limit=10&cursor=%d.1.4294967296.0", db.Block(0)): http.StatusBadRequest,
	} {
		resp := srv.Get(t, path)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s: %d %.120s, want %d", path, resp.StatusCode, body, want)
		}
	}
}
