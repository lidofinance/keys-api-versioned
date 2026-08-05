package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Node operator groups, end to end: written by the writer, read back through HTTP.
//
// Groups exist for one module family (curated-onchain-v2, whose MetaRegistry holds
// them) and for no other, so the surface has to say three different things — a
// group, "in no group", and "groups do not exist here" — and say them only on
// /ext/v1, because /v1 is the canonical service's contract and it has no such field.

// groupedRegistry is two modules: module 1 has groups, module 2 has none. Operator 1
// of module 1 is in no group, which on chain is the NO_GROUP_ID sentinel — the case
// that must not come out as a group called 0.
func groupedRegistry() testreg.Spec {
	spec := testreg.Spec{Modules: 2, OperatorsPerModule: 3, KeysPerOperator: 5}
	return spec.WithGroups(func(module, operator int) *model.GroupID {
		if module != 1 {
			return nil
		}
		id := model.GroupID(7)
		switch operator {
		case 1:
			id = model.NoGroupID
		case 2:
			id = model.GroupID(4)
		}
		return &id
	})
}

func TestTheExtendedSurfaceServesNodeOperatorGroups(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := groupedRegistry()
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	operators := operatorsAt(t, srv, "/ext/v1/modules/1/operators")
	if len(operators) != 3 {
		t.Fatalf("got %d operators, want 3", len(operators))
	}

	for _, want := range []struct {
		operator int
		present  bool
		value    string
	}{
		{0, true, "7"},
		// The sentinel, and the whole reason the field is not a plain integer: 0 is
		// a real on-chain value that names no group.
		{1, true, "null"},
		{2, true, "4"},
	} {
		raw, ok := operators[want.operator]["groupId"]
		if ok != want.present {
			t.Fatalf("operator %d: groupId present = %t, want %t", want.operator, ok, want.present)
		}
		if got := string(raw); got != want.value {
			t.Errorf("operator %d: groupId is %s, want %s", want.operator, got, want.value)
		}
	}

	// A module with no group registry does not grow the field at all. `null`
	// everywhere would read as "these operators were left out of a group", which is
	// a different and false statement.
	for i, operator := range operatorsAt(t, srv, "/ext/v1/modules/2/operators") {
		if _, ok := operator["groupId"]; ok {
			t.Errorf("operator %d of a module without groups carries groupId", i)
		}
	}
}

// The canonical surface is the canonical service's contract, and it has no group
// field. A consumer repointed here must not be able to tell — which is also why the
// positive twin above exists: without it this would pass on a service that never
// read groups at all.
func TestTheCanonicalSurfaceNeverServesGroups(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := groupedRegistry()
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	for _, path := range []string{
		"/v1/operators",
		"/v1/modules/1/operators",
		"/v1/modules/1/operators/0",
		"/v1/modules/1/operators/keys",
		"/v2/modules/operators/keys",
	} {
		resp := srv.Get(t, path)
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d, want 200 — body %s", path, resp.StatusCode, body)
		}
		if strings.Contains(body, "groupId") {
			t.Errorf("GET %s carries groupId on the canonical surface", path)
		}
	}
}

// Every place an operator is rendered has to agree, or a consumer picking the
// combined stream over the operator endpoint silently gets less.
func TestGroupsReachEveryOperatorRepresentation(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := groupedRegistry()
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))
	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))

	single := srv.Get(t, "/ext/v1/modules/1/operators/0")
	singleBody := readBody(t, single)
	if single.StatusCode != http.StatusOK {
		t.Fatalf("GET the single operator: status %d — body %s", single.StatusCode, singleBody)
	}
	var one struct {
		Data struct {
			Operator map[string]json.RawMessage `json:"operator"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(singleBody), &one); err != nil {
		t.Fatalf("decode the single operator: %v — body %s", err, singleBody)
	}
	if got := string(one.Data.Operator["groupId"]); got != "7" {
		t.Errorf("the single-operator endpoint reports groupId %q, want 7", got)
	}

	// /ext/v1/operators groups by module; module 1 comes first.
	all := srv.Get(t, "/ext/v1/operators")
	allBody := readBody(t, all)
	if all.StatusCode != http.StatusOK {
		t.Fatalf("GET every operator: status %d — body %s", all.StatusCode, allBody)
	}
	var every struct {
		Data []struct {
			Operators []map[string]json.RawMessage `json:"operators"`
			Module    struct {
				ID int `json:"id"`
			} `json:"module"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(allBody), &every); err != nil {
		t.Fatalf("decode every operator: %v", err)
	}
	seen := false
	for _, group := range every.Data {
		for i, operator := range group.Operators {
			raw, ok := operator["groupId"]
			if group.Module.ID != 1 {
				if ok {
					t.Errorf("module %d operator %d carries groupId", group.Module.ID, i)
				}
				continue
			}
			if !ok {
				t.Fatalf("module 1 operator %d carries no groupId", i)
			}
			if i == 0 && string(raw) != "7" {
				t.Errorf("module 1 operator 0 reports groupId %s, want 7", raw)
			}
			seen = true
		}
	}
	if !seen {
		t.Fatal("/ext/v1/operators returned no operators for module 1")
	}

	// The two streams, which marshal operators on their own path.
	for _, path := range []string{
		"/ext/v1/modules/1/operators/keys",
		"/ext/v1/modules/operators/keys",
	} {
		resp := srv.Get(t, path)
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d — body %s", path, resp.StatusCode, body)
		}
		if !strings.Contains(body, `"groupId":7`) {
			t.Errorf("GET %s carries no group for module 1", path)
		}
		if !strings.Contains(body, `"groupId":null`) {
			t.Errorf("GET %s never reports the ungrouped operator", path)
		}
	}
}

// A group edit is one operator row, not a rewrite of the module. The writer diffs
// operators field by field, and a nullable column added to that comparison is the
// easy thing to get wrong in both directions: forget it and the change is lost,
// compare it with `=` instead of `is not distinct from` and every operator of a
// group-less module churns on every rebuild.
func TestAGroupChangeIsPickedUpAndCostsOneRow(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := groupedRegistry()
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	moved := spec.WithGroups(func(module, operator int) *model.GroupID {
		if module != 1 {
			return nil
		}
		id := model.GroupID(7)
		if operator == 1 {
			// The operator that was in no group joins one: null → 9.
			id = model.GroupID(9)
		}
		if operator == 2 {
			id = model.GroupID(4)
		}
		return &id
	})

	build := moved.Fill(ctx, t, db.Writer, testreg.At(db.Block(1)), moved.ModuleIDs()...)
	if err := build.Publish(ctx, "latest", 0); err != nil {
		t.Fatalf("publish the second version: %v", err)
	}

	srv := testpg.NewServer(t, db.Reader(t, "latest", 3))
	operators := operatorsAt(t, srv, "/ext/v1/modules/1/operators")
	if got := string(operators[1]["groupId"]); got != "9" {
		t.Errorf("operator 1 reports groupId %s after joining group 9", got)
	}
	if got := string(operators[0]["groupId"]); got != "7" {
		t.Errorf("operator 0 reports groupId %s, want 7 — it did not move", got)
	}

	// Module 2 has no groups in either version, so nothing about it may have been
	// rewritten. A null-vs-null comparison done with `=` closes and reopens all of
	// it, which is invisible in a response and visible here.
	var rows int
	err := db.Writer.Pool().QueryRow(ctx, `
		select count(*) from operator_version where module_id = 2 and from_block > $1`,
		db.Block(0)).Scan(&rows)
	if err != nil {
		t.Fatalf("count module 2 operator rows: %v", err)
	}
	if rows != 0 {
		t.Errorf("%d operator rows of the group-less module were rewritten, want 0", rows)
	}
}

// operatorsAt reads a module's operators as raw JSON objects, so a field's absence
// is distinguishable from its zero value.
func operatorsAt(t *testing.T, srv *testpg.Server, path string) []map[string]json.RawMessage {
	t.Helper()
	resp := srv.Get(t, path)
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200 — body %s", path, resp.StatusCode, body)
	}
	var payload struct {
		Data struct {
			Operators []map[string]json.RawMessage `json:"operators"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode %s: %v — body %s", path, err, body)
	}
	return payload.Data.Operators
}
