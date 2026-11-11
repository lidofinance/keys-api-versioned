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

// Groups exist only for curated-onchain-v2, so /ext/v1 says three things: a group, no group, and
// groups do not exist here. /v1 has no such field.

// Operator 1 of module 1 is in no group: the on-chain NO_GROUP_ID sentinel, which must not read as 0.
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
		// 0 is a real on-chain value naming no group.
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

	// No group registry: the field is absent, since null would claim the operators were left out.
	for i, operator := range operatorsAt(t, srv, "/ext/v1/modules/2/operators") {
		if _, ok := operator["groupId"]; ok {
			t.Errorf("operator %d of a module without groups carries groupId", i)
		}
	}
}

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

// A consumer picking the combined stream over the operator endpoint must not get less.
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

	// The streams marshal operators on their own path.
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

// The operator diff must compare the nullable group with `is not distinct from`: forget it and the
// change is lost, use `=` and every operator of a group-less module churns.
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

	// A null-vs-null `=` comparison would rewrite all of module 2, invisibly in a response.
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

// Raw JSON objects, so a field's absence is distinguishable from its zero value.
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
