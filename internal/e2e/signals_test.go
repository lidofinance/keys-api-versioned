package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
)

// CSM and CMv2 can lower an operator's vetted count without moving the nonce. The operator event
// rebuild reads no keys, so the flags follow the new staking limit in the store, and the version
// before keeps the old ones.
func TestLoweringAStakingLimitUnvetsKeptKeysWithoutReadingThem(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSignalSource(db.Block(0))
	syncer := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)

	if r := syncer.Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}
	before := src.block

	src.limits[[2]int{1, 0}] = 2
	src.events = true
	r := syncer.Cycle(ctx)
	if r.Err != nil || !r.Rebuilt {
		t.Fatalf("event cycle: rebuilt %t (%s), err %v", r.Rebuilt, r.Reason, r.Err)
	}
	if r.KeysStaged != 0 {
		t.Errorf("read %d keys, want none: only the operators moved", r.KeysStaged)
	}
	if r.KeysChanged != 2 {
		t.Errorf("%d key rows changed, want 2: keys 2 and 3 of operator 0", r.KeysChanged)
	}
	assertVetted(t, db, "latest", src)
	assertVetted(t, db, fmt.Sprint(before), newSignalSource(0))

	src.events = false
	if r := syncer.Cycle(ctx); r.Err != nil || r.Action != ingest.ActionExtend {
		t.Fatalf("next cycle: action %q (%s), err %v; want extend", r.Action, r.Reason, r.Err)
	}
}

// The router fields of the published rows are read from the store after a restart: a status change
// while the writer was down moves no nonce either.
func TestARouterFieldChangeIsSeenAcrossARestart(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()
	src := newSignalSource(db.Block(0))

	if r := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil).Cycle(ctx); r.Err != nil {
		t.Fatalf("cold sync: %v", r.Err)
	}

	// Control: a restart over unchanged rows rebuilds nothing.
	if r := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil).Cycle(ctx); r.Err != nil || r.Action != ingest.ActionExtend {
		t.Fatalf("restart without a change: action %q (%s), err %v; want extend", r.Action, r.Reason, r.Err)
	}

	src.status[2] = 1
	restarted := ingest.NewSyncer(src, db.Writer, ingest.SyncConfig{}, nil)
	r := restarted.Cycle(ctx)
	if r.Err != nil || r.Action != ingest.ActionRebuild || !strings.Contains(r.Reason, "router fields") {
		t.Fatalf("restart after a status change: action %q (%s), err %v; want a rebuild for the router fields",
			r.Action, r.Reason, r.Err)
	}
	if r.KeysStaged != 0 {
		t.Errorf("read %d keys, want none", r.KeysStaged)
	}
	var status int32
	var active bool
	if err := db.Writer.Pool().QueryRow(ctx,
		`select status, active from module_version where module_id = 2 and to_block is null`).Scan(&status, &active); err != nil {
		t.Fatalf("module row: %v", err)
	}
	if status != 1 || active {
		t.Errorf("module 2 row: status %d, active %t; want 1 and false", status, active)
	}

	if r := restarted.Cycle(ctx); r.Err != nil || r.Action != ingest.ActionExtend {
		t.Fatalf("next cycle: action %q (%s), err %v; want extend", r.Action, r.Reason, r.Err)
	}
}

// signalSource adds what moves no nonce: operator staking limits behind an event, and router status.
type signalSource struct {
	*depositSource
	limits map[[2]int]int
	status map[int]int32
	events bool
}

func newSignalSource(block int64) *signalSource {
	return &signalSource{
		depositSource: newDepositSource(block, map[[2]int]int{}),
		limits:        map[[2]int]int{},
		status:        map[int]int32{},
	}
}

func (s *signalSource) limit(module, operator int) int {
	if n, ok := s.limits[[2]int{module, operator}]; ok {
		return n
	}
	return s.shape.KeysPerOperator
}

func (s *signalSource) Modules(ctx context.Context, at source.Ref) ([]model.StakingModule, error) {
	modules, err := s.depositSource.Modules(ctx, at)
	for i := range modules {
		if status, ok := s.status[int(modules[i].ID)]; ok {
			modules[i].Status, modules[i].Active = status, status == 0
		}
	}
	return modules, err
}

func (s *signalSource) Operators(ctx context.Context, at source.Ref, m model.StakingModule) ([]model.Operator, error) {
	operators, err := s.depositSource.Operators(ctx, at, m)
	for i := range operators {
		operators[i].StakingLimit = int32(s.limit(int(m.ID), int(operators[i].Index)))
	}
	return operators, err
}

func (s *signalSource) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	_, err := s.KeysFrom(ctx, at, m, nil, fn)
	return err
}

func (s *signalSource) KeysFrom(ctx context.Context, at source.Ref, m model.StakingModule, from map[int32]int32, fn func(model.Key) error) (map[int32]int32, error) {
	return s.depositSource.KeysFrom(ctx, at, m, from, func(k model.Key) error {
		k.Vetted = int(k.Index) < s.limit(int(m.ID), int(k.OperatorIndex))
		return fn(k)
	})
}

func (s *signalSource) OperatorsChanged(context.Context, model.StakingModule, int64, int64) (bool, error) {
	return s.events, nil
}

func (s *signalSource) VettedBelowStakingLimit() {}

var (
	_ source.OperatorWatcher     = (*signalSource)(nil)
	_ source.StakingLimitVetting = (*signalSource)(nil)
)

// assertVetted: every key a version serves carries the flag its operator's limit gives.
func assertVetted(t *testing.T, db *testpg.DB, pin string, src *signalSource) {
	t.Helper()
	ctx := context.Background()
	spec := src.spec()
	served := 0
	for _, id := range spec.ModuleIDs() {
		rows, err := db.Writer.Pool().Query(ctx, `
			select k.operator_index, k.key_index, k.vetted
			  from key_version k, versions v
			 where v.block_number = (case when $2 = 'latest'
			                              then (select max(block_number) from versions where status = 'published')
			                              else $2::bigint end)
			   and k.module_id = $1 and k.from_block <= v.block_number
			   and (k.to_block is null or k.to_block > v.block_number)`, id, pin)
		if err != nil {
			t.Fatalf("keys of module %d at %s: %v", id, pin, err)
		}
		for rows.Next() {
			var operator, index int
			var vetted bool
			if err := rows.Scan(&operator, &index, &vetted); err != nil {
				t.Fatal(err)
			}
			served++
			if want := index < src.limit(id, operator); vetted != want {
				t.Errorf("%s: module %d operator %d key %d vetted %t, want %t", pin, id, operator, index, vetted, want)
			}
		}
		rows.Close()
	}
	if want := spec.Modules * spec.KeysPerModule(); served != want {
		t.Errorf("%s: %d key rows, want %d", pin, served, want)
	}
}
