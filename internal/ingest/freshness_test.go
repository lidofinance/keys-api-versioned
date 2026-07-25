package ingest

import "testing"

func version() Version {
	return Version{
		BlockNumber:  4_200_000,
		BlockHash:    "0xaa",
		ModuleNonces: map[string]int64{"0x01": 10, "0x02": 20},
	}
}

func head() HeadState {
	return HeadState{
		BlockNumber:  4_200_005,
		BlockHash:    "0xbb",
		ModuleNonces: map[string]int64{"0x01": 10, "0x02": 20},
	}
}

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*HeadState)
		want    Action
		modules int
	}{
		{
			name:   "nothing changed",
			mutate: func(*HeadState) {},
			want:   ActionExtend,
		},
		{
			name:   "reorg wins over everything",
			mutate: func(h *HeadState) { h.Reorg = true; h.ModuleNonces["0x01"] = 99 },
			want:   ActionSupersede,
		},
		{
			name:    "one nonce moved",
			mutate:  func(h *HeadState) { h.ModuleNonces["0x02"] = 21 },
			want:    ActionRebuild,
			modules: 1,
		},
		{
			name:    "a new module appeared",
			mutate:  func(h *HeadState) { h.ModuleNonces["0x03"] = 1 },
			want:    ActionRebuild,
			modules: 1,
		},
		{
			name:    "a known module vanished",
			mutate:  func(h *HeadState) { delete(h.ModuleNonces, "0x02") },
			want:    ActionRebuild,
			modules: 1,
		},
		{
			name:   "gap at the limit",
			mutate: func(h *HeadState) { h.BlockNumber = 4_200_000 + 100 },
			want:   ActionRebuild,
		},
		{
			name:   "operator events",
			mutate: func(h *HeadState) { h.OperatorEvents = true },
			want:   ActionRebuild,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := head()
			tc.mutate(&h)

			got := Evaluate(version(), h, 100)
			if got.Action != tc.want {
				t.Fatalf("action = %q (%s), want %q", got.Action, got.Reason, tc.want)
			}
			if tc.modules > 0 && len(got.ChangedModules) != tc.modules {
				t.Fatalf("changed modules = %v, want %d", got.ChangedModules, tc.modules)
			}
			if got.Reason == "" {
				t.Fatal("decision carries no reason")
			}
		})
	}
}
