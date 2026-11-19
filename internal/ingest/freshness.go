package ingest

import (
	"fmt"
	"sort"
	"strings"
)

type Action string

const (
	// ActionExtend advances the observed block; data and lastChangedBlockHash stay.
	ActionExtend  Action = "extend"
	ActionRebuild Action = "rebuild"
	// ActionSupersede: the version's branch is gone; take it off, then rebuild.
	ActionSupersede Action = "supersede"
)

type Version struct {
	BlockNumber  int64
	BlockHash    string
	ModuleNonces map[string]int64
}

type HeadState struct {
	BlockNumber  int64
	BlockHash    string
	Reorg        bool
	ModuleNonces map[string]int64
	// OperatorEvents reports operator changes in (version block, head block].
	OperatorEvents bool
	// SourceChange names a replaced router or contract version (empty = none). No other signal sees
	// it: a replacement router can report the same modules with the same nonces.
	SourceChange string
	// AuditedAt is the block of the last rebuild that read every key (0 = the version's block).
	AuditedAt int64
}

type Decision struct {
	Action         Action
	Reason         string
	ChangedModules []string
	// Audit: the block-gap rule is due; every module is read in full. With no nonce change a healthy
	// audit writes nothing.
	Audit bool
	Depth ReadDepth
}

type ReadDepth int

const (
	// DepthKeys is the zero value, so a decision that forgets the depth reads too much, not too little.
	DepthKeys ReadDepth = iota
	// DepthOperators: operator metadata moves without a nonce.
	DepthOperators
	// DepthModule: a replaced router or contract version decodes the tuple differently; keys are untouched.
	DepthModule
)

func (d ReadDepth) String() string {
	switch d {
	case DepthModule:
		return "modules"
	case DepthOperators:
		return "operators"
	default:
		return "keys"
	}
}

func (d ReadDepth) reads(what ReadDepth) bool { return d <= what }

// Evaluate applies the rules in the canonical updater's order. maxBlockGap only audits a nonce
// that lied, so it is thousands of blocks: at 30 it re-read all keys every 6 minutes on mainnet.
//
// The gap counts from the last full read, not from the version: every rebuild moves the version,
// and on mainnet nonces move more often than every maxBlockGap blocks, so a gap from the version
// never came due and a deposited key kept unread was never checked again.
func Evaluate(v Version, head HeadState, maxBlockGap int64) Decision {
	audited := head.AuditedAt
	if audited == 0 {
		audited = v.BlockNumber
	}
	gap := head.BlockNumber - audited
	auditDue := maxBlockGap > 0 && gap >= maxBlockGap

	if head.Reorg {
		return Decision{Action: ActionSupersede, Reason: "reorg detected", Depth: DepthKeys}
	}

	// Above the nonce rule: the module rows came from a contract no longer in use; keys are not implicated.
	if head.SourceChange != "" {
		return Decision{Action: ActionRebuild, Reason: head.SourceChange, Depth: DepthModule}
	}

	var changed []string
	for module, nonce := range head.ModuleNonces {
		known, ok := v.ModuleNonces[module]
		if !ok {
			changed = append(changed, module)
			continue
		}
		if known != nonce {
			changed = append(changed, module)
		}
	}
	for module := range v.ModuleNonces {
		if _, ok := head.ModuleNonces[module]; !ok {
			changed = append(changed, module)
		}
	}
	if len(changed) > 0 {
		sort.Strings(changed)
		reason := "nonce changed for " + strings.Join(changed, ", ")
		if auditDue {
			reason += fmt.Sprintf("; block gap %d since the last full read is at or above the limit %d", gap, maxBlockGap)
		}
		return Decision{
			Action:         ActionRebuild,
			Reason:         reason,
			ChangedModules: changed,
			Audit:          auditDue,
			Depth:          DepthKeys,
		}
	}

	if auditDue {
		return Decision{
			Action: ActionRebuild,
			Reason: fmt.Sprintf("block gap %d since the last full read is at or above the limit %d",
				gap, maxBlockGap),
			Audit: true,
			Depth: DepthKeys,
		}
	}

	if head.OperatorEvents {
		return Decision{Action: ActionRebuild, Reason: "operator events in range", Depth: DepthOperators}
	}

	return Decision{
		Action: ActionExtend,
		Reason: fmt.Sprintf("no change up to block %d", head.BlockNumber),
		Depth:  DepthModule,
	}
}
