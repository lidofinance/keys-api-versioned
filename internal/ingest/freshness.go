package ingest

import (
	"fmt"
	"sort"
	"strings"
)

// Freshness decides, once per polling cycle, whether the published version still
// reflects the chain — and it decides it from cheap signals, without re-reading
// a single key.
//
// The signals are the ones the canonical service already uses to move
// lastChangedBlockHash (staking-module-updater.service.ts): a module's nonce
// changing, a reorg, too large a gap since the last observed block, and operator
// events in the range. If none of them fired, the published version is still
// current and only its observed block advances — which is exactly what the
// canonical service does when it keeps lastChangedBlockHash and updates the
// block meta.

// Action is what the poller should do with the currently published version.
type Action string

const (
	// ActionExtend means nothing changed: advance the version's observed block
	// and leave its data and lastChangedBlockHash alone.
	ActionExtend Action = "extend"
	// ActionRebuild means the version no longer reflects the chain: mark it
	// stale, build a new version, publish it.
	ActionRebuild Action = "rebuild"
	// ActionSupersede means the chain moved off the branch this version was
	// built on: take it off, then rebuild.
	ActionSupersede Action = "supersede"
)

// Version is what the store already holds.
type Version struct {
	BlockNumber int64
	BlockHash   string
	// ModuleNonces is the per-module nonce observed when the version was built.
	ModuleNonces map[string]int64
}

// HeadState is what one polling cycle reads from the execution layer. Every
// field here is cheap: module nonces, a block header, an event filter.
type HeadState struct {
	BlockNumber int64
	BlockHash   string
	// Reorg is the outcome of the parent-hash walk the canonical service does.
	Reorg bool
	// ModuleNonces are the nonces at head.
	ModuleNonces map[string]int64
	// OperatorEvents reports operator changes in (version block, head block].
	OperatorEvents bool
	// SourceChange names a change in what the registry is being read from — a
	// different staking router, or a contract version that makes the same tuple mean
	// something else — and is empty when there was none.
	//
	// It is not covered by any of the other signals, which is the reason it exists. A
	// replacement router can report the same modules with the same nonces, and then
	// every cheap signal says nothing changed while the module rows in the published
	// version came from a contract nobody uses any more.
	SourceChange string
}

// Decision carries the action and why it was taken, so the reason can be logged
// and shown rather than guessed at later.
type Decision struct {
	Action         Action
	Reason         string
	ChangedModules []string
	// Audit is set when this rebuild was triggered by the block-gap rule rather than by
	// a signal. It exists to be measured: the audit trusts none of the cheap signals,
	// so a healthy one writes nothing, and one that writes rows is the only evidence
	// that a nonce did not move when it should have.
	Audit bool
	// Depth is how much of a module has to be read again, and it belongs with the
	// reason rather than with the caller: what changed is what says what is stale.
	// Every rebuild rewrites module rows regardless — that read is the module set,
	// which the cycle already has.
	Depth ReadDepth
}

// ReadDepth is how much of a module a rebuild re-reads.
type ReadDepth int

const (
	// DepthKeys re-reads keys and operators: a nonce moved, or nothing is trusted.
	//
	// First, so that it is the zero value. A decision that forgot to say how much is
	// stale then reads too much rather than too little — the failure that costs
	// requests instead of the one that serves wrong data.
	DepthKeys ReadDepth = iota
	// DepthOperators re-reads operators and no keys. Operator metadata moves without a
	// nonce, and a rename is not a reason to read half a million keys.
	DepthOperators
	// DepthModule re-reads neither: the module set already in hand is written and
	// everything else is inherited. What a replaced staking router or a changed
	// contract version needs — the tuple is decoded differently, while the keys behind
	// it live in the module contracts and are untouched.
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

// reads reports whether this depth includes that one.
func (d ReadDepth) reads(what ReadDepth) bool { return d <= what }

// Evaluate applies the rules in the same order the canonical updater does.
//
// maxBlockGap is not the canonical MAX_BLOCKS_OVERLAP, despite looking like it.
// There the window exists because the decision rests on an event scan, and a scan
// that falls too far behind cannot be trusted. Here the decision rests on a nonce
// read at head — a contract counter, not a scan — which cannot be missed by being
// late. So the gap rule earns its keep only as a periodic audit against a nonce that
// lied, and it should be a few thousand blocks rather than thirty: at thirty it fires
// every six minutes and re-reads half a million keys to write nothing, which was
// measured happening on mainnet.
//
// The audit is worth keeping precisely because a healthy one writes nothing: it is a
// verifiable statement that the cheap signals were telling the truth.
func Evaluate(v Version, head HeadState, maxBlockGap int64) Decision {
	if head.Reorg {
		return Decision{Action: ActionSupersede, Reason: "reorg detected", Depth: DepthKeys}
	}

	// Above the nonce rule, because it is not about a module's contents. The module rows
	// were decoded from a contract that is no longer the one in use, or
	// under a version that no longer applies, so they are rewritten from the set just
	// read. Keys and operators live in the module contracts and are not implicated: if
	// a replacement router reports different module addresses, the nonce rule below is
	// what notices, and it scopes the key read to those modules.
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
	// A module disappearing from head is a change too.
	for module := range v.ModuleNonces {
		if _, ok := head.ModuleNonces[module]; !ok {
			changed = append(changed, module)
		}
	}
	if len(changed) > 0 {
		sort.Strings(changed)
		return Decision{
			Action:         ActionRebuild,
			Reason:         "nonce changed for " + strings.Join(changed, ", "),
			ChangedModules: changed,
			Depth:          DepthKeys,
		}
	}

	if maxBlockGap > 0 && head.BlockNumber-v.BlockNumber >= maxBlockGap {
		return Decision{
			Action: ActionRebuild,
			Reason: fmt.Sprintf("block gap %d is at or above the limit %d",
				head.BlockNumber-v.BlockNumber, maxBlockGap),
			Audit: true,
			// The audit exists to catch a signal that lied, so it trusts none of them.
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
