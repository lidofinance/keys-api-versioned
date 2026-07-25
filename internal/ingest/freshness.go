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
}

// Decision carries the action and why it was taken, so the reason can be logged
// and shown rather than guessed at later.
type Decision struct {
	Action         Action
	Reason         string
	ChangedModules []string
}

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
		return Decision{Action: ActionSupersede, Reason: "reorg detected"}
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
		}
	}

	if maxBlockGap > 0 && head.BlockNumber-v.BlockNumber >= maxBlockGap {
		return Decision{
			Action: ActionRebuild,
			Reason: fmt.Sprintf("block gap %d is at or above the limit %d",
				head.BlockNumber-v.BlockNumber, maxBlockGap),
		}
	}

	if head.OperatorEvents {
		return Decision{Action: ActionRebuild, Reason: "operator events in range"}
	}

	return Decision{
		Action: ActionExtend,
		Reason: fmt.Sprintf("no change up to block %d", head.BlockNumber),
	}
}
