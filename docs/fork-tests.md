# Testing against a fork

`internal/fork` runs the whole service — the chain reader, the loop, the writer, the
store and the HTTP surface — against an anvil fork of Hoodi at a pinned block, with the
registry mutated on purpose.

```bash
FORK_UPSTREAM_RPC=http://…-hoodi-geth… make test-fork
```

## What the other two levels cannot do

| | `go test ./...` and `internal/ingest` | `internal/e2e` | `internal/fork` |
|---|---|---|---|
| Postgres | no | yes | yes |
| The sync loop | yes, against a fake store | no | yes |
| A `source.Provider` | a scripted double | none at all | the real one |
| Contract semantics | none — the nonce is an `int32` with no link to any key | none | real |
| A branch with the same height and different content | no | no | yes |
| `eth_getLogs` topics | no | no | yes |
| Cost | milliseconds | 8 s | ~2 s per scenario once the cache is warm |

Three consequences of that gap, all of them found while writing this package:

- **The loop had never run against Postgres.** `ingest.Syncer` is constructed in exactly
  two places, `sync_test.go` (with a fake store) and `cmd/keys-api`. `internal/e2e`
  drives the writer directly.
- **The operator-event scan had never run at all.** The scripted double does not
  implement `source.OperatorWatcher`, so `operatorEvents` short-circuits at
  `sync.go:490` and the log scan, the "assume it changed" fallback at `sync.go:236` and
  the rule at `freshness.go:117` were dead in tests. That is the path
  [next-steps.md](next-steps.md) flags as the one where a mistake is invisible: a wrong
  event signature yields a wrong topic hash, which returns no logs and looks exactly
  like "nothing changed".
- **Contract semantics contradict a stated assumption.** `source.go:33` says key
  identity is `(operator index, key index)` and is "stable for the life of the key".
  `removeSigningKeys` moves the last key into the vacated slot.

## What it found

**A reader could report a current version as superseded, or the reverse.**
`appSnapshot.stillOnChain` — the re-check that runs at the start of every query
continuing a response — looked the version up by block number:

```sql
select status from versions where block_number = $1
```

Two versions share a height whenever a reorg is followed by a rebuild: the one the
losing branch published, now superseded, and the one the winning branch publishes at the
same height. The query picked between them arbitrarily. The failure it produces is a 409
(or an aborted body, past the first flush) on a version that is perfectly current — and
the same coin flip can hide a genuinely superseded one. Fixed by naming the version row
rather than its height, and controlled: with the old query
`TestAReorgThatSwapsAKeyIsNotServedFromTheLosingBranch` fails with exactly that
signature.

It is worth noting why no existing test reached it. `internal/e2e` supersedes versions
and then publishes the next one at a *different* block, because its blocks are handed
out by a counter. Only a real chain re-mines a height.

**A cold start reported `VersionBlock` 0.** Every other rebuild path sets it; the
cold-start branch set the timestamp and not the block, so `published_version_block` and
`blocks_behind_head` stayed unset for one interval after the longest build the service
ever does. One line, `sync.go`.

## The two reorg primitives are not interchangeable

Measured here rather than taken from the documentation, because picking the wrong one
produces a test that proves nothing:

- **`anvil_reorg(depth, txs)`** re-mines the rewound blocks **with their original
  transactions**. Block hashes change and the head keeps moving forward — which is what
  a node reports during a real reorg — but the state does not diverge. Right for "a
  short reorg that touched nothing of ours", wrong for a branch that has to hold
  different keys.
- **`evm_snapshot` / `evm_revert`** restores the state, so the new branch can be built
  with different content. The cost is that the head jumps backwards to the snapshot,
  which the loop reads as a lagging endpoint (`sync.go:165`) rather than a fork, so the
  scenario has to mine back up to the old height first.

`anvil_reorg` also only rewrites blocks anvil produced itself. Reaching the fork
boundary fails with a bare `Resource not found`; one local block above the fork is not
enough, two are. `Anvil.Reorg` checks that and says so.

## What is bounded, and why

Hoodi's module 1 holds **67 242 keys** at the pinned block and CSM has 467 operators. A
fork serves the first read of either by pulling hundreds of thousands of storage slots
out of the upstream node — slow, and hard enough on a shared node to draw 502s, which
happened while this was being measured.

So the harness wraps the provider in a decorator that keeps one module. Sandbox is a
real `curated-onchain-v1` registry with four operators and thirty-six keys, so every
semantic under test is reachable for a thousandth of the traffic. The decorator lives in
the test package rather than being a service option, and it forwards the optional
interfaces — a decorator that swallowed `OperatorWatcher` would turn the event scan off
without saying so, which is the one path this package exists to exercise.

**What that costs:** these scenarios say nothing about a cycle handling several modules
at once. A scenario about the module set has to widen the filter (`HarnessOpts.Modules`,
or `AllModules`) and pay for it.

## Keys that describe themselves, on a real registry

`internal/testreg`'s leverage is that a generated pubkey carries the module, operator,
index and content generation it belongs to, so a wrong key fails on the key alone. That
looked like it could not survive the move to a chain.

It survives: `SigningKeys.saveKeysSigs` checks the array lengths and that the key is not
all zero, and nothing else — BLS validity is the deposit contract's business, not the
registry's. So a fork test injects keys it can recognise into the real contract, and
`MintKey` reuses testreg's layout.

## The scenarios

Implemented, in `internal/fork`. Each one that asserts a positive has a control that
fails on the same behaviour, which is the rule this repo already paid to learn
(next-steps.md:158).

| Scenario | How it is built | Control |
|---|---|---|
| The loop publishes a version from the chain | one cycle, cold | a quiet second cycle must extend, not rebuild |
| A reorg swaps a key for a different one at the same height and nonce | `evm_revert`, then re-add a different minted key | the same script with the nonce differing rebuilds correctly |
| A reorg whose only change was dropped costs no key reads | `evm_revert`, mine back to the old height | the swap scenario above, which does re-read |
| A reorg above the published version is not even noticed | `anvil_reorg` over the tip | the tip hash must change and the version's must not |
| A rename is picked up though it moves no nonce | `setNodeOperatorName` | the same run with `OperatorWatcher` hidden must *not* notice |
| Removing a key moves the last key into its slot | `removeSigningKeys` mid-array | the contract's own read has to show the swap before the service is asked |
| A deposit flips `used` on exactly one key | vet, then `obtainDepositData` as the router | the operator's own counter must move first |

Not yet built, in rough order of value:

- **A module whose `getType` is unrecognised wedges the loop.** `ErrUnsupportedModule`
  (`provider.go:273`) is caught nowhere: the log scan fails, `sync.go:236` assumes a
  change, the rebuild hits the same module and fails, and the previous version — marked
  stale at `sync.go:281` — stays stale forever while the cycle fails every interval.
  Needs `addStakingModule` plus a stub at a fresh address.
- **A lagging or lying endpoint.** A second anvil forked ten blocks earlier, added to the
  endpoint list. Two branches that no test reaches today come alive: `BlockHashAt`
  returning `""` (`sync.go:449`) and the head-behind-version guard (`sync.go:165`). It
  also reproduces the hosted-provider case recorded in next-steps.md, where `Head` is
  answered by one node and the follow-up read routed to another.
- **A reorg deeper than the retention window**, which is the one case that forces a
  defined full rebuild.
- **A reorg during a rebuild.** Reads are pinned with `requireCanonical: false`, so a
  version that spans a reorg is fully self-consistent on a dead branch, caught only on
  the next cycle.
- **A module status change on the router** — `DepositsPaused`, `Stopped`. It moves no
  module nonce, because the counter is in the module and the status is in the router, so
  it is invisible until the gap audit.
- **`META_REGISTRY` error conflation** (`community.go:163`): a transient failure at the
  first CMv2 read is indistinguishable from "this module has no registry", and the empty
  result is cached for the life of the process.
- **A key getter returning fewer keys than asked.** Nothing compares the returned count
  against the span, so the writer closes the missing rows as deletions. Needs a stub
  module.
- **Multicall3 edge cases**: absent, some other contract at the address (the probe only
  checks that the code is non-empty, `multicall.go:88`), out-of-gas and the bisection
  ladder, and a batch capped at three the way a metered provider caps it.

## The harness

- `anvil.go` — starting a node, mining, snapshots, reorgs, `setCode`, sending a
  transaction from an impersonated sender. `--auto-impersonate` removes signing
  entirely: every mutation is sent from an address that already holds the role on the
  forked chain.
- `registry.go` — the write half of the registry ABI and the mutations, plus `grant`,
  which hands the test sender an Aragon role by impersonating that role's *permission
  manager*. A scenario therefore never has to know which committee holds a role today.
- `keys.go` — minting recognisable keys.
- `service.go` — assembling the service and running one cycle at a time.

Two things the harness does deliberately:

**The ticker is never started.** Tests call `Cycle` when they want one. That is what
makes "the rename landed in the block after the version" a statement rather than a race,
and it is the same trick that makes `internal/e2e` deterministic, moved to the chain
side. anvil runs with `--no-mining` for the same reason.

**Every scenario asserts its preconditions** (`requireSandboxPreconditions`). Upstream
drift or a re-pin has to fail loudly rather than quietly turning a scenario into a test
of something else.

## Re-pinning the fork block

`FORK_BLOCK` defaults to 3 290 000. Changing it:

- invalidates the anvil cache for the old block, so the first run pulls state from
  upstream again — minutes, and enough load to draw 502s from a shared node;
- invalidates every precondition. Sandbox's operator count, operator 1's thirty keys and
  its zero deposited keys are constants in `fork.go`. Re-read them at the new block and
  update them; the preconditions will say which ones moved.

The upstream node must be an archive node — the scenarios read state at a block far
behind head. The Hoodi tooling node used here serves state at `head - 1 000 000`.
