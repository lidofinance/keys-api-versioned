# Published snapshots with `latest` / `safe` modifiers

Status: **implemented and running** — this is the design the service is built on. The read path is `internal/store`, the writer is `internal/ingest`, and both are measured on the mainnet registry in the README.

This note is kept as the reasoning: it was written before the writer existed, so it argues for the choices rather than describing them, and the experiments it names (`cmd/e1`, `MODE=versioned`) were the prototypes that produced the first numbers. Those commands are gone; their successors are the service itself. Figures below are from the original synthetic stand (499 800 keys, 3 modules, 211 MiB per dump) unless stated otherwise.

## The freshness guarantee is what costs, not the runtime

The canonical service must not serve a half-rewritten registry, and it buys that guarantee by holding a `REPEATABLE READ` transaction open for the entire duration of a dump. The registry updater meanwhile mutates the same rows in place — upsert per operator, delete when keys disappear on-chain. Two consequences follow: dead tuples accumulate at the rate of key churn, and autovacuum cannot reclaim them while a reader still holds an older snapshot. Every additional consumer that polls the full key set widens that window.

Measured on the stand, with a writer reproducing the per-operator key sync:

| | clean table | during churn | table size |
|---|---|---|---|
| canonical TS/NestJS, `/v1/keys` p50 | 5.56 s | 8.23 s | 237 MB → 3.2 GB during the run |
| PoC Go over the same rows, p50 | 356 ms | 2.35 s | same table |
| PoC Go serving an in-memory snapshot, p50 | 70 ms | 71 ms | not read during requests |

The third row is the point. Once reads stop touching the table that is being rewritten, churn stops mattering — the Go service loses 6.6× of its own speed when it reads the live table under churn, and none of it when it does not. A rewrite in a compiled language moves the first row toward the second; it does nothing about the difference between the second and the third. That difference is data layout.

This is also the documented reason `/v1/keys` has no pagination: `rest-api.md` states that pagination is unsupported because updates may occur during processing. Rows move under the reader, so there is no stable cursor. Pinning a version removes the objection.

## Proposal

Publish immutable versions of the registry and let the client choose which one it wants:

| Modifier | Meaning | Client |
|---|---|---|
| `latest` | newest fully synced version; may be rewound by a reorg | dashboards, widgets, anything that prefers fresh over stable |
| `safe` | newest version past reorg depth (CL-justified, exact rule TBD) | monitoring, alerting, anything that must not flap |
| `block=<n>` / `version=<id>` | one pinned version explicitly | diff runners, reconciliation, debugging |

Every response carries the version identifier it was served from. It is monotonic, so a consumer that sees it go backwards knows a reorg rewound the chain and can react instead of silently mixing two chains. Absent a modifier, the default stays whatever the canonical contract does today, so existing consumers do not change behaviour.

## Precedent: lido-mev-monitoring

Our own stack already runs this pattern, and it is worth copying deliberately rather than by feel. In `lido-mev-monitoring`:

- `meta_version` — one row per version: `version_id` (autoincrement), block number, block hash, timestamp, plus a `status` that drives the lifecycle `NEW → SYNCED → MATCHED → CAN_BE_DRAINED → DRAINED → LEGACY`.
- `node_operator_key_version`, `node_operator_version`, `staking_module_version` — per-version rows referencing `meta_version_id`, with an identity table (`node_operator_key`: index, operator index, module address) separate from the versioned attributes.
- Readers resolve a version first: `findLatestVersion()` selects the newest row whose status is `SYNCED` or `MATCHED` — a version being written (`NEW`) is invisible until it is complete.
- Retention is explicit: keep the last N matched versions, drop versions no longer referenced, and delete leftover `NEW` rows on startup to clean up after a crash mid-sync.

What to take: the status lifecycle, the "invisible until complete" rule, crash cleanup, and explicit retention. What not to take unexamined: `node_operator_key_version` stores the pubkey and the deposit signature again for every version. At key-set scale that is the expensive part.

## Storage strategies

Base measurement: one copy of the key table with indexes is 237 MB at 500 000 keys.

| Strategy | Cost per version | Read predicate | Notes |
|---|---|---|---|
| Full copy per version (mev-monitoring shape) | ~237 MB | `where meta_version_id = V` | simplest; retention becomes mandatory quickly |
| Identity table + versioned attributes | tens of MB, needs measuring | join identity, filter by version | pubkey and signature stored once |
| Validity ranges: `valid_from_block`, `valid_to_block` | proportional to churn only | `valid_from <= B and (valid_to is null or valid_to > B)` | one table, append-only, unchanged keys cost nothing |

For a key registry where almost nothing changes between two blocks, validity ranges are the natural fit: a version costs the churn, not the key set. The version table then holds only labels and block metadata — which is exactly the `meta_version` idea, minus the data duplication.

Writes become append-only, which is the second half of the win: no in-place updates means no dead tuples from the update path, so the bloat measured above largely goes away, and vacuum is no longer blocked by long readers because readers no longer need long transactions.

## What it unlocks

- **Pagination.** A pinned version gives a stable cursor. Consumers stop pulling 211 MiB per poll.
- **Incremental reads.** `since=<version>` returns what changed between two published versions, which is what the polling consumers actually want.
- **Short transactions.** Reads can run read-committed, per page, because the data they read is immutable.
- **Honest staleness.** A consumer that asks for `safe` knows what it got; today "fresh" and "stable" are the same endpoint and neither is stated.

## Open questions

- ~~Reorg semantics~~ — answered below: superseded versions are kept, labels carry the change through `lastChangedBlockHash`, and an explicit pin gets 409 or 410.
- Where `safe` comes from: reorg depth in blocks, CL justified checkpoint, or a configured lag. This decides whether a CL endpoint becomes a hard dependency.
- Retention window: how many versions, and who pays for the storage. Needs the per-version cost measured, not guessed.
- Two concurrent writers (one serving `latest`, one syncing the next version) is the natural shape, but it needs a rule for what happens when sync falls behind head by more than the retention window.
- Whether the canonical service wants this at all. It is a contract change on a protocol-critical service, so the decision belongs to its owners. Our own service can adopt it first for our consumers, which is also the cheapest way to produce evidence.

## E1 result: the pinned read is not more expensive

Implemented as `key_version` (validity ranges over blocks) plus `versions` (published versions with a status and a label), written by `internal/ingest` and served by `internal/store` with `?snapshot=latest|safe|<block>|<hash>`. The prototype that produced the first numbers used `registry_key_versioned` / `registry_snapshot` and has been removed. Target scale is a window of 10–20 versions, everything older pruned. Same dataset as the rest of the stand: 499 800 keys, 211 MiB per dump.

Full dump through the identical render path, 20-version window:

| Read | p50 seq | p50 at 4 concurrent |
|---|---|---|
| plain table, `REPEATABLE READ` per dump (today's shape) | 354 ms | 783 ms |
| versioned, `PIN=latest` | **221 ms** | **375 ms** |
| versioned, `PIN=<block>` inside the window | 231 ms | — |

The threshold was "within +20 % of the plain read". The pinned read came out 37 % faster, and twice as fast under concurrency, because it does not hold a long transaction and each query is an ordinary short read.

**Reads do not notice the writer.** Publishing a version per second while dumping:

| | idle | during publishing |
|---|---|---|
| versioned, `PIN=latest` | 221 ms | **222 ms** |
| plain table, Go, in-place churn | 356 ms | 2 350 ms |
| plain table, canonical, in-place churn | 5 560 ms | 8 230 ms |

**Write and storage cost, 20 versions at 1 % churn each:**

| | value |
|---|---|
| publish one version (close ~5 000 rows, open ~5 000) | 53–94 ms |
| rows after 20 versions | 594 762 (499 800 current + 94 962 history) |
| size, versioned, compacted | 232 MB table + 21 MB indexes |
| size, plain table, compacted | 186 MB table + 37 MB indexes |
| prune to the newest 20 versions | 105 k rows deleted in 96 ms, vacuum 229 ms |

A 20-version window costs about 13 % more on disk than the single-version table, and pruning is a sub-second job. Storage is not the argument against this.

**Time travel is exact.** Pinned at the base version, the service returned all 499 800 keys byte-identical to the canonical response (only the meta block differs, since it comes from a different table).

### Two findings worth carrying into the design

1. **`latest` must be a different query shape.** With the range predicate `valid_from <= B and (valid_to is null or valid_to > B)`, a pin at the head matches almost every row, so the index stops being selective and the planner switches to a sequential scan. Measured at 273 versions: 38 ms → 174 ms. Current rows need their own predicate (`valid_to_block is null`) against a partial index. The store now picks the shape by whether the pin is the head.
2. **A missing version needs its own error.** Falling through to the canonical 425 "Too early response" is misleading — the version is not late, it is gone. Implemented below as 409 and 410.

Scale note: history is meant to stay at 10–20 versions. A run to 273 versions (1.86 M rows) showed the read path degrading and version writes slowing from 57 ms to 380 ms, part of which is the experiment harness selecting victims by `OFFSET`. Retention is not optional.

## Reorg rule: reuse the refresh signal that already exists

The canonical contract already carries the flag a consumer needs. `lastChangedBlockHash` stays put while nothing changes and moves to the current block hash whenever a module was re-read — including the `isReorgDetected` path in the updater. So a consumer's rule is unchanged: *if `lastChangedBlockHash` differs from the one I hold, refetch.* A reorg then needs no new concept for anyone polling a label, because it looks like any other change.

Verified on the stand. Publishing two versions that change nothing left `lastChangedBlockHash` on the older block (`…401654`) while the block numbers advanced to 4200021 and 4200022 — no spurious refetch. Then a three-deep reorg (`cmd/e1 -reorg-depth 3`: supersede the newest versions, drop the key rows that only existed on the losing branch, reopen the rows it had closed, republish the same heights on another branch — 32 ms) moved `lastChangedBlockHash` to the new branch, and the served key set changed with it.

What genuinely needs a new answer is an explicit pin, and only there:

| Pin | Behaviour after a reorg | Status |
|---|---|---|
| `latest` / `safe` | follows the canonical branch; `lastChangedBlockHash` moves | 200 |
| block **hash** | the exact version; if its branch lost, the read fails and names the head to jump to | **409** with `headBlockNumber` and `headLastChangedBlockHash` |
| block **number** | follows the canonical branch at that height, since a height alone cannot say which branch was meant | 200 |
| outside the retention window, or a hash we do not hold | gone, not late | **410** |
| nothing published yet | unchanged from canonical | 425 |

Measured responses:

```
PIN=0xa000…401656 (superseded)  → 409 {"headBlockNumber":4200022,
                                       "headLastChangedBlockHash":"0xb000…401656", …}
PIN=4200001 (pruned)            → 410 {"message":"snapshot 4200001 is no longer retained"}
PIN=4200022 (height)            → 200, served from the surviving branch
```

Two consequences worth stating in the contract: **superseded versions are kept, not deleted** — that is what makes a definite 409 possible instead of an ambiguous 404 — and **a consumer that needs exactness must pin by hash**, because a height pin will silently follow the reorg. A rough edge remains: a hash we never had is reported as 410 "no longer retained", which is the likely reason but not a certainty.

## Version lifecycle: status decides what is current

Following the shape `lido-mev-monitoring` already uses, currency is a property of the version row, not of the arrival of its data:

| Status | Meaning | Served? |
|---|---|---|
| `building` | the writer is filling this version | no — an explicit pin on it gets 425 |
| `published` | complete and trusted | yes |
| `stale` | complete, but we found out it no longer reflects the chain and a resync is needed | yes, as a fallback, flagged |
| `superseded` | taken off the chain by a reorg | no — an explicit pin gets 409 |

Two properties follow, and both were tested.

**A version can be written across many transactions.** `building` makes the payload invisible until one small `update … set status = 'published'`. That matters for anything larger than incremental churn — an initial sync or a full rebuild — because it removes the need to fit a version into a single transaction.

**`latest` prefers a trusted version over a fresher distrusted one.** Marking the head `stale` moved `latest` back to the newest `published` version, with no flag raised — a consistent, slightly older answer. Only when nothing trusted remains does `latest` serve a stale version, and then it says so:

```
X-Snapshot-Stale: true
```

The signal is a header, not a body field, so every JSON response stays byte-identical to the canonical contract. `safe` never resolves to a stale version at all — that is what distinguishes it from `latest`. A dump served from a stale version is still internally consistent: it is one version, just not the chain's.

## The defect this design had, and how it showed up

A dump issues one query per module. The first implementation served `latest` from `valid_to_block is null` against a partial index — cheaper, and wrong: publishing a version between two of those queries moves the set of open rows, so the response gets stitched together from two versions.

Measured with a publisher that rewrites the whole key set per version, setting `used` to one value so that any mixed response is visible:

| Read predicate for `latest` | Result |
|---|---|
| `valid_to_block is null` (partial index) | **2 of 6** dumps mixed two versions — torn exactly at keys 166601 and 333201, the first key of modules 2 and 3 |
| range predicate at the resolved block | **0 of 6**, publisher confirmed running throughout |

The range predicate is immune because closing a row sets `valid_to_block` to a later block, so the row stays visible to a read pinned at an earlier one. The cost of giving up the partial index is the earlier finding about selectivity at head — which is affordable at a window of 10–20 versions and is the reason the window matters.

The first version of the detector was itself wrong: it flipped each row's own `used` value, which preserves the seeded 80/20 mix, so it reported tears on consistent data. Uniform values per version fixed it. The lesson repeats — a consistency check needs its own control before its verdict means anything.

## If the stream is cut

| How the dump ends | What the client sees |
|---|---|
| graceful restart (`SIGTERM`) | the in-flight dump drains and completes: 222 MB, valid JSON |
| `SIGKILL` mid-dump | transport error (`curl` exit 18) **and** malformed JSON — truncated at 196 MB, unterminated string |

So a cut dump is detectable twice over, at the transport and at the parser, and it is never a silently mixed one. Worth contrasting with the canonical service, where `STREAM_TIMEOUT` destroys the stream at 60 s by design and its own comment notes this "closes the stream without ']' and corrupts the result" — same detectability, but self-inflicted rather than caused by a crash.

## What decides that `latest` went stale

The same signals the canonical service uses to move `lastChangedBlockHash`, read once per polling cycle and none of them requiring a key to be re-read. From `staking-module-updater.service.ts`, in order: reorg, `prevNonce !== currNonce` per module, too large a block gap (`MAX_BLOCKS_OVERLAP`), operator events in the range. Nothing fired means the version is still current.

`internal/ingest/freshness.go` is that decision as a pure function, with the version's per-module nonces stored on the version row (`registry_snapshot.module_nonces`):

| Signal at head | Action |
|---|---|
| reorg | `supersede` — take the version off the chain, then rebuild |
| any module nonce differs | `rebuild` — mark stale, build the replacement, publish |
| head is `max-block-gap` or more ahead | `rebuild` — the event scan is no longer trustworthy |
| operator events in range | `rebuild` |
| none of the above | `extend` — advance the version's observed block, leave data and `lastChangedBlockHash` alone |

Unit tests cover all seven branches, including a module appearing and a module vanishing. `cmd/freshness` runs one cycle against the database and applies the outcome, so the transitions can be driven deterministically without an execution layer.

Observed through the API:

```
cycle 1, nothing changed:
  decision: extend — no change up to block 4200030
  served:   block 4200030, lastChanged …00401654, stale-header=no      ← block moved, data did not

cycle 2, nonce of module 1 moved:
  decision: rebuild — nonce changed for 0xad0000…2e4d
  previous version marked stale, new version written as building, then published
  served:   block 4200040, lastChanged …00401668, stale-header=no      ← consumer refetches

while the replacement is still building:
  served:   block 4200040, lastChanged …00401668, stale-header=yes     ← last trusted data, flagged
```

The middle line is the one that matters for availability: a resync in progress does not take reads down and does not silently serve half a version. Consumers keep the last complete version, and the flag tells them it is no longer trusted.

Two notes on what is not decided by this. The cheap signals say *that* something changed, not *what* — the rebuild still has to re-read the changed modules' keys, which is ingest work (step two). And a nonce is per module, so a rebuild can be scoped to the modules whose nonce moved rather than the whole registry; the current tool rebuilds everything.

## Experiments still open

1. **E2 — reorg depth beyond the window.** What happens when a reorg is deeper than the retention window, so the fork point itself was pruned.
2. **E3 — pagination and diffs.** p95 for a 10 000-key page at a pinned version, and the size of a `since=<version>` diff for a typical block. This is the number that decides whether consumers can stop dumping.
3. **E4 — own ingest.** Everything above reads a table populated from the seeded registry. The writer that produces versions from the execution layer is step two (`internal/ingest`).
