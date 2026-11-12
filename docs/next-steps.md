# Where this stands and what comes next

Written to be picked up cold. Nothing below assumes the session that produced it.

## State

Everything below is measured on the real mainnet registry unless it says otherwise: 511 877 keys, 4 modules, 731 operators, 216.7 MiB per dump, synced from our tooling nodes by the canonical service, with every reader serving from that same database.

**Done in this round:** E6 (real mainnet data) and E7 (published versions inside the embedded store). Both are written up in the README.

Proven, with numbers in the README and the design notes:

- A Go serving layer over the canonical schema is byte-identical on the compared surface (10 of 10 endpoints) and 11–20× faster with 6–16× less memory; under a 1 CPU / 512 MiB container limit the gap widens rather than narrows.
- The bottleneck that hurts in production is not the runtime. Reading a table that is being rewritten costs the Go service 6.6× of its own speed; reading an immutable published version costs it nothing (221 ms idle, 222 ms while a version is being published).
- Published versions over validity ranges cost nothing to read (37 % *faster* than the live table) and about 13 % more on disk for a 20-version window. Pruning is a sub-second job.
- Reorgs need no new consumer-facing concept: `lastChangedBlockHash` already carries "refetch". Explicit pins get 409 (superseded) or 410 (pruned).
- Staleness is decided from cheap chain signals only — nonce per module, reorg, block gap, operator events — and a resync in progress keeps serving the last trusted version behind `X-Snapshot-Stale: true`.
- An embedded LSM store cannot hand a client a half-applied update, provided one logical update is one batch and readers take a snapshot. Both directions tested.
- Versioning inside the embedded store costs nothing for `latest` (194 ms versioned against 199 ms unversioned) and is byte-identical to canonical on the full dump. Publishing is a sub-millisecond status flip; retention is a 1 ms `DeleteRange` plus a 299 ms compaction to reclaim.
- Parity holds on real data for all three backends across every endpoint checked, several byte-identical.

**Built since:** the service itself, `cmd/keys-api` — ingest plus API in one process, over its own schema. See [architecture.md](architecture.md). What that closed:

- Backlog item 1 (own ingest) — the writer exists: versions through `building → published`, per-module diffs so unchanged keys are never rewritten, block-range versioning, retention, reorg rollback to a fork point found in our own version table. Community modules (CSM, CMv2) are implemented too, so no module is skipped.
- Backlog item 2, writer side (per-module scoping) — a nonce change re-reads that module only. Verified end to end: a cold build wrote 33 key rows for 33 keys; the next version, one key added to one module, read 19 keys and wrote 1 row.
- Backlog item 6 (operators) — stored, versioned and served, because `ethereum-validators-monitoring` needs them.
- Backlog item 9 (truthful per-module `lastChangedBlockHash`) — derived from content, not from the act of reading. Verified: a module whose content did not move keeps its stamp while another module's moves.
- Backlog item 10 (metrics and health) — Prometheus at `/metrics`, liveness and a readiness that fails until something is published.
- The consumer surface: `/v1/operators`, the module operator endpoints, and the `/v2/modules/operators/keys` stream `lido-mev-monitoring` parses.
- The source seam: `internal/source` with `lidocore` and `static` implementations. A different registry — stVaults — implements the same interface and writes through the same writer into the same schema.

- Backlog items 3 and 4, and E3 (pagination and `since=` diffs) — both implemented and measured, see below.

**Cold sync of our own writer against mainnet**, batch 40 / concurrency 6 against the two tooling nodes, into an empty database:

| | |
|---|---|
| Total, empty database to published version | **107 s** on the host (repeat run 110 s); **2 min 28 s – 4 min 15 s** in the container at 1 CPU |
| Keys written | 511 877 keys and 731 operators across 4 modules (39 / 82 / 609 / 1) |
| module 1 — 471 732 keys, 39 operators | 47 s |
| module 2 — 14 205 keys, 82 operators | 8 s |
| module 3 (CSM) — 25 940 keys, 609 operators | 51 s |
| module 4 (CMv2) — 0 keys, 1 operator | 0.5 s |
| Result on disk | key table 105 MB, key indexes 97 MB, whole database 211 MB |
| Quiet cycle, steady state | 0.81 s mean over 9 cycles (header, modules with nonces, one log filter per module, one row update) |

The container figure is the one that matters for a deployment, and the gap between the two is not Postgres: it is one core doing the JSON decoding and ABI unpacking for half a million keys while also running the HTTP server. Its own spread — 2 m 28 s against 4 m 15 s across two runs — is what else the host was doing at the time, mostly the canonical service syncing beside it: module 1 took 1 m 4 s in the quieter run and 2 m 32 s in the busier one. CSM's 609-operator tail crawls at ~300 keys/s either way.

Two things that table says plainly. The sync is **round-trip bound, not key bound**: CSM's 26 000 keys cost more than module 1's 472 000, because its 609 operators are two calls each plus a name lookup, and module 1 has 39 operators. And the index footprint is now nearly the table's own size — five indexes, three of them added for paging and diffs. Trimming that is a measurement worth doing (`kv_from`/`kv_to` are the candidates; the diff predicates may be served well enough by `kv_range`).

For comparison, the canonical service's cold sync of the same registry took about eight minutes, but at batch 20 / concurrency 2 — deliberately throttled for shared nodes. That is not a like-for-like number and should not be quoted as a speedup without re-running it at matched knobs.

**Paging** (E3, first half): 511 877 keys in 52 pages of 10 000, median page 35 ms, min 18, max 42, no gaps and no duplicates. The walk took 22 s, during which the chain advanced three blocks: all 52 pages came from one pinned version while the head in `meta` moved. That last part cost a design fix — a version's identity now stays at the block its data was read at, and the observed head is a separate field, because the earlier version renumbered itself on a quiet cycle and cursors 410'd mid-walk.

**Diffs** (E3, second half): verified on real responses that an old dump plus the diff equals a fresh dump, including a deposit flipping `used` on an existing key — it appears as one removal plus one addition, which is what the storage did. On mainnet-scale data, a diff between two versions of the 511 877-key registry answered in **2–5 ms and 527 bytes** where the full dump is 227 MB. Both versions were content-identical there, so that is the query cost rather than a churn size; the size for a real change follows the key count directly, since the response is those keys and nothing else.

**Two things mainnet taught in the process, both now fixed:**

*The gap rule was firing every six minutes and doing nothing.* `MAX_BLOCKS_OVERLAP = 30` copied from the canonical service made the loop re-read 486 000 keys once per 30 blocks and write zero rows. It reads that way there for a good reason — the decision rests on an event scan, which cannot be trusted if it falls behind — but here the decision rests on a nonce read at head, a contract counter that being late cannot cause you to miss. So the rule is only worth keeping as a periodic audit against a lying signal, and the default is now 1 000 blocks (about three hours). The audit is worth keeping precisely because a healthy one writes nothing: `keys_read=511877 keys_changed=0` is a verifiable statement that the cheap signals were telling the truth.

*And that audit produced the cleanest evidence yet for the `lastChangedBlockHash` fix.* A full re-read of every module published a new version whose `last_changed_block_hash` was byte-identical to the previous one, at mainnet scale — the exact case where the canonical field moves and lies. Recorded in the version table as two rows sharing one stamp.

Also fixed while measuring, both mine: `sync_cycle_duration_seconds` was always zero (deferred write to an unnamed return value), and a quiet cycle was reporting the version's age as the head's age, which made stale data look fresh on the one gauge meant to alert on it.

Still not built: validator exit endpoints and a mainnet-scale diff measurement (the query cost is measured; the response size for a real change needs a nonce to move while the stand is up). The OpenAPI document is done — `/openapi.yaml`, rendered at `/docs`, with a test holding it to the router.

**Corrections this round produced, worth not re-learning:**

- A full-copy version in the embedded store costs ~230–300 MB on real keys, not the 52 MB the synthetic set suggested — real pubkeys and signatures are high-entropy hex that Snappy barely compresses. Full-copy suits two or three versions, not twenty.
- The single-writer lock bites in practice: pruning failed with `resource temporarily unavailable` while a reader held the store.
- `/v1/modules/keys` kills the canonical service at a 512 MiB limit on real data — that endpoint is not streamed there. Its container reports `OOMKilled=false, exit=0` because PID 1 is `yarn`, which survives its child being killed, so a Node OOM looks like a routine restart. Anything watching restart reasons in k8s would be misled.
- One trailing byte separated us from byte-identical on non-streamed endpoints: `json.Encoder.Encode` appends a newline, Fastify does not.
- Two defects were in the *detector*, not the implementations: nested key arrays were not sorted, and the ignore list did not reach the flat-keys path, so ordinary order differences and a live-chain meta block read as content mismatches. Both fixed; `diff -live` is the mode for a syncing pair.

## Implementation backlog

Ordered so each item is useful on its own.

1. **Own ingest behind `internal/ingest`.** The read path exists and works against mainnet (`cmd/ingest`, `internal/eth`, `internal/ingest/source_el.go`): it reuses the canonical service's own ABI files and its call sequence — Locator → StakingRouter.getStakingModules → per-module `getType` → per-operator `getNodeOperator` + `getSigningKeys` in batches of 1100, with `getNonce` read from the module contract rather than the router. Verified on mainnet: four modules with matching nonces in 322 ms, and module 2's 14 205 keys in 9.3 s (1 522 keys/s at batch 20 / concurrency 4).

   What is left: the **writer** (versions through `building → published`, scoped to changed modules) and the **community-module interface** — only `curated-onchain-v1` is implemented, so modules 3 and 4 are skipped loudly rather than silently producing an incomplete version. Acceptance: `cmd/diff` green against the canonical service, both following the same chain, for a full day.
2. **Per-module incrementality, on both sides.** This is the cheapest item on the list and it needs no new endpoint.

   *Writer side:* `Evaluate` already returns `ChangedModules`; `cmd/freshness` ignores it and rebuilds everything. The validity-range layout supports a partial rebuild natively — only the changed module's keys get new rows. Acceptance: a nonce change in one module writes rows for that module only, and a dump pinned before and after differs only in that module.

   *Consumer side:* the canonical contract already carries what a consumer needs to skip work — `/v1/modules` returns `nonce` and `lastChangedBlockHash` per module, and it is a kilobyte-sized response. A consumer polls that, compares per module, and fetches `/v1/modules/{id}/keys` only for the ones that moved. Mainnet numbers make the payoff concrete: module 1 holds roughly 90 % of the keys (446 400 deposited of ~484 000 across four modules), so any change confined to modules 2–4 lets a consumer skip about nine tenths of the data it pulls today.

   **Use `nonce`, not `lastChangedBlockHash`.** Checked on the live mainnet service (`keys-api.lido.fi`, canonical 4.0.1) on 2026-07-25: all four modules carried the *same* `lastChangedBlockHash`, equal to the global one in `meta`. It is not a per-cycle watermark — over 45 s the head advanced four blocks and the value did not move, and `MAX_BLOCKS_OVERLAP = 30` against a 5 s job interval means the whole-set branch does not normally fire. So the field is per module in the code (the no-change branch re-upserts a module with its own previous hash, `staking-module-updater.service.ts:141`; only `updateStakingModule` at :170 moves it) but collapses to one value whenever something rewrites every module in one cycle — a restart, a stall past 30 blocks, or a reorg. After that you cannot tell "module 2 unchanged" from "everything was stamped at that block", so it is not a reliable sole skip signal. Its DTO description ("used to determine that a change has been made to this staking module") over-promises; worth reporting upstream.

   `nonce` has none of that: a contract counter, independent per module (18680 / 1130 / 5200 / 8 on mainnet), unaffected by restarts and re-reads. It also covers deposits, so a `used` flag flipping moves it — `obtainDepositData` in `NodeOperatorsRegistry` calls `_increaseValidatorsKeysNonce()`. In our own service, expose a per-module version id alongside it; `module_nonces` is already stored on the version row.

   Acceptance: a documented polling recipe plus a measurement of the bytes a consumer avoids over a day of mainnet blocks.
3. **Pagination on a pinned version.** The reason the canonical service refuses it — rows moving under the reader — does not apply once a version is pinned. Acceptance: `?limit`/`?cursor` on `/v1/keys` at a pinned version, with a cursor that survives across requests.
4. **`since=<version>` diffs.** Return what changed between two published versions. This is the endpoint that lets consumers stop pulling 211 MiB per poll. Acceptance: response size for a typical block is kilobytes, and applying the diff to a held snapshot equals a fresh dump.
5. **Distinguish "hash never seen" from "hash pruned".** Both currently answer 410 "no longer retained". Acceptance: an unknown hash answers 404.
6. **Operators and operator keys.** Out of PoC scope so far; needed before any consumer can move over. Acceptance: `cmd/diff` covers them.
7. **Extension fields we can add without asking anyone.** *Half decided:* the surface question is settled — `/v1` and `/v2` are the canonical contract and nothing else, and everything this service adds lives under `/ext/v1`, where every canonical read is mirrored and only there are `snapshot`, `limit` and `cursor` honoured. `/v3` was rejected because the canonical service versions per endpoint through NestJS URI versioning, so that number is one decorator away in a repository this team does not own. The boundary is held by tests in `internal/e2e/surface_test.go` and `internal/api/openapi_test.go`. What is still open is the storage and codec half below. The point of owning the service is adding a field for an EVM or no-widget need in an afternoon instead of negotiating capacity with another team. Two things make that possible, and both are cheap if designed in now rather than retrofitted. *Storage:* a fixed core record plus an optional extension section (length-prefixed tags, or CBOR), so a new field is a new tag — existing records are never rewritten and readers ignore tags they do not know. *Contract:* the default response stays byte-identical to canonical, and extensions are opt-in (`?fields=…`, or a separate path), so adding a field cannot break a consumer that did not ask for it. Because everything is versioned, the schema id belongs on the version row: new versions carry new fields, old versions keep their codec, no migration. Acceptance: adding a denormalised field (operator name next to a key, say) touches the ingest and one codec tag, changes no existing record, and leaves `cmd/diff` green on the default response.
8. **Swagger, since the choice is Go.** The canonical service publishes an OpenAPI document through `@nestjs/swagger`, and consumers may be generating clients from it. In Go this is either annotations (`swaggo`) or a hand-written spec served with a UI. Acceptance: the published document describes the same surface as canonical for the endpoints this service serves, and a client generated from it works against both.
9. **Make `lastChangedBlockHash` truthful in our version** (wanted, not required). The canonical field moves whenever a module is *re-read*, so a restart, a stall past 30 blocks or a reorg stamps every module with the same block and the per-module signal collapses. Fix it by deriving the field from content rather than from the act of reading: keep a per-module digest on the version row (nonce plus a rolling hash over the module's keys) and move `lastChangedBlockHash` only when the digest differs from the previous version's. The digest also covers the case where a reorg returns the same nonce with different content. Keep `meta.elBlockSnapshot.lastChangedBlockHash` as it is, for compatibility. Acceptance: force a full re-read of every module and confirm the per-module value does *not* move for modules whose content is unchanged — the exact behaviour that fails on the canonical service today.
10. **Metrics and health that match the deployment.** Prometheus endpoint, the alert-relevant gauges (snapshot age, staleness, rebuild duration). Acceptance: the existing dashboards for the canonical service can point at this without editing panels.

## Experiment backlog

Each has a threshold, so the outcome is a decision. **E6 is next** — see the note under it for why.

- **E2 — reorg handling as rollback plus catch-up. Threshold met in the loop, one caveat left and no real reorg observed.** The rollback and the fork-point walk were already there; what was missing is the half the design claimed and the code did not do — after rolling back it rebuilt *every* module, so a one-block fork cost a full 512 000-key read on mainnet. Now the version that survived the rollback becomes the one the cycle decides against, which makes a reorg a rollback plus the ordinary incremental check.

  Both thresholds hold, in `sync_test.go` against a scripted chain: a fork whose only change was dropped with its block costs **zero key reads** and ends in `extend`; a fork where one module moved re-reads that module alone; and a fork below everything held answers with a defined full rebuild rather than an error. The tests were checked against a control — restoring the old behaviour fails two of them, reading modules `[1 2 3]` where `[2]` is correct — because a passing test that would also pass on the bug measures nothing.

  What is left, precisely:

  - **Nonce equality across a reorg is not sufficient on its own.** A counter can roll back two increments and take two different ones on the new branch, arriving at the same value with different content. The safe fix is an event scan over the reorged range, and it is not free to add: the trimmed ABI files here carry no key-related events at all, so it needs the right event signatures (`SigningKeyAdded`, `VettedSigningKeysCountChanged` and their CSM equivalents) taken from the contracts rather than guessed — a wrong signature yields a wrong topic hash, which returns no logs and looks exactly like "nothing changed". Until then this service is no worse than the canonical one, which also trusts the nonce, and the window is narrow.
  - ~~**No reorg has been observed on a real chain.**~~ Forcing one no longer needs a devnet: `internal/fork` reorgs an anvil fork of Hoodi and runs the real loop over the result — see [fork-tests.md](fork-tests.md). Three shapes are covered: a key swapped for a different one at the same height *and the same nonce*, which is the case this caveat was written about; a fork whose only change was dropped, which costs zero key reads and ends in `extend`; and a reorg above the published version, which the detector does not see because it probes the version's own height only. Two things that came out of building it are worth carrying: `anvil_reorg` re-mines the rewound blocks **with their original transactions**, so it changes hashes and not state — content divergence needs `evm_revert` — and a reorg followed by a rebuild puts two versions at one height, which is how the reader's status re-check was found to be identifying versions by block number. A reorg observed in the wild is still worth having; `reorgs_total` on the Hoodi stand is still the way to get it.
  - **The API side of "F is gone" is separate.** A pinned hash that has been superseded or pruned still answers 410 either way; splitting unknown from pruned is item 5 in the list above.
- **E3 — pagination and diffs.** p95 for a 10 000-key page at a pinned version, and diff size for a typical block. Threshold: a page under 100 ms, a diff under 1 MiB.
- **E4 — retention under real churn.** Cost of keeping 20 versions when churn is mainnet-shaped rather than a flat 1 % per version. Threshold: storage growth bounded and prune duration under a second.
- **E5 — two writers, one store (low priority now).** The `latest`/`safe` design wants a writer publishing while another syncs. For Postgres this is a policy question; for the embedded store it needs `Checkpoint()`. Threshold: a reader replica serves from a checkpoint with no torn reads.
- **E6 — done.** Real mainnet data, results in the README. A cold sync took ~8 minutes with the provider knobs deliberately below default. Original note kept below for the endpoints and addresses.
- ~~**E6 — a real key set, on mainnet. Do this one first.**~~ It needs no new code: the canonical service does the ingest, our service reads the same database, and `diff` plus `bench` run unchanged. That converts every number in this repo from synthetic to real, and it produces the response-parity evidence on real data that the adoption gate asks for. It also finishes the one observation still missing — whether a module's `lastChangedBlockHash` diverges from the others once an individual module changes, which needs a longer window than a synthetic run. Cold sync time is itself a datapoint worth recording. Mind the load on a shared node: `PROVIDER_JSON_RPC_MAX_BATCH_SIZE` and `PROVIDER_CONCURRENT_REQUESTS` are the knobs. Everything so far is synthetic 499 800 keys in 3 modules, which turns out to be close to mainnet scale (~484 000 deposited validators across four modules). Read endpoints available for this: `<mainnet-archive-node>` (geth 1.16.7) and `<mainnet-archive-node>` (erigon 3.3.10), both mainnet, both answering. Contract addresses come from the Locator — mainnet `0xC1d0b3DE6792Bf6b4b37EccdcC24e45978Cfd2Eb`, which resolves the Staking Router to `0xFdDf38947aFB03C621C71b06C9C70bce73f12999`; the deployed-contracts page at docs.lido.fi is the reference. Threshold: same ratios as the synthetic set, or an explained difference.
- **E10 — done.** Binary storage measured: table halves (200 → 100 MB), sequential latency unchanged (374 vs 380 ms), CPU-bound concurrency costs ~37 % (1.53 vs 1.12 s at four concurrent dumps on one core). Results and the recommendation in the README. What is left: decide after E3, since the workload that penalises it is the one pagination removes.
- **E8 — a lean TypeScript/Node baseline.** The canonical service is not slow because of V8; it is slow because of what sits on top of it — NestJS plus MikroORM hydration, `ethers.utils.getAddress` per key, JSONStream, an object allocated per key. A service on bare `http`/fastify with `pg-query-stream`, a hand-rolled serializer and a memoized checksum should recover most of that. This experiment settles the language question with a number instead of a preference, and the stand measures it unchanged: same seeded database, `diff` for parity, `bench` for latency and memory. Threshold: within 3× of the Go p50 and under 100 MiB peak at 1 CPU / 512 MiB means the language is not the deciding factor and TypeScript is free to win on maintenance grounds; materially worse means the choice costs something that has to be accepted deliberately.
- **E7 — done.** Full-copy versions implemented (`internal/store/lsm_versioned.go`, `MODE=lsm-versioned`) and measured; the delta layout remains design. What is left from it: the delta layout itself, which is what makes a 20-version window affordable in the store. Threshold: `latest` stays a single range scan and retention by `DeleteRange` is cheaper than the Postgres prune measured at 96 ms.

## Ingest throughput: what the initial sync is actually limited by

Observed on the E6 mainnet sync (throttled to batch 20 / concurrency 2 on purpose): 511 877 keys fetched in about seven minutes, but only ~28 operators per minute, and the operators being fetched at that point held one key each. The limit is **round trips per operator**, not key volume. Corrected count, taken from our own store: 731 operators across the four modules — 39 in the curated module, 82 in SimpleDVT, 609 in CSM, 1 in CMv2. CSM is the tail, and it is why its 26 000 keys cost more wall clock than module 1's 472 000. Ordered by expected effect:

1. ~~**Open the throttles first, before writing any code.**~~ Done, and it is not most of the win: at batch 100 / concurrency 8 the full mainnet key read takes 37 s of chain time, but that is with reads packed. The knobs alone do nothing for the tail, because a JSON-RPC batch size of 100 is irrelevant when the reader sends one call at a time.
2. ~~**Parallelise across operators.**~~ Done, and it turned out to be the same change as item 3 rather than a separate one: the reader now plans every operator's key spans up front and packs them across operators into request groups (`internal/source/lidocore/keys.go`), which is what makes both batching and aggregation possible. Groups run concurrently and are emitted in operator order, so the writer still streams. The note about why the canonical service cannot do this and we can still holds: it writes a module inside one transaction, our `building` status does not.
3. ~~**Aggregate calls with Multicall3.**~~ Done, with a result worth stating plainly: **against a node of our own, packing is the whole win and aggregation adds nothing measurable** — CSM went 58.2 s → 3.8 s on batching alone, and 3.7 s with aggregates. Aggregation earns its place on hosted providers instead: dRPC's free plan refuses any JSON-RPC batch over three requests, and there aggregation is the only packing available (module 2: 8.96 s / 86 requests plain against 2.50 s / 18 requests aggregated). Both ceilings from the original note are real and one of them shaped the design — an aggregate's inner calls share one gas budget, so two 1100-key reads together come back `out of gas` while 200 one-key reads are fine. Aggregates are therefore bounded by keys returned rather than call count, and reads too big to share go out plainly. Numbers and the mistake made on the way in the README.
4. **Build from event logs instead of per-operator calls.** `eth_getLogs` over ranges returns thousands of key additions per response with no per-operator round trips, and erigon often serves wide ranges better than geth — we have one of each to compare. Caveats worth checking before committing: the add-key event carries the pubkey but likely not the deposit signature, and `used`/`vetted` are state rather than events, so logs give you *what changed* while calls still supply the rest. That makes logs the right tool for the incremental path and a partial one for the initial load.
5. **WebSocket is not the lever here, with one exception.** The cost is request latency plus server-side execution; HTTP keep-alive already removes most of what WS would save, and WS adds reconnect and backpressure handling. The exception is `eth_subscribe("logs")` for steady state — that replaces the polling loop entirely and delivers the per-module "something changed" signal without asking for nonces at all.

**E9 — ingest throughput. Threshold met.** The whole mainnet key set — 511 877 keys, 731 operators — comes off the chain in 37 s against our geth (49.6 s at the stand's throttled knobs), and the cold sync end to end, empty database to published version in a 1-CPU container, is **49 s** against 2 m 28 s – 4 m 15 s before packing. Well inside the 10-minute threshold, and the per-operator round-trip limit the experiment was written about is gone: 8.5 calls per request against 1.0.

Where the 49 s goes: 40.5 s is module 1's chain read. **The sync stays node-bound** — the writer does not stand out above the node's own drift, which was 40.5 to 46.4 s for the same module across runs an hour apart. The earlier inference from read-only numbers — 37 s of reads against a 58 s sync, therefore Postgres has become the bottleneck — was wrong, and worth recording as a reminder that two runs at different knobs do not subtract. Steady state did not regress either: 0.70 s mean over 12 quiet cycles against 0.81 s before.

One decision came out of the steady-state measurement rather than the sync: aggregating calls that already fit one JSON-RPC batch made the quiet cycle slower (0.99 s against 0.87 s), so aggregation now engages only when the plain path would need more than one request. At the stand's batch of 40 that still covers the CSM tail — 4.44 s / 35 requests against 5.78 s / 53 — and it leaves the four-call freshness cycle alone.

What the runs did not settle:

- ~~**The cold sync at 2 CPU.**~~ Settled, and the way it was settled is the point. Cold syncs at 2 and 4 CPU came back 210 s, 72 s and 165 s (the last one a 1-CPU control minutes later), which says nothing about CPU: the shared node's speed drifted 3–4× across the same afternoon — module 1's read was 40.5 s at one point and 193 s at another on identical code. Wall clock against a shared node cannot resolve a container limit. What does resolve it is the container's own CPU accounting: over a full cold sync at a 1-CPU limit the service used **9.95 CPU-seconds and was throttled zero times** in 769 scheduling periods. Nothing was ever queued waiting for a core, so more cores have nothing to give. Average utilisation over the sync is about 6 %, and `docker stats` peaks at 0.3 of a core.
- **Erigon against geth.** Both endpoints exist and only geth was measured, which matters for item 4 below, since erigon is the one expected to serve wide log ranges better.
- ~~**Only mainnet.**~~ Hoodi now runs too: five modules, 87 717 keys, cold sync to a published version in 14 s, Multicall3 present. It covers two things mainnet cannot — the Sandbox module, and `curated-onchain-v2` holding keys, whose operator names come from the module's `META_REGISTRY` rather than being generated. Hoodi is also where the batch cap was isolated as the only variable: CSM's 1386 calls take 2.4 s aggregated against 33.5 s plain once the batch is capped at three, on the same node.
- **Whether module 1's 46 s can come down at all.** It is bound by execution on the node — about half a second per 1100-key read, unchanged by batching or aggregation — so the only levers left there are event logs or a smaller key batch with more concurrency, and neither is measured.
- ~~**Parity after the change.**~~ Re-run on Hoodi with both services following the same chain: eight endpoints equal, 37.1 MiB of `/v1/keys` on both sides, and module 5's 74 operators equal field by field including the `META_REGISTRY` names. That covers the risk the packing actually carried — key-to-operator attribution, which key counts cannot catch.

Items 4 and 5 above are untouched, and they are the ones that would make the incremental path cheap rather than the initial load fast.

## Found by the end-to-end suite

`internal/e2e` drives the writer, the store and the HTTP surface against a real Postgres, with the writer called directly so no chain is involved (`make test-e2e`). It exists because the two defects before it — retention deleting rows under a reader, and a diff silently losing removals — were both found by reading code rather than by a test, which is not a method that scales. Three things came out of writing it.

**Concurrent builds over one module corrupt each other, silently.** Two builds rewriting the same module at the same time both report success, and each published version then serves the other's content: the version staged at one generation read back as another, and the second read back as the generation before both. No error anywhere. The cause is the close-then-open pair in `pgModuleKeys.Commit` running under concurrent snapshots — each build asks "is there an open row for this key" against its own snapshot, so both close what they saw and neither inserts. A transaction-scoped advisory lock per module id around that pair would make the outcome either correct or a wait. Latent today, because a deployment runs one ingest; not latent for **E5** ("two writers, one store"), which is exactly this question. The reproduction is `TestTwoConcurrentBuildsOverOneModuleFailRatherThanCorrupt`.

Now prevented at the process level: a writer (`KEYS_API_MODE=ingest` or `all`) takes a session-level advisory lock for its schema before it migrates or builds and holds it for its life, so a second writer — a rollout overlap — waits, logging once that it does, while its API already serves. The lock uses the two-int key form, which `pg_locks` tells apart from the single-bigint leases. If the holding session dies (a database restart) the writer stops and waits for the lock again; the process and its API keep running. The test now runs two writers through that lock and passes; without it, it fails every time. What it showed on the way: the corruption does not need overlapping builds, only a build at a lower block committing after one at a higher block — sequential builds in that order corrupt the same way. Writers under the lock read the head after taking it, so they go forward; a guard in `Begin` refusing a block below the newest published version would close the remaining path, for instance a restart that switches `KEYS_API_BUILD_AT_FINALIZED` on.

**A malformed request answered the wrong way, twice, and parity runs could not see it.** `?snapshot=<garbage>` fell through to 500, and a module id that is neither a number nor an address answered 404 where the canonical service answers 400 with `module_id must be a contract address or numeric value`. Both are now `store.ErrBadRequest` → 400. Worth noting how they hid: `cmd/diff` compares successful responses, so the error surface has never been part of the parity claim.

**The rejected read predicate is worse than "sometimes stitched".** The first attempt at a control test interleaved a publish between two naive reads and expected them to disagree; they agreed, because rows do not arrive at publication time — a build inserts them as it goes and `Publish` only flips a status. So "the rows with no `to_block`" exposes a version that is still being written. `TestTheNaivePredicateAnswersFromTwoVersionsAtOnce` now demonstrates that directly, and it is what makes the pinned-dump tests meaningful rather than vacuous.

## Found by comparing the error surface (2026-07-28)

`cmd/diff -errors` closes the gap the e2e suite recorded and could not close on its own: parity had only ever been checked on successful responses. Twenty-one cases against canonical 4.0.1, both services following Hoodi, and the first run came back 17 of 21. Details and the table are in the README; the shape of what it found is what matters here.

Three of the four were ours being *more permissive* than the contract — a malformed pubkey answered 404 instead of 400, and an empty or misspelled `find` body answered 200 with no keys instead of 400. All three turn "your request is wrong" into "what you asked for does not exist", which is the difference between a consumer fixing its call and a consumer retrying forever. The fourth is the opposite: Go answers 405 for a wrong method and Fastify answers 404, and on the canonical surface theirs wins.

The systematic one is worth more than any single case: **every error body was missing `error`**. The field was declared in `httpError` and never assigned, so a consumer logging or matching on it saw a difference on every failure. No amount of reading the happy path would have found that.

One thing the run confirmed rather than found, and the whole `/ext/v1` split rests on it: the canonical service does ignore an undeclared query parameter, so `/v1` ignoring `snapshot`/`limit`/`cursor` is the compatible behaviour and a 400 would not be.

And one defect the run created rather than revealed, kept here because the lesson generalises: the method-mismatch rewrite inserted a `ResponseWriter` wrapper, and `headerSent`/`streamFailed` type-asserted for `*recorder` directly — so "abort the stream, this version left the chain" quietly became "write a 409 into the middle of a dump". The mid-response reorg test caught it. Both helpers now walk `Unwrap()`, which is the convention `http.ResponseController` uses; they were silently coupled to being the outermost writer.

## Rough edges to fix while passing by

- ~~**Pointing the service at a different chain silently wipes the store.**~~ Fixed, and the fix is worth reading for the shape rather than the code. Observed while bringing the comparison pair up: a redeploy picked `PROVIDERS_URLS` out of `.env` and landed on mainnet with a Hoodi store in the database. The loop probed the stored version's height on the new chain, found a different hash, called it a reorg, found nothing that survived, and cold-started — no error, nothing above `reorg deeper than the retained window`. The data was gone and the service looked healthy. It matters because third parties run this, and confusing `CHAIN_ID` is the mistake they make.

  Two layers, and the first one is the better kind of fix. **The store now lives in a schema named after the chain** (`chain_1`, `chain_560048`), which makes the mistake *impossible* rather than detectable: a mainnet run does not find the Hoodi rows, so it cannot roll them back — it cold-syncs into an empty schema and the Hoodi data sits untouched. Fix the variable, restart, and the rows are there. Detecting-and-refusing was the first design and it is strictly worse: it needs an identity row, an adoption rule for stores written before it existed, and it still leaves the operator to work out what to do. A DSN that already carries a `search_path` wins, which is how `internal/testpg` gives each test its own schema and doubles as the escape hatch for a deployment that wants its current layout.

  **Second layer, for what naming cannot catch:** a devnet reusing chain id 1 lands in the same schema as mainnet with entirely different contracts. So the loop now refuses to roll back when the stored module set and the chain's share no address — which no reorg can produce, since a fork cannot change which contracts exist. It costs no query: the version row's `module_nonces` map is keyed by module address. `TestCycleRefusesToRollBackAForeignModuleSet` with a control that still rolls back a genuine fork.

  One consequence to state plainly: an existing deployment resyncs once, because its rows are in `public` and the service now looks in `chain_<id>`. Nothing is deleted — the old schema stays as it is.

- The key table's indexes are now nearly the size of the table itself (97 MB against 105 MB on mainnet): five of them, three added for paging and diffs. `kv_from` and `kv_to` are the candidates to drop — the diff predicates may be served well enough by `kv_range`. Measure before removing.
- ~~A 404 on a pin blamed a module.~~ Fixed: `ErrNotFound` is shared between the module lookup and pin resolution and hardcoded `module <ref> is not supported`, so a consumer asking for a version that was never published was told its module was unsupported. It now says `snapshot <ref> is not a published version`; the module wording is unchanged, because that one matches the canonical service. Worth recording how it surfaced — the control experiment for the new 404/410 tests printed the body, which is the only reason anyone read it.
- An audit rebuild republishes a content-identical version, which costs a version row and a full read. Harmless, but it means the version table gains a row every audit interval whether or not anything happened; retention absorbs it.
- The canonical service will not boot if `PROVIDERS_URLS` has no TLD in the host — its `isUrl` validation rejects a bare service name like `fakerpc`, and the failure surfaces as a misleading `Cannot find module dist/src/main` from the `start:prod` fallback. Only relevant now if someone points it at an in-cluster name.
- ~~`/v1/modules/keys` builds the whole grouped response in memory on our side too.~~ Fixed: it streams now, 227 MB out at an 11 MiB peak. Worth recording how it was found, because it is the argument for keeping the stand: a parity run against the canonical service reported `216.7 MiB vs 74.1 MiB` with empty `key` strings on our side. That endpoint marshals with `encoding/json` rather than the streaming encoder, and the binary store fills only the byte fields — so it had been serving keys with `"key": ""`. Filling the strings then made it OOM at 512 MiB, which is what forced the streaming rewrite. Two bugs, one of them mine and invisible to every test, caught by comparing against the thing being replaced.
- On a live pair this service sat one nonce behind on CSM: that module deposits every minute or two and its rebuild took ~70 s in a 1-CPU container. The chain-read part of that is now under 4 s, so what is left of the 70 s is the writer, and whether the lag closes has to be re-checked on a live pair rather than assumed. Per-module scoping still matters for the same reason: it rebuilds 26 000 keys rather than 512 000.
- A hosted provider can fail a read of the head it has just reported. dRPC answered `eth_getBlockByNumber("latest")` with block 25 610 391 and then refused `eth_getBlockByHash` for it — `no available upstreams … Upstream height 25610391 is less than 25610392` — because the pool routes the second call to a different upstream. Only relevant if this service is ever pointed at a load-balanced endpoint, and then the fix is a retry around the head read rather than anything structural: every read after it is pinned to that hash, which is the property we want.

## Bringing the stand up

The stand is now two services on the same chain — ours and the canonical one — over one Postgres with a database each:

```bash
export PROVIDERS_URLS=http://node-1,http://node-2
docker compose up -d --build            # both sync mainnet themselves
docker compose run --rm tools diff -a http://canonical:3000 -b http://keys-api:3000 -live
docker compose run --rm tools bench -url http://keys-api:3000/v1/keys
```

Ours is ready in under two minutes; the canonical service takes about eight. `docker compose down -v` drops both databases. `docker compose --profile static up -d --build keys-api-static` runs ours with no chain at all, from `testdata/registry.json`.

Two rules the runs taught: benchmark **from inside the compose network** — through published ports on Docker Desktop the host hop caps everything at ~188 MiB/s and every service looks identical — and give the canonical service more than 512 MiB if the endpoint list includes `/v1/modules/keys`.

The four alternative read backends and their tooling (`cmd/server` with its modes, `cmd/seed`, `cmd/e1`, `cmd/load-lsm`, `cmd/torn-check`, `cmd/binschema`, `cmd/freshness`, `tools/fakerpc`, the churn script) were removed once versioned Postgres was decided on. Their measurements are in the README; the code is in git history. Reproducing an embedded-store number means checking out an earlier commit, not running something here.

## What to pick up next

E3 (pagination and `since=<version>` diffs) is the one that changes consumer behaviour, and the versioned backends it needs now exist on both stores. E9's read side is done — the cold sync's chain reads are 37 s — so the cheap thing left there is re-running the cold sync end to end to find out what the writer now costs, and the valuable thing is event logs (item 4), which is about the incremental path rather than the initial load.

## Decided: Postgres versioned is the production path

The embedded store wins on the dump (194 ms against 288 ms) and loses on everything that matters for running the thing:

| | Postgres versioned | embedded store |
|---|---|---|
| Readers | any number of replicas | one process — the file lock is exclusive |
| Writer separate from the reader | yes | no, it has to live in the serving process |
| Rolling deploy | ordinary | the new pod cannot open the store until the old one exits, so a start-before-stop rollout crash-loops |
| Adding a field | `alter table … add column`, or a `jsonb` column for open-ended extras | a length-prefixed tag in a positional record plus a schema id on the version row |
| Backups, point-in-time recovery | established | `Checkpoint()` plus tooling we would write |
| Cost of history | one extra version costs only the churn | ~230–300 MB per version in the full-copy layout |
| Full dump, real mainnet data | 288 ms | 194 ms |

Giving up 1.5× on the dump still leaves 25× against the canonical service, and it buys multiple readers, a writer that is not the server, an ordinary rollout and field changes without an encoding scheme. That trade is not close.

The embedded store stays in the repo as the floor of the serving path and as a possible read-side cache later (via `Checkpoint()`), not as the primary store.

**Adding fields, concretely and without magic.** Real columns for anything we know we want — typed, indexable, visible in `\d`, migrated with one statement. A single `jsonb` column for genuinely open-ended per-consumer extras, so a one-off field for EVM or no-widget needs no migration at all. On the API side the rule from before still holds: the default response stays byte-identical to canonical and extension fields are opt-in, so a field added for one consumer cannot surprise another. Nothing here needs a schema id, a tag registry or a codec version.

What this closes: E5 (two writers over one embedded store) drops to low priority, and the delta layout for the LSM is no longer on the path. What it opens: the extension mechanism above, and E3 on the Postgres versioned backend.

## Decisions that need a person, not an experiment

- Where `safe` comes from: reorg depth in blocks, a CL justified checkpoint, or a configured lag. A CL endpoint would become a hard dependency.
- How deep the retention window is, and who pays for the storage.
- Whether the canonical service adopts published snapshots at all. It is a contract change on a protocol-critical service, so it belongs to its owners; our own service can adopt it first for our consumers, which is also the cheapest way to produce evidence.
- Whether this PoC continues as a comparison harness or becomes a service we run. That decision gates items 6 and 7 above.
- **Language — provisionally settled: Go stays**, on the stated condition that CPU and memory are better by a multiple. Against today's canonical service that condition is met with margin under equal container limits (1 CPU / 512 MiB): 20× on sequential p50, 26–39× under four concurrent dumps, idle memory 5 MiB against 98 MiB, peak 20–33 MiB against 284 MiB.

  What is *not* yet measured is Go against a **lean** TypeScript service — bare `http`/fastify, `pg-query-stream`, hand-rolled serializer, memoized checksum — which is what E8 is for. Expect the CPU gap to narrow substantially there; expect the memory gap to hold, because V8's baseline heap plus its GC behaviour while streaming a 200 MiB response cannot land near 20 MiB. If E8 comes back inside 1.5× on both axes, the multiple-times condition is not met by the language itself and TypeScript deserves to win on maintenance grounds. That is the only scenario that should overturn this.

  Keep separate from the above: the language does not touch the problem that actually hurts. A dump read from a table being rewritten costs Go 6.6× of its own speed, and no runtime fixes that — published snapshots do.
