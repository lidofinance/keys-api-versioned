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
7. **Extension fields we can add without asking anyone.** The point of owning the service is adding a field for an EVM or no-widget need in an afternoon instead of negotiating capacity with another team. Two things make that possible, and both are cheap if designed in now rather than retrofitted. *Storage:* a fixed core record plus an optional extension section (length-prefixed tags, or CBOR), so a new field is a new tag — existing records are never rewritten and readers ignore tags they do not know. *Contract:* the default response stays byte-identical to canonical, and extensions are opt-in (`?fields=…`, or a separate path), so adding a field cannot break a consumer that did not ask for it. Because everything is versioned, the schema id belongs on the version row: new versions carry new fields, old versions keep their codec, no migration. Acceptance: adding a denormalised field (operator name next to a key, say) touches the ingest and one codec tag, changes no existing record, and leaves `cmd/diff` green on the default response.
8. **Swagger, since the choice is Go.** The canonical service publishes an OpenAPI document through `@nestjs/swagger`, and consumers may be generating clients from it. In Go this is either annotations (`swaggo`) or a hand-written spec served with a UI. Acceptance: the published document describes the same surface as canonical for the endpoints this service serves, and a client generated from it works against both.
9. **Make `lastChangedBlockHash` truthful in our version** (wanted, not required). The canonical field moves whenever a module is *re-read*, so a restart, a stall past 30 blocks or a reorg stamps every module with the same block and the per-module signal collapses. Fix it by deriving the field from content rather than from the act of reading: keep a per-module digest on the version row (nonce plus a rolling hash over the module's keys) and move `lastChangedBlockHash` only when the digest differs from the previous version's. The digest also covers the case where a reorg returns the same nonce with different content. Keep `meta.elBlockSnapshot.lastChangedBlockHash` as it is, for compatibility. Acceptance: force a full re-read of every module and confirm the per-module value does *not* move for modules whose content is unchanged — the exact behaviour that fails on the canonical service today.
10. **Metrics and health that match the deployment.** Prometheus endpoint, the alert-relevant gauges (snapshot age, staleness, rebuild duration). Acceptance: the existing dashboards for the canonical service can point at this without editing panels.

## Experiment backlog

Each has a threshold, so the outcome is a decision. **E6 is next** — see the note under it for why.

- **E2 — reorg handling as rollback plus catch-up.** With versions, a reorg should not be a resync: find the fork point F (walk your own version table comparing stored block hashes against the node), mark everything above it superseded, then run the ordinary freshness check between F and the new head — only modules whose state actually differs get re-read. Two things to settle by experiment: the behaviour when F is outside the retention window (the one case that does force a full rebuild), and whether nonce equality is trustworthy across a reorg or whether the reorged range needs an event scan instead. Threshold: a reorg with no key changes in the reorged range costs zero key reads, and a defined answer (not a 500) when F is gone.
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

1. **Open the throttles first, before writing any code.** Batch size and concurrency were deliberately set below default for a shared node. Measure the node's real capacity at batch 100–500 and concurrency 8–16; this is configuration and may well be most of the win.
2. **Parallelise across operators.** Keys of different operators at a fixed block hash are independent reads, so a bounded pool (8–16 in flight) converts a per-operator serial wait into a per-batch one. Note why the canonical service cannot do this comfortably and we can: it writes a whole module inside one transaction, while our `building` status lets N workers write into an invisible version concurrently and publish with one flip.
3. **Aggregate calls with Multicall3.** This attacks the measured bottleneck directly: instead of one `eth_call` per operator range, pack many `getSigningKeys(op, offset, limit)` into a single aggregated call, so a tail of 500 small operators collapses into a handful of round trips. Watch two ceilings — the node's `eth_call` gas cap (`--rpc.gascap`) and response size, so the aggregate has to be chunked deliberately.
4. **Build from event logs instead of per-operator calls.** `eth_getLogs` over ranges returns thousands of key additions per response with no per-operator round trips, and erigon often serves wide ranges better than geth — we have one of each to compare. Caveats worth checking before committing: the add-key event carries the pubkey but likely not the deposit signature, and `used`/`vetted` are state rather than events, so logs give you *what changed* while calls still supply the rest. That makes logs the right tool for the incremental path and a partial one for the initial load.
5. **WebSocket is not the lever here, with one exception.** The cost is request latency plus server-side execution; HTTP keep-alive already removes most of what WS would save, and WS adds reconnect and backpressure handling. The exception is `eth_subscribe("logs")` for steady state — that replaces the polling loop entirely and delivers the per-module "something changed" signal without asking for nonces at all.

**E9 — ingest throughput.** Measure the four options above against both nodes, starting from the unthrottled baseline. Threshold: a cold sync of the mainnet key set inside 10 minutes, and a steady-state cycle that costs a handful of calls when nothing changed.

## Rough edges to fix while passing by

- The key table's indexes are now nearly the size of the table itself (97 MB against 105 MB on mainnet): five of them, three added for paging and diffs. `kv_from` and `kv_to` are the candidates to drop — the diff predicates may be served well enough by `kv_range`. Measure before removing.
- An audit rebuild republishes a content-identical version, which costs a version row and a full read. Harmless, but it means the version table gains a row every audit interval whether or not anything happened; retention absorbs it.
- The canonical service will not boot if `PROVIDERS_URLS` has no TLD in the host — its `isUrl` validation rejects a bare service name like `fakerpc`, and the failure surfaces as a misleading `Cannot find module dist/src/main` from the `start:prod` fallback. Only relevant now if someone points it at an in-cluster name.
- ~~`/v1/modules/keys` builds the whole grouped response in memory on our side too.~~ Fixed: it streams now, 227 MB out at an 11 MiB peak. Worth recording how it was found, because it is the argument for keeping the stand: a parity run against the canonical service reported `216.7 MiB vs 74.1 MiB` with empty `key` strings on our side. That endpoint marshals with `encoding/json` rather than the streaming encoder, and the binary store fills only the byte fields — so it had been serving keys with `"key": ""`. Filling the strings then made it OOM at 512 MiB, which is what forced the streaming rewrite. Two bugs, one of them mine and invisible to every test, caught by comparing against the thing being replaced.
- On a live pair this service sits one nonce behind on CSM: that module deposits every minute or two and its rebuild takes ~70 s in a 1-CPU container. Inherent until CSM reads get faster (Multicall3 or event logs, item E9) — and a reason per-module scoping matters, since it rebuilds 26 000 keys rather than 512 000.

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

E3 (pagination and `since=<version>` diffs) is the one that changes consumer behaviour, and the versioned backends it needs now exist on both stores. E9 (ingest throughput) is the cheapest win available — the first step there is configuration, not code: open the provider knobs and re-measure the 8-minute sync before writing a parallel fetcher.

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
