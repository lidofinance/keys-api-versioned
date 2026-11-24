# keys-api-versioned

A Keys API written in Go that keeps the registry as **published versions** in its own Postgres schema, plus a stand that runs it next to the canonical `lido-keys-api` on the same chain so the two can be compared.

It is meant to be droppable under `ethereum-validators-monitoring` and `lido-mev-monitoring`: the endpoints they call are implemented, and responses are byte-identical to the canonical service, framing whitespace included. Pointing one of them here is a URL change.

How it is put together — the source seam, the schema, one cycle, paging and diffs — is in [docs/architecture.md](docs/architecture.md). Where the work stands and what comes next is in [docs/next-steps.md](docs/next-steps.md).

## Running it

Against a chain: both services, one Postgres, two databases.

```bash
cp .env.example .env   # then set PROVIDERS_URLS, CHAIN_ID and LIDO_LOCATOR
docker compose up -d --build
docker compose run --rm tools diff -a http://canonical:3000 -b http://keys-api:3000 -live
docker compose run --rm tools bench -url http://keys-api:3000/v1/keys
```

`LIDO_LOCATOR` is required and has no per-chain default in the binary. An address compiled in needs a rebuild and a redeploy to correct, and a two-entry map of chains reads as a claim that those are the only chains this runs on — which is false, since devnets are what it is pointed at most often. The well-known values for mainnet and Hoodi are in `.env.example`.

With no chain at all — up in a second, for a demo or for pointing a consumer at something:

```bash
docker compose --profile static up -d --build keys-api-static
```

It shares the database with the chain-fed service but writes schema `static_<chain id>`, so the demo registry never lands among the mainnet rows. A process refuses to start on a store another source has written.

Ours listens on 3010, the canonical service on 3000. Environment variables are documented at the top of [cmd/keys-api/main.go](cmd/keys-api/main.go); the names the canonical service already uses are reused. `KEYS_API_MODE=server` serves without syncing and `KEYS_API_MODE=ingest` syncs without serving, so several read replicas can sit behind one writer. Running more than one writer on a schema is safe: one holds the writer lock and the others wait. A writer whose lock session died cannot commit once another has taken over (`writer_fence`), and it exits instead of queueing behind it.

Generate load from inside the compose network. Through a published port on Docker Desktop the host hop caps everything at ~188 MiB/s and every service looks identical.

## The surface

| Endpoint | Notes |
|---|---|
| `GET /v1/keys` | full dump, streamed |
| `GET /v1/keys/{pubkey}`, `POST /v1/keys/find` | pubkey lookups |
| `GET /v1/modules`, `GET /v1/modules/{id}` | module list and one module |
| `GET /v1/modules/keys` | grouped by module, streamed |
| `GET /v1/modules/{id}/keys`, `POST …/keys/find` | per-module dump and lookup |
| `GET /v1/operators` | grouped by module; what EVM reads for operator names |
| `GET /v1/modules/{id}/operators`, `…/operators/{operator_id}` | one module's operators, one operator |
| `GET /v1/modules/{id}/operators/keys` | module, operators and keys in one stream |
| `GET /v2/modules/operators/keys` | everything in one stream; what mev-monitoring reads |
| `GET /v1/modules/{id}/validators/…/{operator_id}` | the two exit routes: validated as canonical, then canonical's 500 for a deployment without its validators registry |
| `GET /v1/status` | `appVersion` is the tracked canonical release; `clBlockSnapshot` is null: no validator ingest |
| `GET /docs`, `GET /openapi.yaml` | the API document, and a page that renders it |
| `/health`, `/health/ready`, `/metrics` | liveness, readiness (503 until something is published), Prometheus |

## Two surfaces

**`/v1` and `/v2` are the canonical contract and nothing else.** Same paths, same bodies byte for byte, same status codes, and the same treatment of parameters that contract does not declare — they are ignored. An existing consumer cannot tell it was repointed here; that is the whole requirement while consumers migrate.

**`/ext/v1` is everything this service added.** Every canonical read is mirrored there, and only there are the extensions honoured:

| | |
|---|---|
| `?snapshot=<latest\|finalized\|block\|hash>` | serve one request from a specific version |
| `?limit`, `?cursor` | page a dump; the cursor names its version, and `snapshot` alongside it is a `400` |
| `GET /ext/v1/keys/diff?since=` | what changed between two versions |
| `GET /ext/v1/modules/diff?since=` | which modules changed — the cheap poll |
| `GET /ext/v1/modules/{id}/keys/diff?since=` | one module's changes |
| `groupId` on an operator | the node operator group, on every operator representation |

The canonical endpoint that lives on `/v2` is mirrored as `/ext/v1/modules/operators/keys`: this surface has a version axis of its own, and the canonical split between `/v1` and `/v2` — an accident of how that service moved one endpoint — does not leak into it.

A request that sends `snapshot`, `limit` or `cursor` to `/v1` gets the canonical answer plus `X-Ignored-Parameters` naming what was dropped. A header rather than a `400`, because a `400` would itself be a difference from canonical — checked, not assumed: its `ValidationPipe` is `{ transform: true }` with no `whitelist`, so NestJS ignores an undeclared parameter. A header rather than silence, because `?limit=100` ignored on `/v1/keys` streams the entire 216 MiB dump and nothing in the body would say so.

`X-Snapshot-Stale: true` is on both surfaces: it says the served version is known not to reflect the chain while a rebuild is in flight, and a header cannot break a consumer parsing the body.

**Node operator groups** are the one field the extended surface adds to a body rather than to a query. `groupId` comes from the module's `META_REGISTRY` — the same contract the CMv2 operator names come from — and it exists for `curated-onchain-v2` and no other module type, which is why absence and `null` mean different things: the field is **absent** where the module has no group registry, **`null`** where it has one and the operator is in no group (`NO_GROUP_ID`, which on chain is `0` and would be a lie as a group id), and a **number** otherwise. It is on `/ext/v1` only, since the canonical operator object has no such field. Mainnet module 4, at the time of writing: 45 operators, 38 in one of 27 groups, 7 in none.

**The default version belongs to the surface.** `/ext/v1` serves `finalized` unless a request pins something else (`KEYS_API_EXT_PIN`, default `finalized`; `latest` when the source has no finality at all, because there the pin answers 501 by design). `/v1` and `/v2` serve `latest` (`KEYS_API_PIN`), which is what the canonical service serves — its keys-update job reads the registry at `getBlock('latest')`. Finalized there would be a difference in content with no way out, since that surface ignores `?snapshot=`. A consumer on the extended surface gets the version a reorg cannot take away and reaches the head with `?snapshot=latest`; a consumer still on `/v1` notices nothing. The one finalized read the canonical service does do is a single field — `finalizedUsedSigningKeys` on a CSM operator, at `blockTag: 'finalized'` inside a body assembled at the head — which is why its own deposited counters are not consistent with each other.

**Why not `/v3`.** The canonical service versions per endpoint through NestJS URI versioning — `app.enableVersioning({ type: VersioningType.URI })`, then `@Version('2')` on the one controller that moved. `/v3` is one decorator away in a repository this team does not own, on a protocol-critical service. A separate namespace leaves it free to reach `/v3` and beyond without ever colliding, and it tells a consumer the truth: moving to `/ext/v1` is a deliberate change of contract, not a version bump.

The boundary is held by tests rather than by intent: `internal/e2e/surface_test.go` checks that `/v1` ignores each extension and that `/ext/v1` honours it, and `internal/api/openapi_test.go` checks that every canonical read has exactly one mirror at the right path — so a canonical route added without one fails the build, which is the mistake that would actually happen.

The document at `/openapi.yaml` is hand-written and embedded rather than generated: half of what a consumer needs to know is contract rather than struct shape — that a request without `snapshot` gets `latest`, that a cursor pins its version, that a diff's two sides reconstruct a dump when applied in order, that `409` carries the head to jump to. A test holds it to the router in both directions, so a route added without a spec entry fails the build.

Not implemented: validator ingest from the consensus layer (the exit routes answer as a canonical deployment with `VALIDATOR_REGISTRY_ENABLE=false`), throttling, CORS, Sentry.

## One schema per chain

The store lives in a schema named after the chain id — `chain_1`, `chain_560048` — and that is a safety property, not tidiness.

A database filled from one chain and opened with a node on another used to be silent and destructive: the loop probes the stored version's height on the new chain, finds a different hash, correctly concludes the branch is gone, finds nothing that survived, and rebuilds from scratch. Nothing above `reorg deeper than the retained window` is logged. The data is gone and the service looks healthy. Third parties run this service, and confusing `CHAIN_ID` is exactly the mistake they make.

Naming the schema after the chain makes that impossible instead of detectable. A mainnet run does not find the Hoodi rows, so it cannot roll them back; it cold-syncs into an empty schema and the Hoodi data sits untouched. Fix the variable, restart, and the rows are back. A DSN that already carries a `search_path` wins — that is how the test harness gives each test its own schema, and it is the escape hatch for a deployment that wants the layout it already has.

What naming cannot catch is a devnet reusing chain id 1, which lands in the same schema as mainnet with entirely different contracts. For that, the loop refuses to roll back when the stored module set and the chain's share no address — something no reorg can produce, since a fork cannot change which contracts exist.

An existing deployment resyncs once when it picks this up: its rows are in `public` and the service now looks in `chain_<id>`. Nothing is deleted.

## Parity

Tracked canonical version: **5.0.0**, plus the unreleased `develop` fixes to operator names and change detection (through `71fc5b3`). Since 4.0.1 that brought `totalWithdrawnKeys` on every operator, the `invalidName<index>` placeholder for undecodable names, CMv2 renames picked up from the `META_REGISTRY` event, and `Cache-Control` on reads.

The contract is the canonical OpenAPI document itself, kept in `internal/api/testdata/canonical-openapi.json` as served by the mainnet deployment (identical, apart from `info`, to one generated from the `5.0.0` tag). `internal/api/canonical_spec_test.go` requires every canonical operation, parameter, status code and response field to be in `openapi.yaml` with a compatible type. `/openapi.yaml` is a superset: it adds `/ext/v1` and may widen a type. What is deliberately not served (the exit routes' 200, 404 and 425) or served narrower (`clBlockSnapshot`, always `null`) is listed in the test with its reason, and an entry that stops being a difference fails too. A weekly workflow compares the file with the deployment; `make canonical-spec` takes a new release. The document covers shape only: byte equality, error bodies and ignored parameters remain `cmd/diff`'s job.

`cmd/diff` fetches the same endpoint from both services, sorts key arrays (neither implementation promises an order) and compares. `-live` tolerates the two being at different blocks, which they always are when both follow a chain.

Latest run, both services following mainnet in the stand, all eight compared endpoints equal:

```
OK  /v1/status                    OK  /v1/keys?used=true        216.7 MiB vs 216.7 MiB
OK  /v1/modules                   OK  /v1/keys?operatorIndex=1
OK  /v1/modules/1                 OK  /v1/modules/1/keys        199.7 MiB vs 199.7 MiB
OK  /v1/keys  216.7 MiB           OK  /v1/modules/keys          216.7 MiB vs 216.7 MiB
```

### The error surface

`diff -errors` compares what the two services do with requests neither can serve: 21 cases over module and operator references, pubkeys, query parameters, request bodies, methods and unrouted paths. It exists because parity had only ever been checked on successful responses, which is parity of half the contract — and the first run proved the point.

Against canonical 4.0.1, both following Hoodi: **17 of 21 agreed, now 21 of 21.** The four differences and what each would have done to a consumer:

| Case | Canonical | Ours, before | Consequence |
|---|---|---|---|
| `GET /v1/keys/not-a-pubkey` | 400 | 404 | a consumer that retries a 404 and gives up on a 400 retried forever |
| `POST /v1/keys/find` `{"pubkeys":[]}` | 400 | 200, no keys | "your request is wrong" read as "none of your keys exist" |
| `POST /v1/keys/find` with the wrong field name | 400 | 200, no keys | a typo in the field name looked like an empty result |
| `POST /v1/keys` | 404 `Cannot POST /v1/keys` | 405 | Go is right and Fastify is not, but on their surface theirs wins |

And one systematic difference the run surfaced that no single case is about: **every error body was missing `error`.** The canonical service sends `{message, error, statusCode}`; the field was declared in our struct and never set, so a consumer logging or matching on it saw a difference on every failure rather than on an edge case. Two answers do not have it there either: the 425 before the first sync and a 500 are a bare `HttpException` and an unknown error to NestJS, so their body is just `{"statusCode":…,"message":…}` — and no error body ends in a newline.

Also confirmed, because the whole `/ext/v1` split rests on it: the canonical service really does ignore a query parameter it does not declare — `?limit=3`, `?snapshot=12345` and `?nonsense=1` all answer 200 there, which is why `/v1` ignores ours instead of answering 400.

`POST …/keys/find` bodies are capped at 1 MiB, Fastify's default and so the canonical service's, and an oversized one gets its answer: 413 `{"statusCode":413,"message":"Request body is too large"}`. The per-module route validates the list exactly like the global one (it used to drop malformed keys silently and answer an empty list with 200).

The expectations are pinned hermetically in `internal/e2e/surface_test.go`, so they hold without a canonical service running; the live run is what discovers a new one.

Byte-identical covers the framing whitespace and the JSON property order of the canonical DTOs. Where the diff reports "equal ignoring key order", the row order out of Postgres differed between the two queries — neither service sorts, so consumers must not depend on order.

Two differences are structural rather than accidental, and `-live` accounts for both. **Module order:** the canonical service returns modules in physical row order (1, 2, 4, 3 on mainnet) and this one sorts by id, so the runner sorts module, operator and grouped arrays before comparing — without that it read module 4 against module 3 and called every field wrong. **Per-module `lastChangedBlockHash`:** it cannot match by design, since there it moves on every re-read and here only when the module's content changed.

One thing a live pair does show honestly: a module mid-deposit differs on its counters. CSM deposits every minute or two, its rebuild took about 70 s in a 1-CPU container, so this service sat one nonce behind on that module — two consecutive runs went `OK` then `FAIL .data[2].nonce: 5213 vs 5212`. That is lag, not disagreement; the key sets matched byte for byte in both runs. CSM's chain reads are now under 4 s rather than 51 s (see below), so what remains of the 70 s is the writer; whether the lag actually closes needs a fresh live-pair run.

One trailing byte used to separate us from byte-identical on the non-streamed endpoints: `json.Encoder.Encode` appends a newline and Fastify does not.

**Re-run on Hoodi after the read path was rewritten**, both services following the same chain: all eight endpoints equal, 37.1 MiB of `/v1/keys` on both sides. Hoodi is the better chain for this check — five modules against mainnet's four, and two of them exercise code mainnet cannot reach (Sandbox, and `curated-onchain-v2` holding keys). Operators are outside `cmd/diff`, so module 5's were compared by hand: 74 operators, every field equal, names included — which is the `META_REGISTRY` lookup agreeing with the canonical service's own.

**A found defect: a reader could call a current version superseded.** The status re-check that runs at the start of every query continuing a response looked the version up by *block number*. Two versions share a height whenever a reorg is followed by a rebuild — the one the losing branch published, and the one the winning branch publishes at the same height — so the lookup picked between them arbitrarily: a 409 on a version that is perfectly current, or the same coin flip hiding a genuinely superseded one. Found by `internal/fork`, which reorgs an anvil fork of Hoodi and runs the real loop over it; no existing test could reach it, because only a chain re-mines a height. Fixed by naming the version row rather than its height, and controlled — with the old query the test fails with exactly that signature. Details and the rest of the fork suite are in [docs/fork-tests.md](docs/fork-tests.md).

**A found defect: retention could delete rows under a reader.** A concurrent *write* cannot disturb a response here, and not by timing luck — a reader selects by validity range at a pinned block, so a row closed at a later block still matches the predicate and a row opened at a later block does not, whether or not the writer's transaction has committed. Rewriting a whole module mid-dump is a non-event. A *delete* is the opposite case: a row that is gone matches no predicate, and retention deletes the rows of versions falling out of the window — which is where a slow consumer streaming 211 MiB ends up.

Demonstrated rather than argued: with retention running inside a reader's snapshot, the reader streamed **0 keys instead of 1**. The fix is a lease — a shared Postgres advisory lock on the version's block, held for the request — and retention lowers its own boundary to the oldest leased version instead of waiting or skipping. Advisory locks rather than a lease table because they are visible across processes (the reader may be a separate replica from the writer that prunes, which is the point of `KEYS_API_MODE=server`) and are released when the holder's connection drops, so there is no expiry to tune and no sweeper to write. A lease that never goes away is reported: `retention held back by a reader still streaming an older version`. A consumer that stops reading is cut off: each write that reaches the socket has 30 s to complete, so a stalled client gives its connection and lease back while a slow one that keeps reading is never cut. A request that finds the pool exhausted fails after 10 s instead of queueing.

Deliberately not covered by a lease: a reorg. When a version leaves the chain its rows must stop being current, or the next version would inherit keys from the losing branch, so the rollback deletes them. **Telling the reader is a separate mechanism, and it was missing.** A response is not one query — a dump runs one per module — so the rollback landed between two of them and the remaining modules came back empty while the handler finished the JSON: a valid, complete-looking answer assembled from a branch that no longer existed. Exactly the failure this design is against, found by writing down what the tests would have to assert.

The fix re-checks the pinned version's status at the start of every query that continues a response — one single-row select, four per mainnet dump. Before the first flush the reader gets a 409 naming the new head; after it, the connection is aborted (`http.ErrAbortHandler`), because a handler that merely returns leaves net/http to close the chunked body cleanly and the truncation arrives as a well-formed response. What the check is not is instant: it runs per query, so a rollback during module 1's stream — 92 % of a mainnet dump — is noticed when module 2 starts. That is a latency, not a correctness gap; rows already streaming come from a portal opened before the delete, so what reached the client is the pinned version's own content.

**Pubkeys are not unique, and both services say so.** Hoodi's CSM holds 9 pubkeys that appear more than once, 11 rows in excess of the distinct count, six of them shared *between different operators* with different `used` flags — one deposited, the others not. Our read reproduces the canonical service exactly here, so this is the chain's content rather than anyone's bug. Worth knowing before writing a consumer that keys the response by pubkey.

## Measurements

All on the real mainnet registry: 511 877 keys, 731 operators, 4 modules, 216.7 MiB per dump.

### Serving, at 1 CPU and 512 MiB each

| | p50 sequential | p50 at 4 concurrent | wall clock, 8 dumps | container memory |
|---|---|---|---|---|
| canonical TS/NestJS | 7.29 s | 27.13 s | 54.6 s | 347–355 MiB |
| this service, published version | 288 ms | 1.01 s | 2.11 s | 24–37 MiB |

20× sequentially, 27× under concurrency, memory an order of magnitude lower. On one core the gap is *wider* than on an unconstrained laptop (11× / 19× there): the canonical service needs several cores to hide its serialization cost.

Two things that cost it more than the runtime does. It recomputes an EIP-55 checksum per key (`ethers.utils.getAddress` in the key DTO constructor), so a full dump does 500 000 keccak256 hashes over three distinct addresses — `internal/ethaddr` memoizes per address instead. And its default `STREAM_TIMEOUT` of 60 s is close: it already needs 27 s for this dataset at four concurrent dumps, and past the limit the stream is destroyed mid-response.

### The finding that decided the design

A dump costs what it costs *while the registry is being rewritten*, which is most of the time in production:

| | idle | during a registry rewrite |
|---|---|---|
| canonical TS/NestJS | 5.56 s | 8.23 s (+48 %, table bloated 237 MB → 3.2 GB) |
| Go over the live table | 354 ms | 2 350 ms (+560 %) |
| Go over a published version | 221 ms | **222 ms** |

The language is worth 15×; publishing versions is what removes the degradation instead of shrinking it. That is why this service writes versions and never rewrites a row under a reader.

### Our own writer, cold sync from an empty database

Latest, with reads packed across operators. Same 1-CPU container and the same deliberately throttled knobs (batch 40 / concurrency 6) as the run before it, so the difference is the packing.

| | now | before packing |
|---|---|---|
| Empty database to published version | **49 s** in a 1-CPU container | 2 m 28 s – 4 m 15 s (107 s on the host) |
| module 1 — 471 732 keys, 39 operators | 40.5 s | 47 s |
| module 2 — 14 205 keys, 82 operators | 1.5 s | 8 s |
| module 3 (CSM) — 25 940 keys, 609 operators | 3.5 s keys, 2.3 s operators | 51 s |
| module 4 (CMv2) — 0 keys, 1 operator | 0.4 s | 0.5 s |
| On disk | key table 105 MB, key indexes 97 MB, database 211 MB | |
| Idle memory after the sync | 26 MiB of 512 | |
| Quiet cycle, steady state | 0.70 s mean over 12 cycles | 0.81 s |

**Still node-bound, not database-bound.** Module 1's chain read alone is 83 % of the sync, and the writer's share does not stand out above the node's own variance: the same reads with no database at all, at the same knobs, took 49.6 s in one run against a 49.0 s full sync in another, while module 1 came back at 40.5, 43.4 and 46.4 s across three runs an hour apart. So the writer costs single-digit seconds at most, and it is not what to optimise next. Opening the throttled knobs is worth roughly a quarter (37.7 s of chain reads at batch 100 / concurrency 8), and after that the only lever left on module 1 is event logs.

Worth stating because subtracting across runs led the wrong way once already: 37 s of chain reads against a 58 s sync looked like Postgres had become the bottleneck, when most of the gap was the throttling the read-only run had not used.

**One CPU is enough, and wall clock cannot prove it.** Cold syncs at 2 and 4 CPU came back 210 s, 72 s and 165 s, the last being a 1-CPU control taken minutes later — the shared node drifted 3–4× over one afternoon, with module 1's read landing anywhere between 40.5 s and 193 s on identical code. The container's own CPU accounting settles it instead: a full cold sync at a 1-CPU limit used **9.95 CPU-seconds and was throttled zero times** across 769 scheduling periods, which means nothing ever waited for a core. Average utilisation is about 6 %, peaks 0.3 of a core, so more cores have nothing to give.

Which is also the warning for anyone comparing numbers taken from this stand at different times: only measurements from the same afternoon are comparable, and where a claim depends on the difference between two runs, the run that is not the claim needs repeating.

Round-trip bound, not key bound: CSM's 26 000 keys cost more wall clock than module 1's 472 000, because CSM has 609 operators and module 1 has 39. That is what the next section fixes.

### Ingest: packing reads into requests

Key reads used to go one per HTTP request, one operator at a time, so mainnet's 731 operators cost 731 round trips and the tail of small CSM operators dominated the sync. Reads are now packed across operators — into a JSON-RPC batch, or into one `Multicall3.aggregate3` call — and the reader hands whole groups to the client, which decides how they travel. Measured against our own mainnet geth, chain reads only, batch 100 and concurrency 8:

| | CSM (module 3), 25 940 keys, 609 operators | all four modules, 511 877 keys |
|---|---|---|
| One read per request, as before | 58.2 s, 606 requests | — |
| Packed into JSON-RPC batches | 3.8 s, 29 requests | 39.2 s, 273 requests, 9.5 calls per request |
| Packed into Multicall3 aggregates | 3.7 s, 44 requests | 37.7 s, 306 requests, 8.5 calls per request |

**Against a node of our own with a batch of 100, packing is the whole win and aggregation adds nothing measurable.** 15× on CSM comes from batching; the 1.5 s between the last two rows is inside the run-to-run spread. Module 1 barely moves either way (33.2 s against 30.9 s for 471 732 keys) because it is bound by execution on the node — about half a second per 1100-key read — not by latency. The tail was the problem, and packing of any kind removes it.

Aggregation earns its place exactly where a batch cannot carry the group. At the stand's throttled batch of 40, a 200-call group needs five requests plainly and one aggregated, and CSM goes 5.78 s / 53 requests to 4.44 s / 35. So the rule is not "aggregate when possible" but **aggregate when the plain path would need more than one request**, which is what the client does. Wrapping calls that already fit one batch measured slightly worse — 0.99 s against 0.87 s per quiet freshness cycle — because the node executes a dispatching contract for reads it would otherwise answer directly.

**Against a hosted provider, aggregation is the only packing available.** Some hosted providers' free plans refuse a JSON-RPC batch of more than three requests outright (`Batch of more than 3 requests are not allowed on free plan`), which caps the plain path at 3 calls per round trip. Module 2, 14 205 keys, concurrency 2:

| | |
|---|---|
| No aggregation, batch capped at 3 | 8.96 s, 86 requests, 2.8 calls per request |
| Multicall3 | 2.50 s, 18 requests, 10.7 calls per request |

So the two mechanisms are not redundant: batching wins where we run the node, aggregation wins where someone else does and meters it.

**Hoodi, and the batch cap isolated.** The same code against Hoodi reads all five modules — 87 717 keys, 41 + 5 + 4 + 467 + 74 operators — in 13.6 s, and the stand publishes a version from an empty database in **14 s**. Multicall3 is deployed there too, so the aggregation path is the one exercised. Hoodi is also the only chain where two module shapes are covered at all: Sandbox (a third curated registry) and `curated-onchain-v2` with actual keys, whose operator names resolve through the module's `META_REGISTRY` — 74 real names, none of them the generated fallback. On mainnet CMv2 is empty, so none of that code ran there.

Hoodi also gives the cleanest measurement of what aggregation is for, because the only variable is the batch cap. Same node, same module, CSM's 1386 calls:

| batch cap | aggregated | plain |
|---|---|---|
| 100 | 2.6 s, 14 requests | 3.0 s, 18 requests |
| 3, as a metered provider imposes | 2.4 s, 17 requests | 33.5 s, 467 requests |

**The ceiling that shaped the design.** An aggregate's inner calls share one `eth_call` gas budget while separate calls in a JSON-RPC batch each get their own. Two reads of 1100 curated keys in one aggregate come back `out of gas` from mainnet geth; 200 reads of one key each are fine. So aggregates are bounded by *keys returned* rather than by call count (`Call.Cost`), and a read too expensive to share goes out plainly. The first version bounded them by count, which cost nothing on the CSM tail and quietly wrecked module 1 — it bisected on every group, then dragged a permanently lowered call cap into the modules after it, turning CSM's 22 requests into 908. Worth recording because both halves of the mistake were invisible in a per-module run: the failure showed up only in a full sync, in the module that had not failed.

Nothing about this needs configuring per endpoint. Presence of the contract is probed once (devnets have none), a refused aggregate is bisected and the budget lowered to what worked, and an endpoint that refuses a single aggregated read three times within ten minutes pauses aggregation for thirty, after which the budget starts again from the configured value. Only a refusal of the aggregate itself (a call-level error such as `out of gas`, or a response-size limit) bisects; a transport failure or a non-2xx status is returned as it is, so an outage cannot shrink the budget. An error in wording no list recognises is not read as a size limit either: that chunk goes plainly, the budget stays, and a repeat counts toward the pause. Read as a refusal, one such answer cut a three-read batch's budget to one, after which no aggregate went out again and nothing restored it. Rotating `PROVIDERS_URLS` forgets the probe answer and the budget. `PROVIDER_MULTICALL=false` is the manual escape hatch.

### The endpoint list is a fallback list, and it has to behave like one

`PROVIDERS_URLS` takes several endpoints, and until recently the second one was decoration. A hosted provider's key hit its daily compute-unit limit and answered every call with a limit error — **inside a 200 OK**, because that is where JSON-RPC errors live. The fallback loop advanced only on transport failures, so the healthy endpoint behind it was never asked, and ingest sat dead for a day with a working provider one line away in the config.

What a failed call has to answer is not "did it fail" but "is this about the endpoint or about the call":

| About the endpoint → try the next one | About the call → return it |
|---|---|
| `daily limit exceeded`, rate limits, quotas | `execution reverted` |
| `Batch of more than 3 requests are not allowed on free plan` | `out of gas` |
| expired or invalid key, `unauthorized` | `gas required exceeds allowance` |
| method or archive state the node does not serve | `intrinsic gas too low` |
| a non-2xx HTTP status, whatever the body says | anything unrecognised |
| a redirect, a response missing a requested id, a body over 64 MiB | |

`out of gas` staying in the right-hand column is load bearing rather than tidy: the Multicall3 bisect is driven by seeing it, so failing over would break the aggregation described above. Unrecognised wording also stays on the right — a wrongly-retried call spends a request on every provider and still fails, while a wrongly-returned one fails the cycle and is retried on the next tick anyway.

An attempt fails early on a dead endpoint (5 s to connect, 30 s to the response headers; `PROVIDER_TIMEOUT_MS` bounds the whole attempt), and an endpoint that fails three times in a row is tried last for 30 s — still asked when everything ahead of it fails.

`keys_api_rpc_endpoint_requests_total{endpoint,result}` makes the failover visible, which matters because the hazard of a fallback list is not noticing the first entry died.

### What a quiet cycle costs, and what the canonical service's costs

A cold sync is a few hundred requests once. The poll runs every `UPDATE_KEYS_INTERVAL_MS` forever, so on a metered endpoint it is the poll that decides the bill — and the number nobody had was how many requests one iteration makes. `keys_api_rpc_requests_total{method,result}` now answers it from the service instead of from arithmetic; the counts below are that arithmetic, read off both implementations at four mainnet modules with nothing changed.

A failing cycle does not keep that pace: consecutive failures back off from the interval, doubling each time with jitter, up to five minutes, and the first success restores the interval.

| | this service | canonical |
|---|---|---|
| head | 1 `eth_getBlockByNumber(latest)` | 1 `eth_getBlockByNumber(latest)` |
| head timestamp | — (taken from the header it already has) | — (same) |
| finality pin | 1 `eth_getBlockByNumber(finalized)` | — (no finalized pin) |
| staking router | in the module-set request | 1 `stakingRouter` on the locator, every iteration |
| contract version | in the module-set request | 1 `getContractVersion` |
| module set | 1 request: locator + version + `getStakingModules` | 1 `getStakingModules` |
| module types | in the nonce batch | 4 `getType`, concurrent, so 1 batched request |
| nonces | 1 batched request, 8 calls | 4 sequential `getCurrentNonce`, so 4 requests |
| reorg check | — while the version's block is finalized | 2 `getFullBlock` + the range, per module: ~12 |
| operator events | 1 `eth_getLogs` per module family: 2 | 4 `eth_getLogs`, one per module |
| **total** | **6**, and **1** when the head has not moved | **~25**, and **1** when the head has not moved |

Both are one poll of one registry, so the gap is entirely in how the requests are shaped. Three things account for most of it: the reorg check re-fetches the same two blocks for every module because a negative answer is not memoised; the nonce reads are sequential `await`s, so the batching the provider is configured for never engages; and the locator is resolved on every iteration to get an address that changes about once a year. The canonical iteration also short-circuits after the first request when the head hash has not moved, which at a 12-second poll on a 12-second chain saves roughly every other iteration — the 25 is the cost of the iterations that do run.

**A poll that finds the head it already has stops after one request.** No new block means no new state, so every signal below would be read at a point already accounted for — and nothing is written either, since extending a version to a head it is already recorded at is a write that says nothing. This is the canonical service's own short-circuit (`Same blockHash, updating is not required`), and it is what makes its five-second default affordable: most polls of a twelve-second chain see the block they saw last time. The consequence is that the bill is set by *blocks observed* rather than by polls, so lowering the interval below the slot time buys phase and almost no cost.

Two of our own eight are worth calling out. `eth_getBlockByHash` fetches a timestamp that was already in the header from the request before it, and is removable. The pair of `eth_getLogs` was four until the scan was grouped by module family; `address` in a filter takes a list, and the loop only ever wants one boolean, so asking per contract paid a round trip per contract for no extra information. The range those two filters cover is now only the blocks since every module's operators were last known current: the last empty scan, or the last rebuild that re-read all of them. Not the published version's block — a rebuild scoped to one module's keys moves it without reading anyone's operators, and starting there skipped the blocks before it. It used to start at the published version's own block, and a version that nothing changed is never rewritten, so the window grew until `MAX_BLOCKS_OVERLAP` forced a rebuild — the same blocks re-read five times a minute for up to three hours, on the method a metered provider bills hardest. The mark does not move when a scan *finds* something: that signal is discharged by a rebuild which re-reads operators, a rebuild can fail, and advancing first would answer "no events" next cycle to a question nothing had acted on. A reorg drops the mark, because the blocks it covered were scanned on a branch that is gone.

Both services re-read the locator and the router's contract version every iteration, and for the same reason: a deploy replaces the router or changes the tuple's shape under a service that is already running, and one that resolved those once at startup keeps reading a router nobody uses or reads a v4 registry with v3 assumptions — silent, and healthy-looking from every other metric. The difference is only that here they are two more calls in a request that was going out anyway, so watching them costs nothing per cycle. `keys_api_staking_router_changes_total` and `keys_api_staking_router_contract_version` are what make either event visible without reading a log, and either one forces the module rows to be written again on the next cycle — the keys are in the module contracts and are not implicated, so a protocol deploy costs a module read and not a 500 000-key one.

Two savings came out of writing the budget down rather than reasoning about it. The event scan used to run before the freshness decision, so a cycle where a nonce had already moved spent both `eth_getLogs` computing a boolean the rebuild had made irrelevant — it is now asked for only when every cheaper signal says nothing changed. And the locator read stopped being its own request: it travels as a second call in the module-set batch, which is what makes it affordable to re-check every cycle rather than pinning the router for the life of the process.

**A restart is not a cold start.** The state a cycle needs comes from the database: one query for the newest published version, whose row carries the per-module nonces the last cycle observed. A process that comes back up compares those against the chain and reads no keys unless something moved. It pays two things once — an extra `eth_call`, because the router address is resolved before there is anything to verify it against, and one wide event scan, because the scan mark lives in memory — and is back to the ordinary budget on its second cycle.

Two exceptions, both worth knowing before setting `MAX_BLOCKS_OVERLAP` narrow. The window counts from the last full read, which each version records, not from the restart, so restarts more often than the window cannot put the audit off. When it is already due as the writer comes up, the audit re-reads everything — so downtime longer than the window costs a full read — and until it publishes, `latest` and `finalized` answer 425: the gap may be downtime, and the newest version that far behind the chain. Pins by number or hash are still served, and the pod stays ready so clients see the 425. And a crash *during* the cold sync is not resumable: an unfinished version stays `building`, `Latest` considers only published and stale ones, so the next start reads the whole registry again. A pod that keeps dying before its first version lands re-reads every key every time it comes up, which is how a daily provider quota goes in an afternoon. All three behaviours are pinned by tests in `internal/ingest/rpc_budget_test.go`; the last one is recorded rather than fixed, because resuming means deciding what a half-read version at a block the chain has moved past is worth.

**Two things about that table are accidents rather than decisions, and both are now fixed.**

The first is that every read ran at the fastest read's cadence. The cycle polls the head every twelve seconds because that is how often the head moves; the finalized block advances once per epoch by definition, and an operator rename is a months-apart event — yet both were read on every tick. They are now paced by the epoch, computed as `SECONDS_PER_SLOT × SLOTS_PER_EPOCH` (12 × 32 by default) rather than fetched, since what the loop wants is a duration and not a consensus client. A fresh process still does both on its first cycle: waiting an epoch to learn anything would cost more than it saves. Because a compute-unit provider bills the *call* and not the block range, scanning once per epoch over a 32-slot range costs what one per-cycle scan cost — so this is a division of the bill, not a trade against freshness of anything that moves.

The second is aggregation. `PROVIDER_MULTICALL` wraps reads into one `Multicall3.aggregate3` only when a JSON-RPC batch cannot carry the group, which is right for a node of our own — measured above, the plain path is faster because the node answers reads directly instead of executing a dispatcher. On a metered provider the comparison inverts: eleven getters in one batch is one round trip and *eleven billed calls*, where one aggregate is one of each. `PROVIDER_METERED` aggregates any multi-call read, taking a quiet cycle from eleven billed calls to two — two rather than one because the module addresses the second step reads come out of the first, so the dependency is real. A flag rather than a list of known provider hosts: the list would go stale, and a deployment knows what it pays for.

**It defaults to true**, because the two mistakes cost differently. Forgetting it on a hosted provider spends five times the compute units on every cycle forever, which is enough to empty a daily quota within hours. Leaving it on against a node of our own costs the dispatcher the node executes instead of answering directly — 0.99 s against 0.87 s on a quiet cycle. One is an outage and the other is 120 ms, so the default belongs on the side that fails cheaply; a stand running its own node sets `PROVIDER_METERED=false`.

Together, against a hosted provider's published weights — `eth_call` at 20 CU, `eth_getLogs` at 75 — two writers sharing one key go from roughly 6.5M compute units a day to about 1.15M at a twelve-second interval, and to ~230k at sixty seconds. A daily quota that used to last hours then lasts days.

Two tests hold this, and they answer different questions. `TestQuietCycleRequestBudget` asserts the counts per method exactly — a cycle makes whole requests, so any new round trip fails it and either goes away or changes one number in a diff a reviewer can argue with. `TestADayOfPollingFitsTheProviderQuota` takes the profile it measures from real cycles, prices it against the provider's published compute-unit weights, projects a day of it at the deployed interval and fails when it no longer fits the daily limit the test is given. Both are worth having: one added call often still fits, and the day it stops fitting is not the day it was added.

The projection reports what it is made of, so a failure names the method that grew:

```
projected 1583740 CU/day for 2 writer(s) at a 5s interval, quota 2000000 (79% used)
  eth_call                   14400 calls      288000 CU
  eth_getBlockByNumber       17505 calls      420120 CU
  eth_getLogs                  450 calls       33750 CU
```

The weights are the provider's and are marked as such; where the published figure is unclear the pessimistic value is used, so the test fails early rather than passing wrongly. Verified against three regressions it has to catch: un-pacing the operator scan (184 % of quota), removing the head-unchanged short-circuit (120 %), and turning off metered aggregation (209 %).

### Paging and diffs

| | |
|---|---|
| Full walk, 52 pages of 10 000 | 511 877 keys, no gaps, no duplicates; median page 35 ms (min 18, max 42) |
| Version stability across a 22 s walk | one pinned version while the chain advanced 3 blocks |
| Diff between two versions | 2–5 ms and 527 bytes, against 227 MB for the full dump |
| Diff correctness | an old dump plus the diff equals a fresh dump, including a deposit flipping `used` |

### Binary storage

Pubkeys and signatures as `bytea`, and a two-byte module id instead of a 42-character address on every row:

| | hex | bytes |
|---|---|---|
| Key table | 200 MB | **100 MB** |
| Full dump, p50 sequential | 380 ms | **374 ms** |
| Full dump, p50 at 4 concurrent | **1.11 s** | 1.53 s |
| Container memory peak | 27 MiB | 17 MiB |

Two results I predicted wrong, in both directions. Sequentially the encoding costs nothing — half the bytes travel from Postgres and that offsets it, against my estimate of +15–25 %. Under CPU-bound concurrency it costs about 37 %, because with one core serving four dumps there is no spare capacity to hide it. Worth it, with the caveat that the workload which punishes it is exactly the one paging and diffs remove.

### Findings worth passing to the canonical service's owners

- **512 MiB is not enough for it on mainnet, in two separate places.** `/v1/modules/keys` is not streamed there — it builds the whole 216.7 MiB response in memory. Ours streams it: 227 MB out at an 11 MiB peak. And its *cold sync* does not fit either: in this stand it restart-looped 12 times at 469 MiB before the limit was raised to 1.5 GiB. Ours syncs the same registry inside 512 MiB.
- **A Node OOM there looks like a clean restart.** `docker inspect` reports `OOMKilled=false, exit=0`, because PID 1 is `yarn` and it survives its child being killed; the only trace is `Killed` in the log. Anything watching restart reasons in Kubernetes would be misled.
- **Per-module `lastChangedBlockHash` collapses.** Checked on the live service: all four modules carried the same value as the global one. It moves whenever a module is *re-read*, so a restart, a stall past `MAX_BLOCKS_OVERLAP` or a reorg stamps every module at once, and afterwards you cannot tell "module 2 unchanged" from "everything was stamped at that block". Use `nonce` for skip decisions. Ours derives the stamp from content instead — verified at mainnet scale: a full re-read of every module published a version whose stamp was byte-identical to the previous one.

### What was removed after the decision

This repo used to carry four more read backends — the canonical schema read directly, an in-process snapshot, and two embedded log-structured stores — plus the tools that loaded and churned them. They existed to answer "how much is the storage engine worth", and they answered it: an embedded store served a dump in 194 ms against 288 ms for versioned Postgres, and lost on everything that matters for running the thing — one process only, because the file lock is exclusive; no rolling deploy; adding a field becomes an encoding scheme; ~230–300 MB per version in the full-copy layout. The numbers above stay, the code does not; git history has it, and [docs/snapshot-modifiers.md](docs/snapshot-modifiers.md) has the design reasoning.

## Layout

```
cmd/keys-api      the service: ingest plus API
cmd/diff          parity runner against the canonical service
cmd/bench         dump latency, throughput and server RSS
cmd/ingest        chain probe: modules, operators, key throughput, no database
internal/source   the seam: what a registry looks like, with no contract knowledge
  lidocore        Lido core — staking router, curated and community modules
  static          a JSON file, for tests and for running without a chain
internal/ingest   the sync loop, the version writer, the schema
internal/store    reading published versions: pins, paging, diffs
internal/api      routing and a hand-rolled streaming encoder
internal/eth      JSON-RPC client and the canonical service's own ABI files
internal/metrics  Prometheus surface
docs/             architecture, next steps, the versioning design note
```
