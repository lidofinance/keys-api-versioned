# keys-api-versioned

A Keys API in Go that stores the registry as published versions in Postgres. It replaces the canonical `lido-keys-api`: `/v1` and `/v2` answer byte for byte as canonical 6.0.0 does, apart from the differences in [docs/migrating-from-canonical.md](docs/migrating-from-canonical.md), so moving a consumer is a URL change.

| | |
|---|---|
| [docs/operations.md](docs/operations.md) | deploying it: requirements, variables, the canonical-to-here mapping, upgrades, alerts |
| [docs/migrating-from-canonical.md](docs/migrating-from-canonical.md) | known consumers, the differences they can see, the latest parity run |
| [docs/architecture.md](docs/architecture.md) | the source seam, the schema, one cycle, paging and diffs, metrics and logs |
| [docs/fork-tests.md](docs/fork-tests.md) | the anvil-fork test suite and what it found |
| [docs/snapshot-modifiers.md](docs/snapshot-modifiers.md) | the versioning design note |
| [docs/next-steps.md](docs/next-steps.md) | what is not done |

## Running it

Against a chain, next to the canonical service, one Postgres with two databases:

```bash
cp .env.example .env   # then set PROVIDERS_URLS, CHAIN_ID and LIDO_LOCATOR
docker compose up -d --build
docker compose run --rm tools diff -a http://canonical:3000 -b http://keys-api:3000 -live
docker compose run --rm tools bench -url http://keys-api:3000/v1/keys
```

- `LIDO_LOCATOR` is required and has no per-chain default: devnets are the most common target. Mainnet and Hoodi values are in `.env.example`.
- This service listens on 3010, canonical on 3000. Variables are in [docs/operations.md](docs/operations.md#configuration); canonical names are reused.
- `KEYS_API_MODE=server` serves without syncing, `ingest` syncs without serving, so read replicas can sit behind one writer. Several writers on one schema are safe: one holds the lock, the rest wait, and a writer that lost its lock session exits (`writer_fence`).
- Generate load from inside the compose network. Through a published port on Docker Desktop everything caps at ~188 MiB/s and all services look the same.

Without a chain, from a registry file:

```bash
docker compose --profile static up -d --build keys-api-static
```

It writes schema `static_<chain id>` in the same database. A process refuses to start on a store another source has written.

## The surface

| Endpoint | Notes |
|---|---|
| `GET /v1/keys` | full dump, streamed |
| `GET /v1/keys/{pubkey}`, `POST /v1/keys/find` | pubkey lookups |
| `GET /v1/modules`, `GET /v1/modules/{id}` | module list and one module |
| `GET /v1/modules/keys` | grouped by module, streamed |
| `GET /v1/modules/{id}/keys`, `POST …/keys/find` | per-module dump and lookup |
| `GET /v1/operators` | grouped by module; EVM reads operator names here |
| `GET /v1/modules/{id}/operators`, `…/operators/{operator_id}` | one module's operators, one operator |
| `GET /v1/modules/{id}/operators/keys` | module, operators and keys in one stream |
| `GET /v2/modules/operators/keys` | everything in one stream; mev-monitoring reads it |
| `GET /v1/modules/{id}/validators/…/{operator_id}` | the two exit routes: validated as canonical, then canonical's 500 for a deployment without a validators registry |
| `GET /v1/status` | `appVersion` is the tracked canonical release; `clBlockSnapshot` is `null` (no validator ingest) |
| `GET /docs`, `GET /openapi.yaml` | the API document and its rendered page |
| `/health`, `/health/ready`, `/metrics` | liveness, readiness (503 until a version is published), Prometheus |

Not implemented: validator ingest from the consensus layer (the exit routes answer as canonical with `VALIDATOR_REGISTRY_ENABLE=false`), throttling, CORS, Sentry.

## Two surfaces

`/v1` and `/v2` are the canonical contract: same paths, bodies, status codes, and undeclared parameters are ignored, as canonical's `ValidationPipe` (`{ transform: true }`, no `whitelist`) does.

`/ext/v1` mirrors every canonical read and is the only place the extensions work:

| | |
|---|---|
| `?snapshot=<latest\|finalized\|block\|hash>` | serve the request from a specific version |
| `?limit`, `?cursor` | page a dump; the cursor names its version, so `snapshot` next to it is a `400` |
| `GET /ext/v1/keys/diff?since=` | what changed between two versions |
| `GET /ext/v1/modules/diff?since=` | which modules changed, the cheap poll |
| `GET /ext/v1/modules/{id}/keys/diff?since=` | one module's changes |
| `groupId` on an operator | node operator group from the module's `META_REGISTRY` |

- The canonical `/v2` endpoint is mirrored as `/ext/v1/modules/operators/keys`; the `/v1`/`/v2` split does not carry over.
- `snapshot`, `limit` or `cursor` sent to `/v1` are ignored and named in `X-Ignored-Parameters`. A `400` would differ from canonical, and silence would hide that `?limit=100` streamed the whole 216 MiB dump.
- `X-Snapshot-Stale: true` (both surfaces): the served version is known to be behind while a rebuild runs.
- `groupId` exists only for `curated-onchain-v2`: absent where the module has no group registry, `null` for an operator in no group (`NO_GROUP_ID`, `0` on chain), a number otherwise. Mainnet module 4: 45 operators, 38 in 27 groups.
- Default version: `/v1` and `/v2` serve `latest`, as canonical does (`KEYS_API_PIN`). `/ext/v1` serves `finalized` (`KEYS_API_EXT_PIN`), or `latest` when the source has no finality.
- Not `/v3`: canonical versions per endpoint with NestJS URI versioning, so `/v3` belongs to its owners. `/ext/v1` cannot collide with it.

Tests hold the boundary:

- `internal/e2e/surface_test.go`: `/v1` ignores each extension, `/ext/v1` honours it.
- `internal/api/openapi_test.go`: every canonical read has exactly one mirror, and `/openapi.yaml` matches the router both ways. The document is hand-written because much of it is contract rather than struct shape: default pins, cursors, diff semantics, the `409` head.

## One schema per chain

The store lives in schema `chain_<chain id>` (`chain_1`, `chain_560048`).

- A wrong `CHAIN_ID` lands in an empty schema and cold-syncs; the other chain's rows stay untouched. Without it, the reorg walk found no common version and silently rebuilt over the old chain's data.
- A DSN with its own `search_path` wins. Tests use it for per-test schemas.
- A devnet reusing chain id 1 shares a schema with mainnet. The loop refuses to roll back when the stored module set and the chain's have no address in common.
- `schema.sql` carries `store.SchemaVersion`. A newer writer rebuilds an older store and logs a WARN; an older writer finds a newer store, changes nothing and exits. Releases from before the number still drop a newer store.
- A server answering 425 logs the reason once a minute: no store, an older writer, an overdue full read, or the first sync. `/health/ready` returns the same text.
- An existing deployment resyncs once on upgrade: its rows are in `public`, now unread. Nothing is deleted.

## Parity

Tracked canonical release: 6.0.0. Results of the latest run and the differences a consumer can see are in [docs/migrating-from-canonical.md](docs/migrating-from-canonical.md).

- Contract: the canonical OpenAPI document as served by the mainnet deployment, in `internal/api/testdata/canonical-openapi.json`. `internal/api/canonical_spec_test.go` requires every canonical operation, parameter, status and field in `openapi.yaml` with a compatible type; deliberate gaps (the exit routes' 200/404/425, `clBlockSnapshot`) are listed with reasons, and a gap that closes fails the test. A weekly workflow compares the file with the deployment; `make canonical-spec` takes a new release.
- `cmd/diff` fetches an endpoint from both services and compares the bodies. `-live` allows the two to be at different blocks and leaves out `meta` and per-module `lastChangedBlockHash`.
- `cmd/diff -errors` compares 21 failing requests: bad references, pubkeys, parameters, bodies, methods, unrouted paths. Error bodies are `{message, error, statusCode}`; the 425 and 500 are `{statusCode, message}`, as canonical sends them.
- `POST …/keys/find`: bodies over 1 MiB get 413, as Fastify's default. Malformed bodies get Fastify's and class-validator's answers; `internal/api/findbody_test.go` pins them.
- Hermetic copies of these expectations are in `internal/e2e/surface_test.go`, so they hold without a canonical service.
- In `-live`, a module mid-deposit (CSM deposits every minute or two) can be one nonce behind. That is lag; the key sets matched.
- Pubkeys are not unique on chain. Hoodi's CSM has 9 repeated pubkeys, some under different operators with different `used`. Both services return all rows, so key a response by pubkey with care.

## Consistency while streaming

A reader selects rows by validity range at its pinned block, so concurrent writes cannot change a response. Deletes can, and two mechanisms cover them:

- Retention: a request holds a shared advisory lock on its version's block, and retention keeps everything at or above the oldest held one instead of waiting. A write that does not reach the socket in 30 s ends the request, so a stalled client gives the lease back; a request that finds the pool exhausted fails after 10 s. A long-held lease is logged.
- Reorg: rollback deletes the losing branch's rows. Each per-module query in a response re-checks the version's status first. Before the first flush the answer is 409 with the new head; after it, the connection is aborted (`http.ErrAbortHandler`), so a cut-off dump never looks complete. The check is per module, so a rollback during module 1 (92 % of a mainnet dump) is noticed when module 2 starts; rows already streaming come from a portal opened before the delete.

## Measurements

Mainnet registry: 511 877 keys, 731 operators, 4 modules, 216.7 MiB per dump.

### Serving, 1 CPU and 512 MiB each

| | p50 sequential | p50 at 4 concurrent | 8 dumps, wall clock | container memory |
|---|---|---|---|---|
| canonical TS/NestJS | 7.29 s | 27.13 s | 54.6 s | 347–355 MiB |
| this service | 288 ms | 1.01 s | 2.11 s | 24–37 MiB |

During a registry rewrite, which is most of the time in production:

| | idle | during a rewrite |
|---|---|---|
| canonical TS/NestJS | 5.56 s | 8.23 s (+48 %, table 237 MB → 3.2 GB) |
| Go over the live table | 354 ms | 2 350 ms (+560 %) |
| Go over a published version | 221 ms | 222 ms |

Go gives 15×; published versions remove the slowdown under writes. That is why rows are never rewritten under a reader.

### Cold sync, empty database

1-CPU container, batch 40, concurrency 6:

| | now | before read packing |
|---|---|---|
| empty database to published version | 49 s | 2 m 28 s – 4 m 15 s |
| module 1: 471 732 keys, 39 operators | 40.5 s | 47 s |
| module 2: 14 205 keys, 82 operators | 1.5 s | 8 s |
| module 3 (CSM): 25 940 keys, 609 operators | 3.5 s keys, 2.3 s operators | 51 s |
| module 4 (CMv2): 0 keys, 1 operator | 0.4 s | 0.5 s |
| on disk | keys 105 MB, key indexes 97 MB, database 211 MB | |
| idle memory | 26 MiB | |
| quiet cycle | 0.70 s mean | 0.81 s |

- The node is the bottleneck. Module 1's chain read is 83 % of the sync; chain reads alone, with no database, took 49.6 s. Batch 100 / concurrency 8 cuts reads to 37.7 s.
- One CPU is enough: a cold sync used 9.95 CPU-seconds with zero throttled periods out of 769.
- The shared node drifted 3–4× within an afternoon (module 1 between 40.5 s and 193 s on the same code). Compare only runs taken together.
- Hoodi (5 modules, 87 717 keys, Sandbox and CMv2 with keys): empty database to published version in 14 s.

### Read packing

Key reads are packed across operators into a JSON-RPC batch or one `Multicall3.aggregate3`. Mainnet, own geth, batch 100, concurrency 8:

| | CSM: 25 940 keys, 609 operators | all modules: 511 877 keys |
|---|---|---|
| one read per request | 58.2 s, 606 requests | — |
| JSON-RPC batches | 3.8 s, 29 requests | 39.2 s, 273 requests |
| Multicall3 | 3.7 s, 44 requests | 37.7 s, 306 requests |

Hoodi CSM, 1386 calls, by batch cap:

| batch cap | Multicall3 | plain |
|---|---|---|
| 100 | 2.6 s, 14 requests | 3.0 s, 18 requests |
| 3 (a free-plan hosted provider) | 2.4 s, 17 requests | 33.5 s, 467 requests |

- With a node of our own, batching is the whole win. Aggregating calls that fit one batch is slightly slower (0.99 s against 0.87 s per quiet cycle), so by default the client aggregates only when the plain path needs more than one request.
- `PROVIDER_METERED=true` (the default) aggregates every multi-call read: a hosted provider bills each call inside a batch, so a quiet cycle drops from 11 billed calls to 2. Forgetting it on a hosted provider empties a daily quota in hours; leaving it on against an own node costs 120 ms. A stand with its own node sets it to `false`.
- Calls in one aggregate share one `eth_call` gas budget, so aggregates are sized by keys returned (`Call.Cost`), not by call count. Two 1100-key reads in one aggregate run out of gas on mainnet geth.
- A refused aggregate (`out of gas`, a response size limit) is bisected and the budget lowered. Transport errors, non-2xx and unrecognised errors do not shrink it. Three refusals of a single read in ten minutes pause aggregation for 30 minutes. `PROVIDER_MULTICALL=false` turns it off.

### Provider failover

`PROVIDERS_URLS` is a fallback list. Hosted providers report quota errors inside a 200 OK, so the client classifies each error by whether another endpoint would answer differently (`internal/eth/rpcerror.go`):

| Next endpoint | Return the error |
|---|---|
| `daily limit exceeded`, rate limits, quotas | `execution reverted` |
| `Batch of more than 3 requests are not allowed on free plan` | `out of gas` (drives the Multicall3 bisect) |
| expired or invalid key, `unauthorized` | `gas required exceeds allowance` |
| method or archive state the node does not serve | `intrinsic gas too low` |
| non-2xx status, redirect, missing response id, body over 64 MiB | anything unrecognised |

Timeouts: 5 s to connect, 30 s to headers, `PROVIDER_TIMEOUT_MS` for the whole attempt. An endpoint failing three times in a row goes last for 30 s. `keys_api_rpc_endpoint_requests_total{endpoint,result}` shows which endpoint is serving.

### RPC per quiet cycle

Four mainnet modules, nothing changed. Counted by `keys_api_rpc_requests_total{method,result}`:

| | this service | canonical |
|---|---|---|
| head | 1 `eth_getBlockByNumber(latest)` | 1 `eth_getBlockByNumber(latest)` |
| finality pin | 1 `eth_getBlockByNumber(finalized)` | — |
| staking router, contract version, module set | 1 request | 3 requests |
| module types and nonces | 1 batched request | 1 batched + 4 sequential |
| reorg check | — while the version's block is finalized | ~12 (`getFullBlock` ×2 plus the range, per module) |
| operator events | 2 `eth_getLogs`, one per module family | 4 `eth_getLogs`, one per module |
| total | 6; 1 if the head has not moved | ~25; 1 if the head has not moved |

- An unchanged head stops the cycle after one request, as canonical does, so cost follows blocks, not polls.
- The finalized block and the operator-event scan are paced by the epoch (`SECONDS_PER_SLOT × SLOTS_PER_EPOCH`); a fresh process reads both on its first cycle.
- The event scan covers only blocks since operators were last known current, and runs only when cheaper signals show no change.
- Locator and router contract version are re-read every cycle inside the module-set request. A change bumps `keys_api_staking_router_changes_total` / `keys_api_staking_router_contract_version` and rewrites module rows, not keys.
- `eth_getBlockByHash` for the head timestamp is still there and removable.
- Failing cycles back off from the interval, doubling with jitter up to 5 minutes.
- Against published compute-unit weights (`eth_call` 20, `eth_getLogs` 75), two writers on one key went from ~6.5M CU/day to ~1.15M at a 12 s interval and ~230k at 60 s.

A restart reads no keys: the newest version row carries the per-module nonces. It costs one extra `eth_call` and one wide event scan. Exceptions:

- If the full-read window (`MAX_BLOCKS_OVERLAP`, counted from the last full read) is already due, the writer re-reads everything, and `latest`/`finalized` answer 425 until it publishes; pins by number or hash are still served.
- A crash during a cold sync is not resumed. A pod that keeps dying before its first version re-reads every key each time and can burn a daily quota.

`internal/ingest/rpc_budget_test.go` pins these. `TestQuietCycleRequestBudget` asserts exact counts per method. `TestADayOfPollingFitsTheProviderQuota` projects a day of measured cycles against the provider's weights and fails past the quota; it catches un-paced event scans (184 %), a missing head short-circuit (120 %) and metered aggregation off (209 %):

```
projected 1583740 CU/day for 2 writer(s) at a 5s interval, quota 2000000 (79% used)
  eth_call                   14400 calls      288000 CU
  eth_getBlockByNumber       17505 calls      420120 CU
  eth_getLogs                  450 calls       33750 CU
```

### Paging and diffs

| | |
|---|---|
| full walk, 52 pages of 10 000 | 511 877 keys, no gaps, no duplicates; median page 35 ms (18–42) |
| version stability over a 22 s walk | one version while the chain advanced 3 blocks |
| diff between two versions | 2–5 ms, 527 bytes, against 227 MB for a dump |
| diff correctness | old dump plus diff equals a fresh dump, `used` flips included |

### Binary storage

Pubkeys and signatures as `bytea`, a two-byte module id instead of the address:

| | hex | bytea |
|---|---|---|
| key table | 200 MB | 100 MB |
| dump, p50 sequential | 380 ms | 374 ms |
| dump, p50 at 4 concurrent | 1.11 s | 1.53 s |
| peak container memory | 27 MiB | 17 MiB |

Encoding is free sequentially and costs ~37 % when one CPU serves four dumps; paging and diffs remove that workload.

## Findings for the canonical service

- 512 MiB is not enough on mainnet. `/v1/modules/keys` builds the 216.7 MiB response in memory (this service streams it at an 11 MiB peak), and the cold sync restart-looped 12 times at 469 MiB until the limit was raised to 1.5 GiB.
- An OOM looks like a clean restart: PID 1 is `yarn`, so `docker inspect` shows `OOMKilled=false, exit=0` and only `Killed` in the log gives it away.
- Per-module `lastChangedBlockHash` moves on every re-read, so a restart, a stall past `MAX_BLOCKS_OVERLAP` or a reorg stamps all modules at once; on the live service all four carried the global value. Use `nonce` to skip work. Here the stamp follows content: a full re-read of every module published the same stamp.
- `STREAM_TIMEOUT` defaults to 60 s, and four concurrent mainnet dumps already take 27 s.
- Each key DTO recomputes an EIP-55 checksum, 500 000 keccak256 hashes per dump over three distinct addresses. `internal/ethaddr` memoizes per address.
- A quiet cycle makes ~25 requests where 6 would do: the reorg check re-fetches the same two blocks per module, nonce reads are sequential `await`s that never batch, and the locator is resolved every cycle as its own request.

## Removed code

Four more read backends lived here: the canonical schema read directly, an in-process snapshot and two embedded log-structured stores. An embedded store served a dump in 194 ms against 288 ms, but it allows only one process and no rolling deploy, and a version costs ~230–300 MB in the full-copy layout. Git history has the code; [docs/snapshot-modifiers.md](docs/snapshot-modifiers.md) has the reasoning.

## Layout

```
cmd/keys-api      the service: ingest plus API
cmd/diff          parity runner against the canonical service
cmd/bench         dump latency, throughput and server RSS
cmd/ingest        chain probe: modules, operators, key throughput, no database
internal/source   the seam: what a registry looks like, with no contract knowledge
  lidocore        Lido core: staking router, curated and community modules
  static          a JSON file, for tests and for running without a chain
internal/ingest   the sync loop, the version writer, the schema
internal/store    reading published versions: pins, paging, diffs
internal/api      routing and a hand-rolled streaming encoder
internal/eth      JSON-RPC client and the canonical service's ABI files
internal/metrics  Prometheus surface
docs/             operations, migration, architecture, fork tests, design note, next steps
```
