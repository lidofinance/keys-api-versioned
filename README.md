# keys-api-versioned

A Keys API written in Go that keeps the registry as **published versions** in its own Postgres schema, plus a stand that runs it next to the canonical `lido-keys-api` on the same chain so the two can be compared.

It is meant to be droppable under `ethereum-validators-monitoring` and `lido-mev-monitoring`: the endpoints they call are implemented, and responses are byte-identical to the canonical service, framing whitespace included. Pointing one of them here is a URL change.

How it is put together — the source seam, the schema, one cycle, paging and diffs — is in [docs/architecture.md](docs/architecture.md). Where the work stands and what comes next is in [docs/next-steps.md](docs/next-steps.md).

## Running it

Against a chain: both services, one Postgres, two databases.

```bash
PROVIDERS_URLS=http://node-1,http://node-2 docker compose up -d --build
docker compose run --rm tools diff -a http://canonical:3000 -b http://keys-api:3000 -live
docker compose run --rm tools bench -url http://keys-api:3000/v1/keys
```

With no chain at all — up in a second, for a demo or for pointing a consumer at something:

```bash
docker compose --profile static up -d --build keys-api-static
```

Ours listens on 3010, the canonical service on 3000. Environment variables are documented at the top of [cmd/keys-api/main.go](cmd/keys-api/main.go); the names the canonical service already uses are reused. `KEYS_API_MODE=server` serves without syncing and `KEYS_API_MODE=ingest` syncs without serving, so several read replicas can sit behind one writer.

Generate load from inside the compose network. Through a published port on Docker Desktop the host hop caps everything at ~188 MiB/s and every service looks identical.

## The surface

| Endpoint | Notes |
|---|---|
| `GET /v1/keys` | full dump, streamed; `?limit`/`?cursor` pages it |
| `GET /v1/keys/{pubkey}`, `POST /v1/keys/find` | pubkey lookups |
| `GET /v1/keys/diff?since=` | what changed between two versions |
| `GET /v1/modules`, `GET /v1/modules/{id}` | module list and one module |
| `GET /v1/modules/diff?since=` | which modules changed — the cheap poll |
| `GET /v1/modules/keys` | grouped by module |
| `GET /v1/modules/{id}/keys`, `POST …/keys/find` | per-module dump and lookup |
| `GET /v1/operators` | grouped by module; what EVM reads for operator names |
| `GET /v1/modules/{id}/operators`, `…/operators/{operator_id}` | one module's operators, one operator |
| `GET /v1/modules/{id}/operators/keys` | module, operators and keys in one stream |
| `GET /v2/modules/operators/keys` | everything in one stream; what mev-monitoring reads |
| `GET /v1/status` | `clBlockSnapshot` is null: no validator ingest |
| `GET /docs`, `GET /openapi.yaml` | the API document, and a page that renders it |
| `/health`, `/health/ready`, `/metrics` | liveness, readiness (503 until something is published), Prometheus |

Two additions any consumer can ignore: `?snapshot=<latest|safe|block|hash>` reads a specific version — the default is `latest`, which is what a consumer already expects — and `X-Snapshot-Stale: true` says the served version is known not to reflect the chain while a rebuild is in flight. Bodies are unchanged either way.

The document at `/openapi.yaml` is hand-written and embedded rather than generated: half of what a consumer needs to know is contract rather than struct shape — that a request without `snapshot` gets `latest`, that a cursor pins its version, that a diff's two sides reconstruct a dump when applied in order, that `409` carries the head to jump to. A test holds it to the router in both directions, so a route added without a spec entry fails the build.

Not implemented: the validator exit endpoints (they need consensus layer ingest), throttling, CORS, Sentry.

## Parity

`cmd/diff` fetches the same endpoint from both services, sorts key arrays (neither implementation promises an order) and compares. `-live` tolerates the two being at different blocks, which they always are when both follow a chain.

On the mainnet registry, 15 endpoints checked, several byte-identical including the full 216.7 MiB dump, `?used=true`, `/v1/modules` and `/v1/modules/{id}`. Byte-identical covers the framing whitespace and the JSON property order of the canonical DTOs. Where the diff reports "equal ignoring key order", the row order out of Postgres differed between the two queries — neither service sorts, so consumers must not depend on order.

One trailing byte used to separate us from byte-identical on the non-streamed endpoints: `json.Encoder.Encode` appends a newline and Fastify does not.

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

| | |
|---|---|
| Empty database to published version | **107 s** on the host, **~4 min 15 s** in a 1-CPU container; batch 40 / concurrency 6 |
| module 1 — 471 732 keys, 39 operators | 47 s |
| module 2 — 14 205 keys, 82 operators | 8 s |
| module 3 (CSM) — 25 940 keys, 609 operators | 51 s |
| module 4 (CMv2) — 0 keys, 1 operator | 0.5 s |
| On disk | key table 105 MB, key indexes 97 MB, database 211 MB |
| Quiet cycle, steady state | 0.81 s mean — a header, module nonces, one log filter per module, one row update |

Round-trip bound, not key bound: CSM's 26 000 keys cost more wall clock than module 1's 472 000, because CSM has 609 operators and module 1 has 39. Opening the provider knobs is the first lever, not code.

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

- **512 MiB is not enough for it on mainnet, in two separate places.** `/v1/modules/keys` is not streamed there — it builds the whole 216.7 MiB response in memory (ours serves that endpoint with a 28 MiB peak). And its *cold sync* does not fit either: in this stand it restart-looped 12 times at 469 MiB before the limit was raised to 1.5 GiB. Ours syncs the same registry inside 512 MiB.
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
