# Running it in production

For whoever deploys this service: Lido's own clusters, oracle operators and council daemon guardians. Why each mechanism exists is in the [README](../README.md) and [architecture.md](architecture.md); this page is what to set and what to watch.

## Requirements

| | |
|---|---|
| Postgres | CI runs 17, the compose stand 16. Older majors, such as the 14 in the council daemon's compose file, are not tested. One database; the service creates schema `chain_<id>` in it. |
| Connection | Direct to Postgres. The writer lock and the reader leases are session-level advisory locks, which a transaction-mode pooler (PgBouncer) breaks without an error. |
| Execution endpoints | Any number, comma separated, tried in order. An archive node is not needed: the service reads at the head and at finalized. |
| Consensus endpoint | None. Epoch timing is computed from `SECONDS_PER_SLOT` × `SLOTS_PER_EPOCH`. |
| CPU, memory | 1 CPU and 512 MiB serve the mainnet registry with room to spare; a cold sync used 10 CPU-seconds, and serving holds 24–37 MiB. Numbers in the README. |
| Image | `Dockerfile`: distroless, runs as `65532:65532`, listens on `PORT` (3000). |

## Topology

- **One process** (`KEYS_API_MODE=all`, the default): syncs and serves. Reader and writer share one connection pool.
- **Split** (`KEYS_API_MODE=ingest` once, `KEYS_API_MODE=server` as many times as needed), all on the same database. Readers never write.

More than one writer on a schema is safe: one holds the writer lock, the others wait. A writer that lost its lock session exits rather than queue. A rolling update therefore works in either topology.

## Configuration

Every variable the binary reads. The secrets file, when present, wins over the environment.

| Variable | Default | |
|---|---|---|
| `CHAIN_ID` | `1` | checked against every endpoint; names the schema |
| `LIDO_LOCATOR` | required | everything else is resolved from it; mainnet and Hoodi values in `.env.example` |
| `PROVIDERS_URLS` | required | execution endpoints, comma separated, in failover order |
| `DATABASE_URL` | required | Postgres DSN. A `search_path` in it overrides the per-chain schema |
| `DATABASE_MAX_CONNS` | `10` | |
| `KEYS_API_MODE` | `all` | `all`, `ingest` or `server` |
| `PORT` | `3000` | API, `/health*` and `/metrics` share it |
| `LOG_LEVEL` | `info` | `debug` explains every cycle; see architecture.md |
| `KEYS_API_ENV` | `unknown` | `env` label on `build_info` |
| `KEYS_API_PIN` | `latest` | version `/v1` and `/v2` serve. Leave it: canonical serves the head |
| `KEYS_API_EXT_PIN` | `finalized` | version `/ext/v1` serves when a request does not pin one |
| `KEYS_API_BUILD_AT_FINALIZED` | `false` | build versions at finalized: no 409s, data about two epochs behind |
| `KEYS_API_SOURCE` | `lido-core` | `static` reads `KEYS_API_STATIC_FILE` instead of a chain |
| `UPDATE_KEYS_INTERVAL_MS` | `5000` | cycle interval |
| `MAX_BLOCKS_OVERLAP` | `1000` | blocks between full re-reads (audits) |
| `RETAIN_VERSIONS` | `20` | published versions kept; a version a reader still streams is kept too |
| `STREAM_MAX_CONCURRENT` | half the pool | concurrent dumps and diffs; the rest get 503 after 10 s. Must stay below `DATABASE_MAX_CONNS` |
| `STREAM_MAX_DURATION_MS` | `600000` | one dump or diff is cut after this |
| `PROVIDER_TIMEOUT_MS` | `120000` | one attempt against one endpoint |
| `PROVIDER_CONCURRENT_REQUESTS` | `8` | |
| `PROVIDER_JSON_RPC_MAX_BATCH_SIZE` | `100` | lower it for a provider plan that refuses large batches |
| `PROVIDER_METERED` | `true` | `false` against a node you run: aggregation costs it a little, saves a hosted plan about 5× |
| `PROVIDER_MULTICALL` | `true` | |
| `PROVIDER_MULTICALL_ADDRESS` | `0xca11…ca11` | |
| `PROVIDER_MULTICALL_MAX_CALLS`, `PROVIDER_MULTICALL_MAX_KEYS` | `200`, `1000` | aggregate budget; a refusal halves it |
| `KEYS_BATCH_SIZE`, `KEYS_CALLS_PER_REQUEST`, `KEYS_PER_REQUEST`, `KEYS_READ_CONCURRENCY` | `1100`, `200`, `2200`, `PROVIDER_CONCURRENT_REQUESTS` | key read packing |
| `SECONDS_PER_SLOT`, `SLOTS_PER_EPOCH` | `12`, `32` | epoch pacing of the operator-event scan; `0` turns pacing off |
| `SECRETS_FILE_PATH` | `/vault/secrets/config` | JSON object of variables. Re-read every `SECRETS_POLL_INTERVAL_IN_SECONDS` (`10`): new endpoints apply live, a new DSN restarts the process. Mode `0400` or `0440`; the service warns on anything wider |

### From a canonical deployment

| Canonical | Here |
|---|---|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | one `DATABASE_URL` |
| — | `LIDO_LOCATOR`, now required: no addresses are compiled in |
| `PROVIDERS_URLS`, `CHAIN_ID`, `PORT`, `LOG_LEVEL`, `PROVIDER_JSON_RPC_MAX_BATCH_SIZE`, `PROVIDER_CONCURRENT_REQUESTS`, `SECRETS_FILE_PATH`, `SECRETS_POLL_INTERVAL_IN_SECONDS` | same names |
| `STREAM_TIMEOUT` | `STREAM_MAX_DURATION_MS`; a stalled reader is also cut after 30 s without progress |
| `CL_API_URLS`, `VALIDATOR_REGISTRY_ENABLE` | not read: no validator ingest, the exit routes answer as with `VALIDATOR_REGISTRY_ENABLE=false` |
| `GLOBAL_THROTTLE_TTL`, `GLOBAL_THROTTLE_LIMIT`, `CORS_WHITELIST_REGEXP`, `SENTRY_DSN` | not implemented |
| `GLOBAL_CACHE_TTL`, `PROVIDER_BATCH_AGGREGATION_WAIT_MS` | no counterpart |
| `LOG_FORMAT` | not read: logs are logfmt (`key=value`), not JSON. A pipeline that parses canonical JSON logs needs a logfmt parser |

Variables this service does not know are ignored, so a canonical `.env` starts it once `DATABASE_URL` and `LIDO_LOCATOR` are added. A malformed number or boolean in a known variable stops the start, naming the variable.

## Upgrades and rollbacks

- The writer applies the schema. A release with a newer schema number rebuilds the store and logs it at WARN; readers answer 425 until the first version on it is published (about 30 s on mainnet against a hosted provider).
- An older writer that finds a newer store changes nothing and exits, naming both numbers. To roll back across a schema change, drop the schema or point the older release at another one.
- A deployment pointed at another chain syncs into that chain's schema and leaves the previous one untouched; point it back and the rows are there. A `CHAIN_ID` that disagrees with the endpoints is refused before anything is read. A store another source wrote is never touched: the process refuses to start on it.

## What to watch

| Signal | Meaning |
|---|---|
| `keys_api_confirmed_age_seconds` | the one to alert on: time since a cycle last confirmed the version against the chain. Covers a stalled node, failing rebuilds and a dead writer |
| `/health/ready` | 503 until a version is published; the body says why |
| `keys_api_rpc_endpoint_requests_total{endpoint,result}` | success on an endpoint that is not first in the list means the first one is failing |
| `keys_api_audit_divergence_total` | an audit found a change no cheap signal reported; should stay 0 |
| `keys_api_staking_router_changes_total`, `keys_api_staking_router_contract_version` | a router replacement or upgrade was followed; expect them only with a protocol deploy |
| log `retention held back by a reader still streaming an older version` | a consumer holds a version open |

Canonical metric names (`lido_keys_api_*`, `build_info`, `http_requests_duration_seconds`, `el_rpc_requests_duration_seconds`) are exported too, so canonical dashboards work unchanged. Do not alert on `keys_api_published_version_age_seconds`: it climbs whenever the registry is quiet.

`/metrics` is on the API port: whoever can reach the API can read it.
