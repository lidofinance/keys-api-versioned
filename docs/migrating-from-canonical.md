# Moving consumers off the canonical service

This service replaces `lido-keys-api`. `/v1` and `/v2` answer as canonical 6.0.0 does, apart from the differences listed below, so for a consumer the move is a URL change; deploying the service itself is in [operations.md](operations.md). Moving back is the same change in reverse: a consumer keeps no state that depends on which service it read.

## Known consumers

Read from each consumer's `develop` branch, November 2026.

| Consumer | Reads | Requires | Setting |
|---|---|---|---|
| `lido-council-daemon` | `/v1/keys`, `POST /v1/keys/find`, `/v1/modules`, `/v1/operators`, `/v1/status` | `appVersion` ≥ 5.0.0 (`MIN_KAPI_VERSION`, semver) | `KEYS_API_URL`, or `KEYS_API_HOST` + `KEYS_API_PORT` |
| `lido-oracle` | `/v1/keys?used=true`, `/v1/modules/{id}/operators/keys?used=true`, `/v1/status` | `appVersion` ≥ 1.5.0 (PEP 440); `elBlockSnapshot.blockNumber` at or above the report block | `KEYS_API_URI`, comma separated |
| `ethereum-validators-monitoring` | `/v1/keys?used=true`, `/v1/operators`, `/v1/modules`, `/v1/status` | | `VALIDATOR_REGISTRY_KEYSAPI_SOURCE_URLS` |
| `ethereum-head-watcher` | `/v1/keys?used=true`, `/v1/operators`, `/v1/modules`, `/v1/status` | | `KEYS_API_URI` |
| `lido-mev-monitoring` | `/v1/status`, `/v1/modules`, `/v2/modules/operators/keys` | | `KEYS_API_URL` |
| validator ejector | the two exit routes under `/v1/modules/{id}/validators/…` | a validators registry | cannot move: those routes answer here as a canonical deployment with `VALIDATOR_REGISTRY_ENABLE=false` does, with a 500 |

`appVersion` in `/v1/status` is the canonical release this service tracks (6.0.0), so both version checks pass; the build's own version is on `build_info`.

The council daemon's `docker-compose.yml` runs the canonical image with its own Postgres 14 next to the daemon. A guardian moves by replacing that service and its variables, see the mapping in [operations.md](operations.md#from-a-canonical-deployment).

## What a consumer can see

| | Canonical | Here |
|---|---|---|
| Order of modules in a response | physical row order (3, 1, 2, 4, 5 on mainnet) | by id. Neither service promises an order of keys; in the last comparison it was the same within every module |
| Per-module `lastChangedBlockHash` | moves on every re-read | moves when the module's content changes |
| `clBlockSnapshot` in `/v1/status` | the CL snapshot | `null`: no validator ingest |
| Exit routes | validators from the CL registry | validation as canonical, then 500 `Validators Registry is disabled` |
| Throttling, CORS | `GLOBAL_THROTTLE_*`, `CORS_WHITELIST_REGEXP` | none: a browser origin or a rate limit has to come from the ingress |
| Extra response headers | | `X-Ignored-Parameters` when a request sends `snapshot`, `limit` or `cursor` to `/v1`; `X-Snapshot-Stale: true` while a rebuild replaces a version known to be behind |
| Error status codes | | identical across the 21 cases `cmd/diff -errors` checks |

Everything this service adds — pinned versions, paging, diffs, operator groups — is under `/ext/v1` and only there; the README's "Two surfaces" section describes it.

## Parity

Last compared on 2026-11-27: `main` against keys-api.lido.fi (6.0.0) on mainnet. `cmd/diff -live` leaves out `meta` and the per-module `lastChangedBlockHash`.

- `/v1/keys` (all, `used=true`, `operatorIndex=1`), `/v1/modules`, `/v1/modules/1`, `/v1/modules/1/keys`, `/v1/modules/keys`, `/v1/operators`, `/v1/modules/1/operators`: equal.
- `/v1/status`: differs only in `clBlockSnapshot`.
- `/v2/modules/operators/keys`: 526,397 records on each side at the same block, equal within every module and in the same order; only the order of modules differs.
- `cmd/diff -errors`: status codes equal in 21 of 21 cases.
- `POST …/keys/find`: the same body on both routes for 58 malformed or unusual bodies and content types; `internal/api/findbody_test.go` pins them.

Run the same comparison against each new canonical release before moving `ContractVersion`.
