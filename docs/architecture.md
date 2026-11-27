# How the service is put together

Two halves that know almost nothing about each other.

```
                     ┌─────────────────────────────┐
  contracts,         │  source.Provider            │   the only part that
  files, whatever ──►│    lidocore  (staking router)│   knows where keys
                     │    static    (a JSON file)  │   come from
                     └──────────────┬──────────────┘
                                    │  modules, operators, keys
                     ┌──────────────▼──────────────┐
                     │  ingest.Syncer              │   freshness, scoping,
                     │  ingest.Store  (Postgres)   │   reorgs, retention
                     └──────────────┬──────────────┘
                                    │  published versions
                     ┌──────────────▼──────────────┐
                     │  store.App → api.Server     │   the keys-api surface
                     └─────────────────────────────┘
```

The upper seam is `internal/source`, the lower one `internal/ingest.Store`. Versions, staleness, reorg rollback, retention and the response contract sit between them and exist once.

## Adding a source

A source answers four questions: the current block, the modules with their change counters, a module's operators, a module's keys. `internal/source/source.go` lists the guarantees it must hold. The one that matters most: a module's nonce moves when its keys move and not otherwise, because freshness is one nonce read per cycle.

- No chain is required. `source/static` reads a JSON file and derives the block identity from the file's modification time and content hash.
- Inside `source/lidocore`, each module family is a `ModuleReader` registered by the `getType` string. `curated-onchain-v1` is the curated registry; `community-onchain-v1` and `curated-onchain-v2` share the CSM-shaped reader, as in the canonical service. A new family is one file.
- A registry whose unit is not a staking module, such as stVaults, maps its unit onto modules → operators → keys. Consumers (`ethereum-validators-monitoring`, the head watcher, `lido-mev-monitoring`) then read either service without knowing the difference.

## The schema

Four tables, created on boot from `internal/ingest/schema.sql`.

`module_version`, `operator_version` and `key_version` keep each value as a block range `[from_block, to_block)`. Reading a version is one predicate:

```sql
where from_block <= :block and (to_block is null or to_block > :block)
```

A new version writes only what moved and inherits the rest. A version stamp per row would mean a full copy, ~100 MB per version on mainnet. On the static fixture, a cold build wrote 33 key rows for 33 keys, and the next version, one key added to one module, read 19 keys and wrote 1 row.

`versions` holds what the data tables cannot say:

- status: `building` versions are invisible until one row update publishes them, so a version can be filled across many transactions. Inferring the head from the newest open row tore 2 dumps of 6 at a module boundary.
- which blocks are versions: `latest`, `safe`, retention and the 409/410 answers need the list.
- the block the data was read at (the identity, never moves) and the block last confirmed current (moves on every quiet cycle).
- `lastChangedBlockHash` and the per-module nonces the next cycle compares against the chain.
- block hashes for the reorg walk, and which source wrote the version.

Pubkeys and signatures are `bytea`, and a key row carries a two-byte module id instead of an address: half the key table (200 MB → 100 MB), one spelling per value. Small tables keep text.

## One cycle

| Case | What is read | What is written |
|---|---|---|
| head unchanged | the head | nothing |
| nothing changed | head, module set with nonces; the finalized block and operator events once per epoch | the version's confirmed block |
| a nonce moved | that module's operators, and its keys from each operator's deposited count at the finalized block on | the rows that differ |
| operator event | operators of every module | operator rows |
| router field, router replaced, contract version changed | nothing beyond the module set | module rows |
| full audit, every `MAX_BLOCKS_OVERLAP` blocks | everything | nothing, if the cheap signals were right |
| reorg | headers back to the newest stored version still on chain | rollback above it, then an ordinary cycle |

Quiet cycle on mainnet: 0.70 s mean. Cold sync numbers are in the [README](../README.md#measurements).

- Deposited keys cannot change, so a deposit costs the keys after it, not the whole module. Mainnet module 1 holds ~90 % of keys, so a change elsewhere costs about a tenth of a full read.
- Operator metadata moves no nonce and is caught by one log filter per module family: `NodeOperatorAdded`, `NodeOperatorNameSet`, `NodeOperatorRewardAddressSet` for the curated registry; `NodeOperatorRewardAddressChanged`, `VettedSigningKeysCountChanged` for CSM-shaped modules, plus `OperatorMetadataSet` at CMv2's `META_REGISTRY` in the same filter. The range starts where every module's operators were last known current.
- `VettedSigningKeysCountChanged` can lower a vetted count without a nonce move. The rebuild reads operators and re-derives `vetted` on the kept keys from the new staking limits.
- Router fields (status, fees, share limits, withdrawal credentials type) arrive with the module set every cycle and are compared with the published module rows. A replaced router or a new contract version is kept as a reason until a rebuild publishes.
- A rebuild's depth comes from its reason, and the default is the deepest read, so a missing rule reads too much rather than too little.
- Known gap: CMv2 node operator groups change by an event that is not watched. A group edit shows up with the module's next key change or the next audit, within hours.
- The finalized block and the event scan are paced by `SECONDS_PER_SLOT × SLOTS_PER_EPOCH`; a fresh process reads both on its first cycle, and a zero constant turns pacing off.
- The audit window defaults to 1 000 blocks (~3 hours), not canonical's 30: here the signal is a contract counter, and 30 would re-read half a million keys every six minutes. It counts from `audited_block`, so frequent restarts cannot postpone it. A writer that starts with the audit due serves 425 on `latest` and `finalized` until it publishes.
- Reorg: the walk anchors only on versions that were served. A reorg below the last 20 served versions forces a full rebuild and is logged. Probing stops once the version is found on chain at or below the finalized block, so a version orphaned while the writer was down is caught on its first cycle back.
- A restart over a populated database reads no keys: it compares stored nonces with the chain. A crash during a cold sync is not resumed.

## Paging and diffs

Both rely on rows that never change once a version is published.

Paging: `?limit=N` returns a page and a cursor. The cursor names the version by block hash, so the walk stays on one version, and a reorg that replaced it at the same height answers 409. `meta.elBlockSnapshot` reports the confirmed block, so the head in `meta` advances during a walk while the data version does not; when both were one number, a quiet cycle renumbered the version and broke cursors with 410.

Diffs: `GET /ext/v1/keys/diff?since=<version>[&to=…][&module=…]`

```json
{ "meta": { "from": {…}, "to": {…}, "removed": 1, "added": 2 },
  "data": { "removed": [ … ], "added": [ … ] } }
```

- `added`: rows that became visible in the interval and are still visible at its end. `removed`: rows visible at its start and not at its end. A changed key appears on both sides.
- Applying removals, then additions, by key identity gives a fresh dump; tests check it on real responses, including a deposit flipping `used`.
- `GET /ext/v1/modules/diff?since=` names the modules that moved, about a kilobyte. Unlike canonical's per-module `lastChangedBlockHash`, it follows content.
- A `since` outside the retention window answers 410: its closed rows are gone and the diff would miss removals.

## Consumers

What each known consumer reads and checks is in [migrating-from-canonical.md](migrating-from-canonical.md). Repointing is a URL change:

```
# ethereum-validators-monitoring: /v1/keys?used=true, /v1/operators, /v1/modules, /v1/status
VALIDATOR_REGISTRY_SOURCE=keysapi
VALIDATOR_REGISTRY_KEYSAPI_SOURCE_URLS=["http://keys-api:3000"]

# lido-mev-monitoring: /v1/status, /v1/modules, /v2/modules/operators/keys
KEYS_API_URL=http://keys-api:3000
```

The extensions under `/ext/v1` are described in the README's "Two surfaces" section.

## Metrics

Alerts and readiness are in [operations.md](operations.md#what-to-watch). Beyond those:

| Metric | Meaning |
|---|---|
| `keys_api_rpc_requests_total{method,result}` | HTTP round trips to the execution endpoints, failover retries included; what a hosted provider bills |
| `keys_api_rpc_calls_total` | contract reads those requests carried; the ratio is what packing saves |
| `keys_api_rpc_splits_total` | the Multicall3 budget is above what the endpoint accepts |
| `keys_api_published_version_age_seconds` | how old the data is; climbs on a quiet registry, not for alerting |

The provider error classification behind failover is in `internal/eth/rpcerror.go` and in the README.

Canonical metric names are exported as well, with two differences:

- `lido_keys_api_keys_by_sr_module_and_operator` is refreshed for the modules a rebuild read, since a quiet cycle reads no operators.
- `validators_registry_*` is absent: there is no CL-side registry.

## Logs

Every line carries `chain_id` and `schema`, so a node on the wrong chain is visible from any line.

At info level a quiet cycle prints nothing; rebuilds, published versions, retention drops and failures are info or above. `LOG_LEVEL=debug` adds:

- one line per JSON-RPC request: method, batch size, endpoint, duration;
- a heartbeat every 5 s during a module read, with the key rate;
- one line per operator as its keys land;
- the freshness decision with its inputs: nonces, block gap, event scan per module;
- the resolved version per request, rows and duration per module query.

A quiet cycle at debug:

```
rpc method=eth_getBlockByNumber calls=1 endpoint=… took=102ms
cycle: head block=25609790 hash=0x8a82… timestamp=1784982599
rpc method=eth_call calls=8 endpoint=… took=140ms            # getType and getNonce for four modules
module read id=1 type=curated-onchain-v1 nonce=18680 …
operator event scan module=1 from=25609758 to=25609790 changed=false
cycle: decision action=extend reason="no change up to block 25609790" behind=33 …
cycle: extended version=25609757 confirmed_at=25609790 last_changed=0xdcb4…
retention: nothing to drop keeping=20 oldest_kept_block=25609757
```
