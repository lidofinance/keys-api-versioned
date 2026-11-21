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

The upper seam is `internal/source`. The lower one is `internal/ingest.Store`. Everything between them — versions, staleness, reorg rollback, retention, the response contract — exists once.

## Adding a source

A source answers four questions: what block are we at, which modules exist and what are their change counters, who are a module's operators, and what are its keys. `internal/source/source.go` states the four guarantees it has to hold up; the important one is that a module's nonce moves when that module's keys move and not otherwise, because the whole freshness mechanism is one nonce read per cycle.

Nothing requires a chain. `source/static` reads a JSON file and derives a block identity from the file's modification time and content hash, which is enough for versions, pins and change detection to work. A source pulling from a spreadsheet or an internal document would look the same: fetch, map to modules/operators/keys, let the writer do the rest.

For Lido core specifically there is a second, smaller seam inside `source/lidocore`: a `ModuleReader` per module family, registered by the type string `getType` returns. `curated-onchain-v1` is the curated registry; `community-onchain-v1` and `curated-onchain-v2` share the CSM-shaped interface, which is the same split the canonical service makes. A new module family is one file.

A registry whose unit is not a staking module — stVaults, where keys hang off vaults — maps its own unit onto the module shape. That is deliberate: consumers read one abstraction (modules → operators → keys) whatever produced it, which is what lets `ethereum-validators-monitoring`, the head watcher and `lido-mev-monitoring` point at either service without knowing the difference.

## The schema

Four tables, created on boot from `internal/ingest/schema.sql`.

`versions` is the version registry: the block its data was read at (its identity, which never moves), the block it was last confirmed current at (which does), `lastChangedBlockHash`, label, **status**, the per-module nonces observed, and which source wrote it.

`module_version`, `operator_version` and `key_version` hold the data as block ranges — one row per value, alive for `[from_block, to_block)`. Reading a version is one predicate:

```sql
where from_block <= :block and (to_block is null or to_block > :block)
```

Why the range and not one version stamp per row: a stamp means a full copy per version, about 100 MB of table and index for mainnet's 500 000 keys, written in full even when one key changed. With ranges, a version writes what moved and inherits the rest by leaving rows open. Measured on the static fixture and visible in the logs: a cold build wrote 33 key rows for 33 keys, and the next version — one key added to one module — read 19 keys and wrote 1 row.

Why `versions` is still a table when the rows are keyed by block: the range says *when* a row was alive, not *whether that block is a version anyone may read*. That is what the version row carries, and none of it can be derived from the data tables:

- **status.** `building` is the whole trick. A version can be filled across many transactions, over minutes, and no reader sees any of it until one row update makes it current. Inferring the head from the data instead — the newest open row — is exactly the torn read this repo measured: a publish landing between two per-module queries stitched a response out of two versions, 2 dumps of 6, tearing at a module boundary.
- **which blocks are versions.** `latest`, `safe`, retention, and the 409/410 answers all need the list. A key row cannot say that block 21 000 001 was published and 21 000 002 was not.
- **the metadata consumers read.** `lastChangedBlockHash` is the refresh signal; `module_nonces` is what the next cycle compares against the chain without reading a key.
- **reorg bookkeeping.** The fork walk compares each stored version's hash against the chain's hash at that height. Without stored hashes there is nothing to compare.

Pubkeys and signatures are `bytea`, and a key row carries a two-byte module id rather than a 42-character address. That halves the key table (200 MB → 100 MB on mainnet), removes the two-spellings problem, and costs nothing on read until the CPU is saturated — the hex goes straight into the response buffer. Small tables keep readable text.

## What one cycle costs

Nothing changed: a block header, the module set with nonces, one log filter per module. One row update. Measured on mainnet: 0.81 s mean per cycle.

A module's nonce moved: the same reads, plus that module's keys and operators, plus the rows that actually differ. Other modules are untouched. On mainnet one module holds about 90 % of the keys, so a change confined to the others is worth roughly a tenth of the work. Within the module, each operator is read from its deposited count at the finalized block on: a deposited key cannot change, and the store already holds those rows, so a deposit costs the keys that follow it rather than the whole module. The audit is the exception and reads everything, since it exists to check the rows that shortcut trusts.

A reorg: the stored version's hash no longer matches the chain at its height. The loop walks its own version table to find the newest version whose block still matches, rolls everything above it back, and then runs the ordinary cycle — so a reorg with no key changes in the reorged range costs no key reads. A reorg deeper than the retained window is the one case that forces a full rebuild, and it says so in the log. Finality ends the probing only once a probe has found the version on chain at or below the finalized block: a version whose block was orphaned while the writer was down is caught on the first cycle back, at the cost of one header read per version that lives until it is final. The walk anchors only on versions that were served, never on one an earlier rollback superseded.

Operator metadata (a rename, a reward address) moves no nonce, so it is caught by a log filter instead — one per module family, over the blocks since every module's operators were last known current (an empty scan, or a rebuild that read them all) rather than from the published version's block, which a rebuild scoped to one module's keys moves without reading anyone's operators: `NodeOperatorAdded`, `NodeOperatorNameSet`, `NodeOperatorRewardAddressSet` for the curated registry; `NodeOperatorRewardAddressChanged` and `VettedSigningKeysCountChanged` for the CSM-shaped modules, plus `OperatorMetadataSet` at CMv2's `META_REGISTRY`, which rides in the same filter. Same events the canonical service watches.

`VettedSigningKeysCountChanged` can lower a CSM or CMv2 operator's vetted count without moving the nonce, and the rebuild it triggers reads operators, not keys. Both readers set a key's `vetted` as its index below the operator's staking limit, so the store re-derives that flag for the module's kept keys from the operator rows just written: the same correction a key read would make, without the key read.

The router's own fields — status, fees, share limits, withdrawal credentials type — move no nonce either: the DAO sets them on the router. The module read every cycle already carries them, so they are compared with the published module rows (read back from the store once after a restart), and a difference rewrites the module rows only. A replaced router or a changed contract version is kept as a reason until a rebuild publishes, so a failed rebuild does not lose it.

A rebuild reads only as deep as the reason requires, and the reason is what says how deep that is: a nonce that moved means keys and operators for that module; an operator event means operators everywhere, because names live off the nonce and which module changed is unknown; a replaced staking router, a changed contract version or a changed router field means neither — only the module rows, since keys live in the module contracts and the router supplies the tuple. The deepest read is the zero value of that decision, so a rule that forgets to say reads too much rather than too little.

One CMv2 field is outside that net: the node operator group lives in the module's `META_REGISTRY` and its event is not watched (the canonical service has no groups), so a group edit is picked up by the next change to that module's keys or by the periodic full audit — hours, not a cycle. The scan watches the registry address the last operator read used, resolved once at `toBlock` after a restart. It is a freshness gap, not a correctness one: what is served is always some consistent version of the chain.

Not every read happens every cycle, and the rule is that a read's cadence should match how fast its subject moves. The head moves every slot, so it is read every cycle; the finalized block advances once per epoch and an operator rename happens a few times a year, so both are paced by the epoch — `EpochDuration`, computed as slot time × slots per epoch rather than fetched, because the loop wants a duration and adding a consensus client to obtain two constants would cost more than it saves. A process that has just started does both on its first cycle regardless: it knows nothing yet. Set either constant to zero and the pacing is off, which is the behaviour that existed before it.

A full audit: every `MAX_BLOCKS_OVERLAP` blocks (default 1 000, about three hours) the loop re-reads everything regardless of the cheap signals, in case one of them lied. A healthy audit writes nothing, which is the point — it is a checkable statement that nonce and events were telling the truth. Note this differs from the canonical service's 30: there the window bounds an event scan, here the signal is a contract counter, and at 30 the audit fires every six minutes and re-reads half a million keys to write nothing. That was measured, not assumed.

A restart, which is the case that looks like a cold sync and is not: `Latest` answers from the `versions` table, so a process that comes back up over a populated database compares the stored per-module nonces against the chain and reads no keys. It pays one extra `eth_call` to resolve the router cold and one wide event scan, both once. A crash *during* a cold sync is the exception — an unfinished version stays `building` and is never resumed, so the next start reads everything again.

The cold sync, for scale: 511 877 keys and 731 operators across four modules, empty database to published version, in **107 s** at batch 40 / concurrency 6. It is round-trip bound rather than key bound — CSM's 26 000 keys cost 51 s against module 1's 472 000 in 47 s, because CSM has 609 operators and module 1 has 39.

## Paging and diffs

Both exist because a version's rows do not move; both are why the canonical service cannot offer them.

**Paging.** `?limit=N` returns a page and a cursor; `?cursor=…` continues. The cursor names the version by its block hash, so every later page is served from the version page one came from, and a walk whose version a reorg replaced at the same height answers 409 instead of continuing on the other branch — a walk is consistent without the consumer knowing that pinning exists. Measured on mainnet: 511 877 keys in 52 pages of 10 000, median page 35 ms, no gaps and no duplicates, over a 22-second walk during which the chain advanced three blocks and the head reported in `meta` moved with it while the pinned version did not.

The version's identity is the block its data was read at, and it never moves. The block the version was last confirmed current at is separate, and that is what `meta.elBlockSnapshot` reports — the same split the canonical service has between its advancing meta and its data. Without that split a quiet cycle would renumber the version and a cursor would 410 mid-walk, which is exactly what an earlier iteration of this did.

**Diffs.** `GET /ext/v1/keys/diff?since=<version>[&to=…][&module=…]` returns what changed:

```json
{ "meta": { "from": {…}, "to": {…}, "removed": 1, "added": 2 },
  "data": { "removed": [ … ], "added": [ … ] } }
```

Over block ranges this is two predicates rather than a comparison of two dumps: `added` is rows that became visible in the interval and are still visible at the far end, `removed` is rows that were visible at the near end and are not at the far end. A key whose value changed appears on both sides, because that is what the storage did — the old row was closed and a new one opened. Apply removals then additions by key identity and the result equals a fresh dump; that invariant is checked against real responses, including the case that matters most, a deposit flipping `used` on an existing key.

`GET /ext/v1/modules/diff?since=…` is the cheap poll to do first: a kilobyte naming the modules that moved. It is trustworthy here in a way the canonical per-module `lastChangedBlockHash` is not, because the writer moves that stamp from content rather than from the act of re-reading.

`since` must still be inside the retention window. If it is not, the answer is 410 rather than a diff computed against a version whose closed rows have been deleted — that diff would silently miss removals.

## Pointing consumers at it

`ethereum-validators-monitoring` reads `/v1/keys?used=true`, `/v1/operators`, `/v1/modules` and `/v1/status`:

```
VALIDATOR_REGISTRY_SOURCE=keysapi
VALIDATOR_REGISTRY_KEYSAPI_SOURCE_URLS=["http://keys-api:3000"]
```

`lido-mev-monitoring` reads `/v1/status`, `/v1/modules` and the combined stream `/v2/modules/operators/keys`:

```
KEYS_API_URL=http://keys-api:3000
```

Response framing is byte-identical to the canonical service, whitespace and stream separators included, so this is a URL change and nothing else. Two additions a consumer can ignore or use:

- `?snapshot=<latest|finalized|block|hash>` serves one request from a specific version. Several requests against one version get consistent answers, and a version that has gone away answers 409 (reorged, with the head to jump to) or 410 (pruned) instead of quietly different data. On `/v1` and `/v2` the parameter is ignored and every request gets `latest` — the newest published version, which is what the canonical service serves and therefore what a consumer already expects. On `/ext/v1` the default is `finalized` instead (`KEYS_API_EXT_PIN`): a version a reorg cannot take away, roughly two epochs behind, with the head one `?snapshot=latest` away.
- `?limit` and `?cursor` page a dump; `/ext/v1/keys/diff?since=` and `/ext/v1/modules/diff?since=` say what changed. All of it lives under `/ext/v1` — see below.
- `X-Snapshot-Stale: true` says the version being served is known not to reflect the chain while a rebuild is in flight. The body is unchanged, so a consumer that ignores the header behaves exactly as before.
- `groupId` on an operator, on `/ext/v1` only, is the node operator group read from the module's `META_REGISTRY`. It answers "which operators belong to the same company" for dashboards that want to select by group — the case `/v1` cannot serve, since the canonical operator object has no such field. Absent where the module has no group registry, `null` where it has one and the operator is in no group; see the `Operator` schema in the API document for why those are not the same answer.

## Running it

```bash
PROVIDERS_URLS=http://node-1,http://node-2 docker compose up -d --build
```

With no chain at all, from a registry file — up in a second, and it exercises the same ingest, versions and endpoints:

```bash
docker compose --profile static up -d --build keys-api-static
```

Environment variables are listed at the top of `cmd/keys-api/main.go`; the names that exist in the canonical service are reused. `KEYS_API_MODE=server` serves without syncing and `KEYS_API_MODE=ingest` syncs without serving, so several read replicas can sit behind one writer.

`/metrics` carries the sync and request series. The one to alert on is `keys_api_confirmed_age_seconds` — how long since a cycle last confirmed the version against the chain — which rises whether the cause is a stalled node, a failing rebuild or a dead writer. Do not alert on `keys_api_published_version_age_seconds`: that is how old the *data* is, and it legitimately climbs whenever the registry is quiet.

The RPC surface is metered too: `keys_api_rpc_requests_total{method,result}` counts HTTP round trips to the execution endpoints, including the ones a fallback endpoint hid, and `keys_api_rpc_calls_total` counts the contract reads they carried. The ratio is what the packing is worth, and the first is what a hosted provider bills — the reason it exists is that "how many requests does a deployment spend" used to be answerable only by reading the source. `keys_api_rpc_splits_total` climbing means the aggregate budget is above what this endpoint tolerates.

`keys_api_rpc_endpoint_requests_total{endpoint,result}` is the one that says which provider is actually serving. `PROVIDERS_URLS` is a fallback list, and the failure mode of a fallback list is that nobody notices the first entry died until the second one does too — so success arriving on a host that is not the first in the list is the alert. It exists because of a day of downtime: a dRPC key hit its daily compute-unit limit, answered every call with `Your key daily limit exceeded` inside a 200 OK, and the Alchemy endpoint configured right behind it never received a single request. The fallback loop only advanced on transport failures, and a JSON-RPC error is not one.

The fix is a classification rather than "retry everything" — see `internal/eth/rpcerror.go`. An error about the *endpoint* (a quota, a rate limit, a batch size the plan refuses, an expired key, a method the node does not serve, a non-2xx status) fails over, because another endpoint would answer. An error about the *call* (`execution reverted`, `out of gas`, a bad parameter) is returned, because every endpoint would answer the same — and `out of gas` in particular is load bearing: the Multicall3 bisect is driven by seeing it, so failing over on it would break aggregation. Unknown wording is treated as call-level, which is the conservative direction: a wrongly-retried call spends requests on every provider and still fails.

`keys_api_staking_router_changes_total` is the one that should stay at zero unless a deploy happened. The locator is re-read every cycle — as a second call inside the module-set request, so it costs no round trip — and when it starts answering with a different staking router the service follows it and increments this. Before that, the address was resolved once and held for the life of the process: a deploy that replaced the router left the service reading a router nobody used any more, reporting a registry that was quietly frozen while every freshness metric looked healthy. The cure was noticing and restarting the pod.

Both events force a rebuild of the module rows on the next cycle — not of the keys. Nothing else would notice: a replacement router can report the same modules with the same nonces, and a version bump changes what a tuple means without moving a counter, so every cheap signal says the published version is current while its module rows came from a contract that no longer applies. `keys_api_staking_router_contract_version` is the same idea one level down. The router's contract version is what says how to read the module tuple — before v4 there is no withdrawal credentials type and 1 is implied — and it is read with the module set, every cycle, rather than assumed from the ABI this binary was built against. The ABI is a property of the build and the tuple is a property of the chain; reading one for the other is how a service claims to support a version it has never seen. An upgrade logs at warn and moves the gauge.

Alongside those, the canonical service's own metrics are exported under the canonical names — `lido_keys_api_*` plus the unprefixed `el_rpc_requests_duration_seconds`, `http_requests_duration_seconds` and `build_info` — so its dashboards and alerts work against this service unchanged. Two deliberate differences: `lido_keys_api_keys_by_sr_module_and_operator` is refreshed for the modules a rebuild read rather than re-read from the database every cycle, because a quiet cycle here reads no operators at all; and `validators_registry_*` has no counterpart, since that is the CL-side registry this service does not have.

The API document is at `/openapi.yaml`, rendered at `/docs`. It is hand-written and embedded, and a test checks it against the router in both directions — a route without a spec entry, or a spec path that is no longer routed, fails the build. Descriptions can still go stale; the surface cannot.

## Watching it work

Every line carries `chain_id` and `schema`, the two attributes that answer "which chain was this?" from any line rather than from the startup one. That question is cheap to answer when you think to ask it and expensive when you do not: a node on the wrong chain produces a log that looks entirely healthy.

`LOG_LEVEL=debug` is worth knowing about, because at info level a cold sync says nothing for tens of seconds at a time and a slow node is indistinguishable from a hang. Debug adds:

- one line per JSON-RPC request — method, batch size, which endpoint answered, how long it took;
- a staging heartbeat every five seconds during a module read, with the running key rate;
- one line per operator as its keys land, with the batch count;
- the freshness decision together with what it was decided from: the nonces, the block gap, the operator-event scan per module;
- the resolved version per served request, and the row count and duration per module query.

A quiet cycle at debug is eight RPC calls and about fifteen lines, which is the whole cycle:

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

At info level the same cycle prints nothing, which is deliberate: a rebuild, a published version, retention actually dropping something, and any failure are all info or above, and everything else is noise on a five-second timer.
