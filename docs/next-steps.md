# What is still open

Written to be picked up cold. Measurements and the reasoning behind decisions live in the [README](../README.md) and [architecture.md](architecture.md); this page lists only what is not done. Earlier versions of it, with the experiment log, are in git history.

## State

The service is `cmd/keys-api`: ingest plus API in one process (`KEYS_API_MODE=all`), or split into a writer (`ingest`) and any number of readers (`server`) over one Postgres. `/v1` and `/v2` are held byte-identical to canonical keys-api 6.0.0 by `cmd/diff` (`-live` for responses, `-errors` for the error surface, key order included); everything this service adds is under `/ext/v1`. A cold mainnet sync (525 488 keys, 5 modules) takes about 30 s against a hosted provider.

The security and weakness review of November 2026 is closed except for the items below.

## Deployment and operations

- **`/metrics` shares the API port.** Anyone who can reach the API reads operator names by index and endpoint hosts. Moving it to its own port needs the chart's scrape config changed in the same release.
- **Reader and writer share one pool in `KEYS_API_MODE=all`.** Dumps and diffs take at most `STREAM_MAX_CONCURRENT` connections (half the pool by default), but a burst of short reads can still hold the rest the writer needs. The split deployment (`ingest` plus `server`) avoids it; separate pools in one process would too.
- **Direct connections only.** Leases and the writer lock are session-level advisory locks, which a transaction-mode pooler (PgBouncer) breaks silently. Nothing checks for one.
- **A schema change is a cold sync for readers.** Migrate now runs in one locked transaction, so two image versions no longer break each other, but readers answer 425 until the new schema has a published version. Building under a fingerprint-suffixed schema and switching readers when it publishes would remove the gap.
- **The secrets file should be mode 0400 or 0440.** The service warns when it is readable by group or others; volumes and the vault agent default to 0644.

## Correctness

- **Nonce equality across a reorg is not sufficient on its own.** A counter can roll back two increments and take two different ones on the new branch. The fix is an event scan over the reorged range, which needs the key events' signatures from the contracts (`SigningKeyAdded`, `VettedSigningKeysCountChanged` and the CSM equivalents); a guessed signature returns no logs and looks like "nothing changed". The canonical service has the same exposure.
- **An operator name over 256 characters fails the build**, as it does in the canonical service (`varchar(256)` on both sides). Truncating would diverge from canonical output.
- **An audit republishes a content-identical version**, one version row per audit interval whether or not anything changed. Harmless; retention absorbs it.
- **A load-balanced provider can fail a read of the head it just reported** (seen on dRPC: `Upstream height … is less than …`). Only relevant behind such an endpoint; a retry around the head read would cover it.

## Observability

- `lido_keys_api_sr_module_key_update_duration_seconds` keeps the canonical `totalKeysAmount` label for panel parity, which creates a series per distinct key count. Drop the label in the scrape config if it matters.
- No fuzz targets are checked in for the ABI decoders. A 45 s fuzz of unpack plus decoders found nothing; a checked-in target would keep it that way.

## Performance and experiments

- **The key table's index footprint** is close to the table's size. `kv_from` and `kv_to` now also serve the build guard and the scan for unfinished attempts, so measure before dropping either.
- **Event logs for the incremental path.** `eth_getLogs` over a range returns key additions without per-operator calls; `used`/`vetted` are state, so calls still supply the rest. `eth_subscribe("logs")` could replace polling in steady state.
- **Erigon against geth** for wide log ranges: only geth was measured.
- **E8, a lean TypeScript baseline** (bare HTTP, streaming query, hand-rolled serializer) against the same seeded database, to settle the language question with a number. Threshold: within 3× of the Go p50 and under 100 MiB peak at 1 CPU / 512 MiB.
- **Validator ingest** from the consensus layer is not built: the exit routes answer "registry disabled". No known consumer reads them — the council daemon and the oracle do not.

## Decisions that need a person

- Where `safe` comes from: reorg depth in blocks, a CL justified checkpoint, or a configured lag. A CL endpoint would become a hard dependency.
- How deep the retention window is, and who pays for the storage.
- Whether the canonical service adopts published snapshots. It is a contract change on a protocol-critical service and belongs to its owners.
- Whether this becomes a service we run for other consumers, which decides how far the extension surface goes.

## Bringing the stand up

Two services on the same chain, ours and the canonical one, over one Postgres with a database each:

```bash
export PROVIDERS_URLS=http://node-1,http://node-2
docker compose up -d --build            # both sync mainnet themselves
docker compose run --rm tools diff -a http://canonical:3000 -b http://keys-api:3000 -live
docker compose run --rm tools diff -errors -a http://canonical:3000 -b http://keys-api:3000
docker compose run --rm tools bench -url http://keys-api:3000/v1/keys
```

Benchmark from inside the compose network: through published ports on Docker Desktop the host hop caps everything at about 188 MiB/s. A hosted provider's free plan may refuse JSON-RPC batches over three requests; set `PROVIDER_JSON_RPC_MAX_BATCH_SIZE=3` for the canonical service there. Give the canonical service more than 512 MiB if the endpoint list includes `/v1/modules/keys`.
