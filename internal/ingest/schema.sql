-- Schema for the versioned registry this service owns. Applied on every boot.
--
-- There is no migration tool and there will not be one: when this file changes, the
-- schema is dropped and rebuilt. Everything in it is derived from the chain, a cold sync
-- is 14 s on Hoodi and 49 s on mainnet, and migrating a cache costs more than refilling
-- it. See Migrate in pgstore.go.
--
-- The alternative was tried for one column and is instructive. `create table if not
-- exists` does nothing to a table that already exists, so an added column needs an
-- `alter`, and an `alter … add column … default 0` leaves old rows carrying a value the
-- writer never wrote: versions from before the change reported finalized block 0, which
-- reads as "finality unknown" rather than "this predates the column". The shape was right
-- and the meaning was wrong. A rename or a type change cannot be written idempotently at
-- all, so the approach only ever covers the easy half.
--
-- The shape is one `versions` table plus three data tables. A version's status
-- decides whether readers can see it; a data row says which block range it was
-- alive for. Reading a version is one predicate on every table, with :b the
-- version's block number:
--
--     where from_block <= :b and (to_block is null or to_block > :b)
--
-- A range rather than a single version stamp is what makes writes cheap. Tagging
-- every row with one version would mean copying all 500 000 keys per version —
-- about 100 MB of table plus index each, and a full rewrite for a change of one key.
-- With ranges, a version that changes one operator writes the handful of rows that
-- moved and inherits the rest by leaving them open. Retention still costs what it
-- should: rows closed before the window fall out and get deleted.
--
-- Bytes, not hex strings, wherever the row count is large. A pubkey is 48 bytes and
-- a signature 96; as 0x text they cost twice that and admit two spellings of the
-- same value. Measured here: the key table halves, 200 MB to 100 MB, sequential
-- latency unchanged, and hex encoding on the way out costs nothing until the CPU is
-- saturated. Small tables keep readable text — twenty version rows are not worth
-- losing psql legibility over.

-- What layout the schema was last built from. When it stops matching the embedded file,
-- the schema is dropped and rebuilt rather than migrated — see Migrate in pgstore.go.
create table if not exists schema_fingerprint (
    id          smallint    primary key default 1 check (id = 1),
    fingerprint text        not null,
    applied_at  timestamptz not null default now()
);

-- Which writer may write: each one that takes the lock bumps the epoch, and every write
-- transaction reads it `for share` first. The bump waits for those, so a writer that lost the
-- lock either finishes before the next one starts or finds a newer epoch and rolls back.
-- Seeded from the clock in microseconds: a rebuild recreates the row, and an epoch counted from
-- zero again could equal the one an old writer still holds.
create table if not exists writer_fence (
    id    smallint primary key default 1 check (id = 1),
    epoch bigint   not null
);
insert into writer_fence (id, epoch)
values (1, (extract(epoch from clock_timestamp()) * 1000000)::bigint)
on conflict do nothing;

create table if not exists versions (
    id                      bigserial primary key,
    -- The block the data was read at. This is the version's identity and it never
    -- changes: pins, cursors and diffs all name a version by it, and a walk that
    -- takes minutes must not have the ground move underneath it.
    block_number            bigint      not null,
    block_hash              varchar(66) not null,
    timestamp               bigint      not null,
    -- The newest block this version was confirmed current at. This is what moves
    -- when a cycle finds nothing changed, and it is what `meta.elBlockSnapshot`
    -- reports — same as the canonical service, where the meta advances while the
    -- data stays from the last update.
    head_block_number       bigint      not null default 0,
    head_block_hash         varchar(66) not null default '',
    head_timestamp          bigint      not null default 0,
    -- The newest finalized block observed when this version was last confirmed. Moves
    -- with the head, for the same reason: it is an observation about the chain, not part
    -- of the version's identity.
    --
    -- It exists so `?snapshot=finalized` can be answered by a predicate — the newest published
    -- version at or below it — instead of by a label somebody has to keep moving. A
    -- version at or below a finalized block cannot be superseded, which is the whole
    -- point of the pin: it never answers 409.
    --
    -- 0 means the source reports no finality (a file, a devnet before its first
    -- finalization, a node without the tag). The pin is then unavailable rather than
    -- guessed at.
    finalized_block_number  bigint      not null default 0,
    last_changed_block_hash varchar(66) not null,
    label                   text        not null default 'latest',
    -- building → published → (stale) → superseded. The status change is what makes
    -- a version current, not the arrival of its rows, so filling a version can take
    -- as long as it takes without a reader ever seeing part of it.
    status                  text        not null default 'building',
    -- Per-module change counters observed when the version was built. One cheap
    -- read per cycle compares them against head to decide whether the version is
    -- still current; nothing else is read to make that decision.
    module_nonces           jsonb       not null default '{}'::jsonb,
    -- Which source produced it, so a store fed by more than one stays legible.
    source                  text        not null default '',
    created_at              timestamptz not null default now(),
    unique (block_number, block_hash)
);

create index if not exists versions_status_id on versions (status, id desc);
create index if not exists versions_block on versions (block_number desc);

create table if not exists module_version (
    row_id                      bigserial primary key,
    module_id                   smallint    not null,
    address                     bytea       not null,
    type                        text        not null,
    name                        text        not null,
    nonce                       bigint      not null,
    module_fee                  int         not null,
    treasury_fee                int         not null,
    target_share                int         not null,
    status                      int         not null,
    last_deposit_at             bigint      not null,
    last_deposit_block          bigint      not null,
    exited_validators_count     int         not null,
    active                      boolean     not null,
    withdrawal_credentials_type int         not null,
    -- Moves only when this module's own content moved. Derived from a comparison of
    -- content, not from the act of reading — which is where the canonical field goes
    -- wrong: there, a restart or a stall stamps every module with the same block and
    -- the per-module signal collapses into the global one.
    last_changed_block_hash     varchar(66) not null,
    from_block                  bigint      not null,
    to_block                    bigint
);

create index if not exists mv_range on module_version (from_block, to_block);
create unique index if not exists mv_open_ident on module_version (module_id) where to_block is null;

create table if not exists operator_version (
    row_id                       bigserial primary key,
    module_id                    smallint     not null,
    operator_index               int          not null,
    active                       boolean      not null,
    name                         varchar(256) not null,
    reward_address               bytea        not null,
    staking_limit                int          not null,
    stopped_validators           int          not null,
    total_signing_keys           int          not null,
    used_signing_keys            int          not null,
    depositable_validators_count int          not null,
    -- The node operator group, as the module's MetaRegistry reports it. Null where
    -- the module has no such registry, which is every module but curated-onchain-v2:
    -- the concept does not exist there, and storing 0 would be indistinguishable
    -- from the registry's NO_GROUP_ID sentinel, which does exist and means "in no
    -- group". Two different facts, so two different values.
    group_id                     int,
    -- Null where the module has no such counter (curated-onchain-v1), as in the
    -- canonical service; 0 is a real value for the CSM-shaped modules.
    total_withdrawn_keys         int,
    from_block                   bigint       not null,
    to_block                     bigint
);

create index if not exists ov_range on operator_version (module_id, from_block, to_block);
create unique index if not exists ov_open_ident on operator_version (module_id, operator_index) where to_block is null;

create table if not exists key_version (
    row_id         bigserial primary key,
    -- A two-byte module id instead of a 42-character address repeated on every row.
    -- The address lives once, in module_version.
    module_id      smallint not null,
    operator_index int      not null,
    key_index      int      not null,
    pubkey         bytea    not null,
    signature      bytea    not null,
    used           boolean  not null,
    vetted         boolean  not null,
    from_block     bigint   not null,
    to_block       bigint
);

-- Serves pinned reads, including the head. The range predicate is the only read
-- predicate: serving the head from `to_block is null` instead lets a publish
-- landing between two per-module queries stitch one response out of two versions,
-- which this repo measured happening (2 of 6 dumps torn at a module boundary).
create index if not exists kv_range on key_version (module_id, from_block, to_block);
-- Serves the writer's diff: find the open row for one key identity.
create unique index if not exists kv_open_ident on key_version (module_id, operator_index, key_index) where to_block is null;
-- Serves /v1/keys/{pubkey} and the find endpoints across modules.
create index if not exists kv_pubkey on key_version (pubkey);
-- Serves pagination: a page is a range scan in key identity order, and the cursor
-- is that identity. Unlike kv_open_ident this one is not partial, because a page of
-- a historical version reads rows that are already closed.
create index if not exists kv_ident on key_version (module_id, operator_index, key_index);
-- Serves `since=` diffs: rows opened, and rows closed, inside a block interval.
create index if not exists kv_from on key_version (from_block);
create index if not exists kv_to on key_version (to_block) where to_block is not null;
