-- Schema for the versioned registry this service owns. Applied on every boot;
-- every statement is idempotent, so there is no migration tool and no migration
-- state to get wrong.
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
