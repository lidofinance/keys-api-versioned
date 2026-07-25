package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// pgBuild is one version being filled in Postgres.
//
// It holds no transaction between calls. Each module's write is its own short
// transaction, which is what the `building` status buys: a version can be filled
// across many transactions, over minutes, and readers see none of it until the
// status flips. The canonical service, having no such status, has to hold one long
// transaction per dump instead — which is where its MVCC bloat comes from.
type pgBuild struct {
	store *PGStore
	id    int64
	at    model.ELBlockSnapshot
	hash  string

	// anyChanged records whether any module's content moved in this version, which
	// decides the version-level lastChangedBlockHash.
	anyChanged bool
	// nonces collects the per-module change counters observed, for the next
	// cycle's cheap check.
	nonces map[string]int64
}

func (b *pgBuild) Block() int64      { return b.at.BlockNumber }
func (b *pgBuild) BlockHash() string { return b.hash }

// Keys opens a per-module key write.
func (b *pgBuild) Keys(ctx context.Context, module model.StakingModule) (ModuleKeys, error) {
	conn, err := b.store.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("begin key write: %w", err)
	}

	// A temporary table scoped to the transaction: no name collisions between
	// concurrent module writes, and no cleanup to forget.
	if _, err := tx.Exec(ctx, `create temp table staged_keys (
			operator_index int   not null,
			key_index      int   not null,
			pubkey         bytea not null,
			signature      bytea not null,
			used           boolean not null,
			vetted         boolean not null
		) on commit drop`); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, fmt.Errorf("staging table: %w", err)
	}

	return &pgModuleKeys{
		build:    b,
		conn:     conn,
		tx:       tx,
		moduleID: module.ID,
		pending:  make([]stagedKey, 0, b.store.keyStageBatch),
	}, nil
}

// stagedKey is the minimum a key write needs. The module address is not here: the
// row carries a two-byte module id instead, and the address lives once in
// module_version.
type stagedKey struct {
	operatorIndex int32
	keyIndex      int32
	pubkey        []byte
	signature     []byte
	used          bool
	vetted        bool
}

// pgModuleKeys stages one module's keys and applies them as a diff.
type pgModuleKeys struct {
	build    *pgBuild
	conn     *pgxpool.Conn
	tx       pgx.Tx
	moduleID int32

	pending []stagedKey
	staged  int64
	done    bool
}

// Add stages a key, flushing to the staging table in batches so that memory does
// not scale with the module's size.
//
// A source may hand over either form — bytes straight from a contract call, or hex
// from a file — and the bytes are what get stored either way.
func (k *pgModuleKeys) Add(ctx context.Context, key model.Key) error {
	pubkey, signature := key.PubkeyBin, key.SignatureBin
	if pubkey == nil {
		decoded, err := model.DecodeHex(key.Key)
		if err != nil {
			return fmt.Errorf("operator %d key %d: %w", key.OperatorIndex, key.Index, err)
		}
		pubkey = decoded
	}
	if signature == nil {
		decoded, err := model.DecodeHex(key.DepositSignature)
		if err != nil {
			return fmt.Errorf("operator %d signature %d: %w", key.OperatorIndex, key.Index, err)
		}
		signature = decoded
	}

	k.pending = append(k.pending, stagedKey{
		operatorIndex: key.OperatorIndex,
		keyIndex:      key.Index,
		pubkey:        pubkey,
		signature:     signature,
		used:          key.Used,
		vetted:        key.Vetted,
	})
	if len(k.pending) >= k.build.store.keyStageBatch {
		return k.flush(ctx)
	}
	return nil
}

func (k *pgModuleKeys) flush(ctx context.Context) error {
	if len(k.pending) == 0 {
		return nil
	}
	rows := k.pending
	n, err := k.tx.CopyFrom(ctx,
		pgx.Identifier{"staged_keys"},
		[]string{"operator_index", "key_index", "pubkey", "signature", "used", "vetted"},
		pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
			r := rows[i]
			return []any{r.operatorIndex, r.keyIndex, r.pubkey, r.signature, r.used, r.vetted}, nil
		}))
	if err != nil {
		return fmt.Errorf("stage keys: %w", err)
	}
	k.staged += n
	k.pending = k.pending[:0]
	return nil
}

// Commit applies the staged set as a diff against the rows currently open.
//
// Two statements do the work, and the order matters. The first closes every open
// row the staged set does not contain identically — gone, or changed. The second
// opens a row for every staged key that no longer has an identical open row, which
// after the first statement is exactly the new and the changed ones. Keys that did
// not move are matched by both and written by neither, which is why a version that
// changes one operator costs a handful of rows rather than 500 000.
func (k *pgModuleKeys) Commit(ctx context.Context) (KeyDiff, error) {
	defer k.release()
	if k.done {
		return KeyDiff{}, errors.New("key write already finished")
	}
	k.done = true

	if err := k.flush(ctx); err != nil {
		return KeyDiff{}, err
	}

	// The comparison below joins on key identity, so the staging table needs it.
	if _, err := k.tx.Exec(ctx, `create index on staged_keys (operator_index, key_index)`); err != nil {
		return KeyDiff{}, fmt.Errorf("index staging table: %w", err)
	}

	block := k.build.at.BlockNumber
	diff := KeyDiff{Staged: k.staged}

	closed, err := k.tx.Exec(ctx, `
		update key_version v set to_block = $2
		 where v.module_id = $1
		   and v.to_block is null
		   and not exists (
		       select 1 from staged_keys s
		        where s.operator_index = v.operator_index
		          and s.key_index = v.key_index
		          and s.pubkey = v.pubkey
		          and s.signature = v.signature
		          and s.used = v.used
		          and s.vetted = v.vetted)`, k.moduleID, block)
	if err != nil {
		return diff, fmt.Errorf("close superseded keys: %w", err)
	}
	diff.Closed = closed.RowsAffected()

	opened, err := k.tx.Exec(ctx, `
		insert into key_version
			(module_id, operator_index, key_index, pubkey, signature, used, vetted, from_block, to_block)
		select $1, s.operator_index, s.key_index, s.pubkey, s.signature, s.used, s.vetted, $2, null
		  from staged_keys s
		 where not exists (
		       select 1 from key_version v
		        where v.module_id = $1
		          and v.to_block is null
		          and v.operator_index = s.operator_index
		          and v.key_index = s.key_index)`, k.moduleID, block)
	if err != nil {
		return diff, fmt.Errorf("open new keys: %w", err)
	}
	diff.Opened = opened.RowsAffected()
	diff.Unchanged = diff.Staged - diff.Opened

	if err := k.tx.Commit(ctx); err != nil {
		return diff, fmt.Errorf("commit key write: %w", err)
	}
	if diff.Changed() {
		k.build.anyChanged = true
	}
	return diff, nil
}

func (k *pgModuleKeys) Abort(ctx context.Context) {
	if k.done {
		return
	}
	k.done = true
	_ = k.tx.Rollback(ctx)
	k.release()
}

func (k *pgModuleKeys) release() {
	if k.conn != nil {
		k.conn.Release()
		k.conn = nil
	}
}

// Operators replaces a module's operator set, by the same diff rule as keys.
func (b *pgBuild) Operators(ctx context.Context, module model.StakingModule, operators []model.Operator) (int, error) {
	block := b.at.BlockNumber

	conn, err := b.store.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin operator write: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `create temp table staged_operators (
			operator_index               int     not null,
			active                       boolean not null,
			name                         varchar(256) not null,
			reward_address               bytea   not null,
			staking_limit                int     not null,
			stopped_validators           int     not null,
			total_signing_keys           int     not null,
			used_signing_keys            int     not null,
			depositable_validators_count int     not null
		) on commit drop`); err != nil {
		return 0, fmt.Errorf("staging table: %w", err)
	}

	staged := make([][]any, 0, len(operators))
	for _, o := range operators {
		reward, err := model.DecodeHex(o.RewardAddress)
		if err != nil {
			return 0, fmt.Errorf("operator %d reward address: %w", o.Index, err)
		}
		staged = append(staged, []any{o.Index, o.Active, o.Name, reward, o.StakingLimit,
			o.StoppedValidators, o.TotalSigningKeys, o.UsedSigningKeys, o.DepositableValidatorsCount})
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"staged_operators"},
		[]string{"operator_index", "active", "name", "reward_address", "staking_limit",
			"stopped_validators", "total_signing_keys", "used_signing_keys", "depositable_validators_count"},
		pgx.CopyFromSlice(len(staged), func(i int) ([]any, error) { return staged[i], nil })); err != nil {
		return 0, fmt.Errorf("stage operators: %w", err)
	}

	closed, err := tx.Exec(ctx, `
		update operator_version v set to_block = $2
		 where v.module_id = $1
		   and v.to_block is null
		   and not exists (
		       select 1 from staged_operators s
		        where s.operator_index = v.operator_index
		          and s.active = v.active
		          and s.name = v.name
		          and s.reward_address = v.reward_address
		          and s.staking_limit = v.staking_limit
		          and s.stopped_validators = v.stopped_validators
		          and s.total_signing_keys = v.total_signing_keys
		          and s.used_signing_keys = v.used_signing_keys
		          and s.depositable_validators_count = v.depositable_validators_count)`, module.ID, block)
	if err != nil {
		return 0, fmt.Errorf("close superseded operators: %w", err)
	}

	opened, err := tx.Exec(ctx, `
		insert into operator_version
			(module_id, operator_index, active, name, reward_address, staking_limit, stopped_validators,
			 total_signing_keys, used_signing_keys, depositable_validators_count, from_block, to_block)
		select $1, s.operator_index, s.active, s.name, s.reward_address, s.staking_limit, s.stopped_validators,
		       s.total_signing_keys, s.used_signing_keys, s.depositable_validators_count, $2, null
		  from staged_operators s
		 where not exists (
		       select 1 from operator_version v
		        where v.module_id = $1 and v.to_block is null
		          and v.operator_index = s.operator_index)`, module.ID, block)
	if err != nil {
		return 0, fmt.Errorf("open new operators: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit operator write: %w", err)
	}

	changed := int(closed.RowsAffected() + opened.RowsAffected())
	if changed > 0 {
		b.anyChanged = true
	}
	return changed, nil
}

// Module writes the module row, moving its lastChangedBlockHash only when
// something about the module actually moved.
func (b *pgBuild) Module(ctx context.Context, m model.StakingModule, contentChanged bool) error {
	address, err := model.DecodeHex(m.RawAddress())
	if err != nil {
		return fmt.Errorf("module %d address: %w", m.ID, err)
	}

	if b.nonces == nil {
		b.nonces = map[string]int64{}
	}
	b.nonces[strings.ToLower(m.RawAddress())] = int64(m.Nonce)

	tx, err := b.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// The open row for this module, if any, plus whether its fields still match.
	// last_changed_block_hash is deliberately not part of the comparison: it is
	// derived from the comparison, and comparing it would make every re-read look
	// like a change.
	var (
		openRowID      int64
		prevLastChange string
		fieldsMatch    bool
	)
	err = tx.QueryRow(ctx, `
		select row_id, last_changed_block_hash,
		       (address, type, name, nonce, module_fee, treasury_fee, target_share, status,
		        last_deposit_at, last_deposit_block, exited_validators_count, active,
		        withdrawal_credentials_type)
		       = ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		  from module_version
		 where module_id = $1 and to_block is null`,
		m.ID, address, m.Type, m.Name, int64(m.Nonce), m.ModuleFee, m.TreasuryFee, m.TargetShare,
		m.Status, m.LastDepositAt, m.LastDepositBlock, m.ExitedValidatorsCount, m.Active,
		m.WithdrawalCredentialsType).Scan(&openRowID, &prevLastChange, &fieldsMatch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// First time this module is seen.
	case err != nil:
		return fmt.Errorf("read open module row: %w", err)
	}

	if openRowID != 0 && fieldsMatch && !contentChanged {
		// Nothing moved: leave the previous row open and its stamp alone. This is
		// the case a re-read after a restart lands in, and it is exactly where the
		// canonical field lies about having changed.
		return tx.Commit(ctx)
	}

	if openRowID != 0 {
		if _, err := tx.Exec(ctx,
			`update module_version set to_block = $2 where row_id = $1`, openRowID, b.at.BlockNumber); err != nil {
			return fmt.Errorf("close module row: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		insert into module_version
			(module_id, address, type, name, nonce, module_fee, treasury_fee, target_share, status,
			 last_deposit_at, last_deposit_block, exited_validators_count, active,
			 withdrawal_credentials_type, last_changed_block_hash, from_block, to_block)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, null)`,
		m.ID, address, m.Type, m.Name, int64(m.Nonce), m.ModuleFee, m.TreasuryFee, m.TargetShare,
		m.Status, m.LastDepositAt, m.LastDepositBlock, m.ExitedValidatorsCount, m.Active,
		m.WithdrawalCredentialsType, b.hash, b.at.BlockNumber); err != nil {
		return fmt.Errorf("insert module row: %w", err)
	}

	b.anyChanged = true
	return tx.Commit(ctx)
}

// Publish makes the version current.
//
// One row update, whatever the version's size. The version-level
// lastChangedBlockHash moves only when some module's content moved, so a consumer
// polling it keeps the skip guarantee the canonical contract promises — only here
// it is true.
func (b *pgBuild) Publish(ctx context.Context, label string) error {
	if label == "" {
		label = "latest"
	}

	lastChanged := b.hash
	if !b.anyChanged {
		var previous string
		err := b.store.pool.QueryRow(ctx, `select last_changed_block_hash from versions
			where status in ('published', 'stale') and id <> $1
			order by block_number desc limit 1`, b.id).Scan(&previous)
		switch {
		case err == nil && previous != "":
			lastChanged = previous
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("previous lastChangedBlockHash: %w", err)
		}
	}

	nonces, err := json.Marshal(b.nonceMap())
	if err != nil {
		return err
	}

	tx, err := b.store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// The previous head is not retired: it stays published, and it stays readable by
	// an explicit pin for as long as retention keeps it. Currency is "the newest
	// published version", not a flag, so succession costs no write to the old row.
	//
	// The one thing to undo is staleness. It was a statement about the head — "this
	// no longer reflects the chain" — and once a newer version exists the older one
	// is history rather than a stale head, so the distrust no longer applies.
	// `superseded` stays reserved for what it actually means: taken off the chain by
	// a reorg. A consumer pinned to one of those needs 409, and a consumer pinned to
	// ordinary history needs its data.
	if _, err := tx.Exec(ctx,
		`update versions set status = 'published' where status = 'stale' and id <> $1`, b.id); err != nil {
		return fmt.Errorf("clear staleness on the previous head: %w", err)
	}

	if _, err := tx.Exec(ctx, `update versions
		   set status = 'published', label = $2, last_changed_block_hash = $3, module_nonces = $4
		 where id = $1`, b.id, label, lastChanged, nonces); err != nil {
		return fmt.Errorf("publish version: %w", err)
	}

	return tx.Commit(ctx)
}

// Abort leaves the version where it is: invisible, claimed by nothing, collected
// by retention. Deleting its rows here would be a second long write on a path that
// is already failing.
func (b *pgBuild) Abort(ctx context.Context) {
	_, _ = b.store.pool.Exec(ctx,
		`update versions set status = 'superseded' where id = $1 and status = 'building'`, b.id)
}

func (b *pgBuild) nonceMap() map[string]int64 {
	if b.nonces == nil {
		return map[string]int64{}
	}
	return b.nonces
}

var (
	_ Build      = (*pgBuild)(nil)
	_ ModuleKeys = (*pgModuleKeys)(nil)
)
