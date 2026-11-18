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

// pgBuild holds no transaction between calls: each module write is its own short transaction,
// and the building status hides the version until Publish.
type pgBuild struct {
	store *PGStore
	id    int64
	at    model.ELBlockSnapshot
	hash  string

	// anyChanged decides the version-level lastChangedBlockHash.
	anyChanged bool
	nonces     map[string]int64
}

func (b *pgBuild) Block() int64      { return b.at.BlockNumber }
func (b *pgBuild) BlockHash() string { return b.hash }

// Keys stages into a session temp table with no transaction open: a module's read takes minutes,
// and an open transaction would hold back vacuum and meet idle_in_transaction_session_timeout.
func (b *pgBuild) Keys(ctx context.Context, module model.StakingModule) (ModuleKeys, error) {
	conn, err := b.store.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}

	// Dropped first: a connection whose last build failed mid-way can come back with it.
	if _, err := conn.Exec(ctx, `drop table if exists staged_keys`); err != nil {
		conn.Release()
		return nil, fmt.Errorf("staging table: %w", err)
	}
	if _, err := conn.Exec(ctx, `create temp table staged_keys (
			operator_index int   not null,
			key_index      int   not null,
			pubkey         bytea not null,
			signature      bytea not null,
			used           boolean not null,
			vetted         boolean not null
		)`); err != nil {
		conn.Release()
		return nil, fmt.Errorf("staging table: %w", err)
	}

	return &pgModuleKeys{
		build:    b,
		conn:     conn,
		moduleID: module.ID,
		pending:  make([]stagedKey, 0, b.store.keyStageBatch),
	}, nil
}

type stagedKey struct {
	operatorIndex int32
	keyIndex      int32
	pubkey        []byte
	signature     []byte
	used          bool
	vetted        bool
}

type pgModuleKeys struct {
	build    *pgBuild
	conn     *pgxpool.Conn
	moduleID int32

	pending []stagedKey
	staged  int64
	done    bool
	keep    map[int32]int32
}

func (k *pgModuleKeys) KeepPrefix(keep map[int32]int32) { k.keep = keep }

// SettledKeys: a run of current rows from key 0 that are all deposited and were written at or below
// finalized. A missing index or a row that fails either test ends the operator's run there.
func (b *pgBuild) SettledKeys(ctx context.Context, module model.StakingModule, finalized int64) (map[int32]int32, error) {
	rows, err := b.store.pool.Query(ctx, `
		with current_keys as (
			select operator_index, key_index, (used and vetted and from_block <= $2) as settled
			  from key_version
			 where module_id = $1 and to_block is null
		), runs as (
			select operator_index,
			       coalesce(min(key_index) filter (where not settled), max(key_index) + 1) as n
			  from current_keys
			 group by operator_index
		)
		select r.operator_index, r.n
		  from runs r
		 where r.n > 0
		   and r.n = (select count(*) from current_keys c
		               where c.operator_index = r.operator_index and c.key_index < r.n)`,
		module.ID, finalized)
	if err != nil {
		return nil, fmt.Errorf("settled keys of module %d: %w", module.ID, err)
	}
	defer rows.Close()

	out := map[int32]int32{}
	for rows.Next() {
		var operator, n int32
		if err := rows.Scan(&operator, &n); err != nil {
			return nil, fmt.Errorf("settled keys of module %d: %w", module.ID, err)
		}
		out[operator] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("settled keys of module %d: %w", module.ID, err)
	}
	return out, nil
}

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
	n, err := k.conn.CopyFrom(ctx,
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

// Commit closes open rows not staged identically, then opens rows for staged keys left without
// one. Unchanged keys are written by neither, so one changed operator costs a handful of rows.
// Rows below a kept prefix were not staged and are left alone.
func (k *pgModuleKeys) Commit(ctx context.Context) (KeyDiff, error) {
	defer k.release()
	if k.done {
		return KeyDiff{}, errors.New("key write already finished")
	}
	k.done = true

	if err := k.flush(ctx); err != nil {
		return KeyDiff{}, err
	}

	tx, err := k.conn.Begin(ctx)
	if err != nil {
		return KeyDiff{}, fmt.Errorf("begin key write: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `create index on staged_keys (operator_index, key_index)`); err != nil {
		return KeyDiff{}, fmt.Errorf("index staging table: %w", err)
	}

	block := k.build.at.BlockNumber
	diff := KeyDiff{Staged: k.staged}
	keepOperators := make([]int32, 0, len(k.keep))
	keepBelow := make([]int32, 0, len(k.keep))
	for operator, n := range k.keep {
		keepOperators = append(keepOperators, operator)
		keepBelow = append(keepBelow, n)
		diff.Kept += int64(n)
	}

	closed, err := tx.Exec(ctx, `
		update key_version v set to_block = $2
		 where v.module_id = $1
		   and v.to_block is null
		   and v.key_index >= coalesce(
		       (select k.n from unnest($3::int[], $4::int[]) as k(operator_index, n)
		         where k.operator_index = v.operator_index), 0)
		   and not exists (
		       select 1 from staged_keys s
		        where s.operator_index = v.operator_index
		          and s.key_index = v.key_index
		          and s.pubkey = v.pubkey
		          and s.signature = v.signature
		          and s.used = v.used
		          and s.vetted = v.vetted)`, k.moduleID, block, keepOperators, keepBelow)
	if err != nil {
		return diff, fmt.Errorf("close superseded keys: %w", err)
	}
	diff.Closed = closed.RowsAffected()

	opened, err := tx.Exec(ctx, `
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

	if _, err := tx.Exec(ctx, `drop table staged_keys`); err != nil {
		return diff, fmt.Errorf("drop staging table: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
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
	_, _ = k.conn.Exec(ctx, `drop table if exists staged_keys`)
	k.release()
}

func (k *pgModuleKeys) release() {
	if k.conn != nil {
		k.conn.Release()
		k.conn = nil
	}
}

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
			depositable_validators_count int     not null,
			group_id                     int,
			total_withdrawn_keys         int
		) on commit drop`); err != nil {
		return 0, fmt.Errorf("staging table: %w", err)
	}

	staged := make([][]any, 0, len(operators))
	for _, o := range operators {
		reward, err := model.DecodeHex(o.RewardAddress)
		if err != nil {
			return 0, fmt.Errorf("operator %d reward address: %w", o.Index, err)
		}
		var group, withdrawn any
		if o.Group != nil {
			group = int32(*o.Group)
		}
		if o.TotalWithdrawnKeys != nil {
			withdrawn = *o.TotalWithdrawnKeys
		}
		staged = append(staged, []any{o.Index, o.Active, o.Name, reward, o.StakingLimit,
			o.StoppedValidators, o.TotalSigningKeys, o.UsedSigningKeys, o.DepositableValidatorsCount,
			group, withdrawn})
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"staged_operators"},
		[]string{"operator_index", "active", "name", "reward_address", "staking_limit",
			"stopped_validators", "total_signing_keys", "used_signing_keys", "depositable_validators_count",
			"group_id", "total_withdrawn_keys"},
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
		          and s.depositable_validators_count = v.depositable_validators_count
		          -- Nullable, so equality is not enough: null = null is null, which
		          -- would close and reopen every operator of a group-less module on
		          -- every rebuild.
		          and s.group_id is not distinct from v.group_id
		          and s.total_withdrawn_keys is not distinct from v.total_withdrawn_keys)`, module.ID, block)
	if err != nil {
		return 0, fmt.Errorf("close superseded operators: %w", err)
	}

	opened, err := tx.Exec(ctx, `
		insert into operator_version
			(module_id, operator_index, active, name, reward_address, staking_limit, stopped_validators,
			 total_signing_keys, used_signing_keys, depositable_validators_count, group_id, total_withdrawn_keys,
			 from_block, to_block)
		select $1, s.operator_index, s.active, s.name, s.reward_address, s.staking_limit, s.stopped_validators,
		       s.total_signing_keys, s.used_signing_keys, s.depositable_validators_count, s.group_id,
		       s.total_withdrawn_keys, $2, null
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

	// last_changed_block_hash is not compared: it derives from the comparison, so every re-read
	// would look like a change.
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
		// Nothing moved: keep the previous row and its stamp (the restart re-read case).
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

// Publish moves the version-level lastChangedBlockHash only when some module's content moved.
func (b *pgBuild) Publish(ctx context.Context, label string, finalized int64) error {
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

	// The previous head stays published and pinnable; only its staleness is cleared. Superseded is
	// reserved for reorged versions, whose pins need 409.
	if _, err := tx.Exec(ctx,
		`update versions set status = 'published' where status = 'stale' and id <> $1`, b.id); err != nil {
		return fmt.Errorf("clear staleness on the previous head: %w", err)
	}

	if _, err := tx.Exec(ctx, `update versions
		   set status = 'published', label = $2, last_changed_block_hash = $3, module_nonces = $4,
		       finalized_block_number = $5
		 where id = $1`, b.id, label, lastChanged, nonces, finalized); err != nil {
		return fmt.Errorf("publish version: %w", err)
	}

	return tx.Commit(ctx)
}

// Abort leaves rows for retention: deleting them would be a second long write on a failing path.
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
