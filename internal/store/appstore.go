package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/ethaddr"
	"github.com/lidofinance/keys-api-versioned/internal/logmask"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// App reads the service's own versioned schema. A read resolves its pin once, then runs short
// queries against rows that never change: no long transaction.
type App struct {
	pool *pgxpool.Pool
	pin  string
	Log  *slog.Logger
	// AcquireTimeout: zero means acquireTimeout.
	AcquireTimeout time.Duration
}

func (a *App) logger() *slog.Logger {
	if a.Log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return a.Log
}

// NewApp's pin is "latest", "finalized", a block number or a block hash.
func NewApp(ctx context.Context, dsn string, maxConns int32, pin string) (*App, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", logmask.DSNParseError(err))
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &App{pool: pool, pin: defaultPin(pin)}, nil
}

func NewAppWithPool(pool *pgxpool.Pool, pin string) *App {
	return &App{pool: pool, pin: defaultPin(pin)}
}

func defaultPin(pin string) string {
	if pin == "" {
		return "latest"
	}
	return pin
}

func (a *App) Close() { a.pool.Close() }

// acquireTimeout: with the pool exhausted a request fails instead of queueing for as long as its
// client waits.
const acquireTimeout = 10 * time.Second

func (a *App) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	timeout := a.AcquireTimeout
	if timeout <= 0 {
		timeout = acquireTimeout
	}
	acquireCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := a.pool.Acquire(acquireCtx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	return conn, nil
}

func (a *App) Snapshot(ctx context.Context, fn func(Snapshot) error) error {
	conn, err := a.acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	meta, versionID, block, status, err := resolveAppPin(ctx, conn, a.pin)
	if err != nil {
		return err
	}
	if meta != nil {
		// Lease the version for the whole response: retention deletes, and a 211 MiB dump to a slow
		// consumer can outlive the retention window.
		release, err := LeaseVersion(ctx, conn, block)
		if err != nil {
			return err
		}
		defer release()
	}
	snap := &appSnapshot{conn: conn, meta: meta, versionID: versionID, block: block,
		stale: status == StatusStale, log: a.logger()}
	if meta != nil {
		// Retention claims a version's lease before deleting it, so a pin resolved just before a prune
		// is caught here, after the lease, rather than by a short response.
		if err := snap.stillOnChain(ctx); err != nil {
			return err
		}
		a.logger().Debug("snapshot resolved", "pin", a.pin, "version_block", block,
			"confirmed_at", meta.BlockNumber, "status", status,
			"last_changed", meta.LastChangedBlockHash)
	}
	if err := fn(snap); err != nil {
		return err
	}
	// Reads run outside a transaction: a rollback committed after the last check reopens rows the
	// last statement then saw under this version's label. One primary-key read closes that window.
	if meta != nil {
		return snap.stillOnChain(ctx)
	}
	return nil
}

func (a *App) PinnedSnapshot(ctx context.Context, pin string, fn func(Snapshot) error) error {
	return (&App{pool: a.pool, pin: defaultPin(pin), Log: a.Log, AcquireTimeout: a.AcquireTimeout}).Snapshot(ctx, fn)
}

// Pins resolve on block_number (the version's identity); the meta shows the observed head, which
// advances on a quiet cycle.
const appVersionCols = `id, block_number, coalesce(nullif(head_block_number, 0), block_number),
	coalesce(nullif(head_block_hash, ''), block_hash),
	coalesce(nullif(head_timestamp, 0), timestamp),
	last_changed_block_hash, status`

// resolveAppPin reports the status too, so the API can flag a known-stale read.
func resolveAppPin(ctx context.Context, conn pinReader, pin string) (*model.ELBlockSnapshot, int64, int64, string, error) {
	var (
		q   string
		arg []any
	)
	switch {
	case pin == "latest":
		q = `select ` + appVersionCols + ` from versions
		     where status in ('` + StatusPublished + `', '` + StatusStale + `')
		     order by id desc limit 1`
	case pin == "finalized":
		// By predicate, not a label a writer must move (the old `safe` label was never set and answered
		// 425 forever). Never answers 409; the finalized block comes from versions, not a node.
		q = `select ` + appVersionCols + ` from versions
		     where status in ('` + StatusPublished + `', '` + StatusStale + `')
		       and block_number <= (select max(finalized_block_number) from versions)
		     order by id desc limit 1`
	case strings.HasPrefix(pin, "0x"):
		// The only pin that survives a reorg unambiguously: a height cannot name the branch.
		q = `select ` + appVersionCols + ` from versions where block_hash = $1`
		arg = append(arg, strings.ToLower(pin))
	default:
		n, err := strconv.ParseInt(pin, 10, 64)
		if err != nil {
			return nil, 0, 0, "", ErrBadRequest{
				What: "snapshot must be latest, finalized, a block number or a block hash",
				Ref:  pin,
			}
		}
		q = `select ` + appVersionCols + ` from versions where block_number = $1
		     order by (status = '` + StatusPublished + `') desc, id desc limit 1`
		arg = append(arg, n)
	}

	var (
		m         model.ELBlockSnapshot
		versionID int64
		pinBlock  int64
		status    string
	)
	err := conn.QueryRow(ctx, q, arg...).Scan(&versionID, &pinBlock, &m.BlockNumber, &m.BlockHash,
		&m.Timestamp, &m.LastChangedBlockHash, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		if pin == "finalized" {
			return nil, 0, 0, "", classifyMissingFinalizedPin(ctx, conn)
		}
		if pin == "latest" {
			// Nothing published yet: the canonical 425 case.
			return nil, 0, 0, "", nil
		}
		return nil, 0, 0, "", classifyMissingAppPin(ctx, conn, pin)
	}
	if missingSchema(err) {
		// Before the writer's first migrate, or while it rebuilds the schema: nothing is published.
		return nil, 0, 0, "", nil
	}
	if err != nil {
		return nil, 0, 0, "", fmt.Errorf("resolve pin: %w", err)
	}

	switch status {
	case StatusPublished, StatusStale:
	case StatusBuilding:
		return nil, 0, 0, "", nil
	default:
		head, lastChanged, err := appHead(ctx, conn)
		if err != nil {
			return nil, 0, 0, "", err
		}
		return nil, 0, 0, "", ErrSuperseded{Ref: pin, HeadBlockNumber: head, HeadLastChangedBlock: lastChanged}
	}

	return &m, versionID, pinBlock, status, nil
}

// classifyMissingFinalizedPin: a source with no finality gets 501, since 425 would make a
// consumer retry forever; versions merely newer than finality get 425.
func classifyMissingFinalizedPin(ctx context.Context, conn pinReader) error {
	var (
		versions  int64
		finalized int64
	)
	if err := conn.QueryRow(ctx,
		`select count(*), coalesce(max(finalized_block_number), 0) from versions`).
		Scan(&versions, &finalized); err != nil {
		return fmt.Errorf("classify finalized: %w", err)
	}
	if versions > 0 && finalized == 0 {
		return ErrNotImplemented{What: "?snapshot=finalized: this source reports no finality"}
	}
	return nil
}

func classifyMissingAppPin(ctx context.Context, conn pinReader, pin string) error {
	var oldest int64
	if err := conn.QueryRow(ctx, `select coalesce(min(block_number), 0) from versions`).Scan(&oldest); err != nil {
		return fmt.Errorf("retention window: %w", err)
	}

	if strings.HasPrefix(pin, "0x") {
		// 410 says "it existed", 404 says "it did not".
		var everSeen bool
		if err := conn.QueryRow(ctx,
			`select exists (select 1 from versions where block_hash = $1)`, strings.ToLower(pin)).Scan(&everSeen); err != nil {
			return fmt.Errorf("classify pin: %w", err)
		}
		if oldest > 0 {
			return ErrPruned{Ref: pin}
		}
		return ErrNotFound{Ref: pin, What: "snapshot"}
	}

	n, err := strconv.ParseInt(pin, 10, 64)
	if err != nil {
		return ErrBadRequest{
			What: "snapshot must be latest, finalized, a block number or a block hash",
			Ref:  pin,
		}
	}
	if oldest > 0 && n < oldest {
		return ErrPruned{Ref: pin}
	}
	return ErrNotFound{Ref: pin, What: "snapshot"}
}

func missingSchema(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "3F000")
}

func appHead(ctx context.Context, conn pinReader) (int64, string, error) {
	var (
		block       int64
		lastChanged string
	)
	err := conn.QueryRow(ctx, `select block_number, last_changed_block_hash from versions
		where status in ('`+StatusPublished+`', '`+StatusStale+`') order by id desc limit 1`).Scan(&block, &lastChanged)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("head: %w", err)
	}
	return block, lastChanged, nil
}

type appSnapshot struct {
	conn *pgxpool.Conn
	meta *model.ELBlockSnapshot
	// block is the version's identity, not the observed head; row predicates and cursors use it.
	block int64
	// versionID: a reorg plus rebuild puts two versions at one height, so the status re-check must
	// go by id.
	versionID int64
	// versionHash is the version's own block hash, set by stillOnChain; cursors carry it.
	versionHash string
	stale       bool

	modules []model.StakingModule
	idByHex map[string]int16
	log     *slog.Logger
}

func (s *appSnapshot) debug(msg string, args ...any) {
	if s.log != nil {
		s.log.Debug(msg, args...)
	}
}

// Stale is surfaced as a header so JSON bodies stay byte-identical to the canonical contract.
func (s *appSnapshot) Stale() bool { return s.stale }

// PinBlock is what a cursor carries: the meta's observed head moves every quiet cycle.
func (s *appSnapshot) PinBlock() int64 { return s.block }

func (s *appSnapshot) ELMeta(context.Context) (*model.ELBlockSnapshot, error) {
	return s.meta, nil
}

// stillOnChain: a dump is one query per module, so a reorg rollback between them would leave
// the rest empty and a complete-looking answer. One single-row select per module.
func (s *appSnapshot) stillOnChain(ctx context.Context) error {
	var status string
	err := s.conn.QueryRow(ctx,
		`select status, block_hash from versions where id = $1`, s.versionID).Scan(&status, &s.versionHash)
	if errors.Is(err, pgx.ErrNoRows) {
		// The lease should have stopped retention; whatever dropped it, the pin is gone.
		return ErrPruned{Ref: strconv.FormatInt(s.block, 10)}
	}
	if err != nil {
		return fmt.Errorf("re-check version %d: %w", s.block, err)
	}
	if status != StatusSuperseded {
		return nil
	}

	head, lastChanged, err := appHead(ctx, s.conn)
	if err != nil {
		return fmt.Errorf("head after a reorg: %w", err)
	}
	return ErrSuperseded{
		Ref:                  strconv.FormatInt(s.block, 10),
		HeadBlockNumber:      head,
		HeadLastChangedBlock: lastChanged,
	}
}

const appModuleCols = `module_id, address, type, name, nonce, module_fee, treasury_fee, target_share,
	status, last_deposit_at, last_deposit_block, exited_validators_count, active,
	withdrawal_credentials_type, last_changed_block_hash`

func (s *appSnapshot) loadModules(ctx context.Context) error {
	if s.modules != nil {
		return nil
	}

	rows, err := s.conn.Query(ctx, `select `+appModuleCols+` from module_version
		where from_block <= $1 and (to_block is null or to_block > $1)
		order by module_id`, s.block)
	if err != nil {
		return fmt.Errorf("modules: %w", err)
	}
	defer rows.Close()

	s.modules = []model.StakingModule{}
	s.idByHex = map[string]int16{}
	for rows.Next() {
		var (
			m       model.StakingModule
			id      int16
			address []byte
			nonce   int64
		)
		if err := rows.Scan(&id, &address, &m.Type, &m.Name, &nonce, &m.ModuleFee, &m.TreasuryFee,
			&m.TargetShare, &m.Status, &m.LastDepositAt, &m.LastDepositBlock,
			&m.ExitedValidatorsCount, &m.Active, &m.WithdrawalCredentialsType,
			&m.LastChangedBlockHash); err != nil {
			return fmt.Errorf("scan module: %w", err)
		}

		raw := model.EncodeHex(address)
		m.ID = int32(id)
		m.Nonce = int32(nonce)
		m.StakingModuleAddress = ethaddr.Checksum(raw)
		m.SetRawAddress(raw)

		s.modules = append(s.modules, m)
		s.idByHex[raw] = id
	}
	return rows.Err()
}

func (s *appSnapshot) Modules(ctx context.Context, addresses []string) ([]model.StakingModule, error) {
	if err := s.loadModules(ctx); err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return s.modules, nil
	}

	wanted := make(map[string]bool, len(addresses))
	for _, a := range addresses {
		wanted[strings.ToLower(a)] = true
	}
	out := make([]model.StakingModule, 0, len(addresses))
	for _, m := range s.modules {
		if wanted[m.RawAddress()] {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *appSnapshot) ModuleByRef(ctx context.Context, ref string) (*model.StakingModule, error) {
	if err := s.loadModules(ctx); err != nil {
		return nil, err
	}

	if id, err := strconv.Atoi(ref); err == nil && id >= 0 {
		for i := range s.modules {
			if int(s.modules[i].ID) == id {
				return &s.modules[i], nil
			}
		}
		return nil, ErrNotFound{Ref: ref}
	}

	// Neither id nor address: 400, as the canonical service answers.
	if !looksLikeAddress(ref) {
		return nil, ErrBadRequest{
			What: "module_id must be a contract address or numeric value",
			Ref:  ref,
		}
	}

	lower := strings.ToLower(ref)
	for i := range s.modules {
		if s.modules[i].RawAddress() == lower {
			return &s.modules[i], nil
		}
	}
	return nil, ErrNotFound{Ref: ref}
}

func looksLikeAddress(ref string) bool {
	if len(ref) != 42 || !strings.HasPrefix(strings.ToLower(ref), "0x") {
		return false
	}
	for _, c := range ref[2:] {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

func (s *appSnapshot) moduleID(ctx context.Context, address string) (int16, bool, error) {
	if err := s.loadModules(ctx); err != nil {
		return 0, false, err
	}
	id, ok := s.idByHex[strings.ToLower(address)]
	return id, ok, nil
}

// keysAtPin is the only read predicate, head included. Serving the head from `to_block is null`
// would stitch a dump from two versions if one is published between per-module queries.
const keysAtPin = `select key_index, operator_index, pubkey, signature, used, vetted
	from key_version
	where module_id = $1
	  and from_block <= $2
	  and (to_block is null or to_block > $2)`

func (s *appSnapshot) StreamKeys(ctx context.Context, moduleAddress string, f model.KeysFilter, fn func(*model.Key) error) error {
	id, ok, err := s.moduleID(ctx, moduleAddress)
	if err != nil || !ok {
		return err
	}
	if err := s.stillOnChain(ctx); err != nil {
		return err
	}

	q := keysAtPin
	args := []any{id, s.block}
	if f.Used != nil {
		args = append(args, *f.Used)
		q += ` and used = $` + strconv.Itoa(len(args))
	}
	if f.OperatorIndex != nil {
		args = append(args, *f.OperatorIndex)
		q += ` and operator_index = $` + strconv.Itoa(len(args))
	}
	// Canonical's order; heap order reshuffles once a module is rewritten. Planned per call: the
	// cached generic plan sorted module 1 on disk (77 MB, +100 ms a dump) instead of using kv_ident.
	q += ` order by operator_index, key_index`

	rows, err := s.conn.Query(ctx, q, append([]any{pgx.QueryExecModeCacheDescribe}, args...)...)
	if err != nil {
		return fmt.Errorf("stream keys: %w", err)
	}
	defer rows.Close()

	started := time.Now()
	var streamed int64

	k := model.Key{ModuleAddress: ethaddr.Checksum(strings.ToLower(moduleAddress))}
	for rows.Next() {
		if err := rows.Scan(&k.Index, &k.OperatorIndex, &k.PubkeyBin, &k.SignatureBin, &k.Used, &k.Vetted); err != nil {
			return fmt.Errorf("scan key: %w", err)
		}
		if err := fn(&k); err != nil {
			return err
		}
		streamed++
	}
	if err := rows.Err(); err != nil {
		return err
	}

	s.debug("keys streamed", "module_id", id, "version_block", s.block, "keys", streamed,
		"took", time.Since(started).Round(time.Millisecond).String())
	return nil
}

func (s *appSnapshot) KeysByPubkeys(ctx context.Context, moduleAddress string, pubkeys []string) ([]model.Key, error) {
	id, ok, err := s.moduleID(ctx, moduleAddress)
	if err != nil || !ok {
		return nil, err
	}

	raw := make([][]byte, 0, len(pubkeys))
	for _, p := range pubkeys {
		b, err := model.DecodeHex(p)
		if err != nil {
			continue
		}
		raw = append(raw, b)
	}

	rows, err := s.conn.Query(ctx, keysAtPin+` and pubkey = any($3)`, id, s.block, raw)
	if err != nil {
		return nil, fmt.Errorf("keys by pubkeys: %w", err)
	}
	defer rows.Close()

	checksummed := ethaddr.Checksum(strings.ToLower(moduleAddress))
	var out []model.Key
	for rows.Next() {
		k := model.Key{ModuleAddress: checksummed}
		if err := rows.Scan(&k.Index, &k.OperatorIndex, &k.PubkeyBin, &k.SignatureBin, &k.Used, &k.Vetted); err != nil {
			return nil, err
		}
		// Marshalled with encoding/json, not streamed, so it needs the hex form.
		k.EnsureHex()
		out = append(out, k)
	}
	return out, rows.Err()
}

const appOperatorCols = `operator_index, active, name, reward_address, staking_limit,
	stopped_validators, total_signing_keys, used_signing_keys, depositable_validators_count, group_id,
	total_withdrawn_keys`

func (s *appSnapshot) Operators(ctx context.Context, moduleAddress string, f model.OperatorsFilter) ([]model.Operator, error) {
	id, ok, err := s.moduleID(ctx, moduleAddress)
	if err != nil || !ok {
		return nil, err
	}

	q := `select ` + appOperatorCols + ` from operator_version
		where module_id = $1
		  and from_block <= $2
		  and (to_block is null or to_block > $2)`
	args := []any{id, s.block}
	if f.Index != nil {
		args = append(args, *f.Index)
		q += ` and operator_index = $` + strconv.Itoa(len(args))
	}
	q += ` order by operator_index`

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("operators: %w", err)
	}
	defer rows.Close()

	checksummed := ethaddr.Checksum(strings.ToLower(moduleAddress))
	var out []model.Operator
	for rows.Next() {
		var (
			o      model.Operator
			reward []byte
			group  *int32
		)
		if err := rows.Scan(&o.Index, &o.Active, &o.Name, &reward, &o.StakingLimit,
			&o.StoppedValidators, &o.TotalSigningKeys, &o.UsedSigningKeys,
			&o.DepositableValidatorsCount, &group, &o.TotalWithdrawnKeys); err != nil {
			return nil, fmt.Errorf("scan operator: %w", err)
		}
		// A null group stays nil to the renderer, which omits the field.
		if group != nil {
			id := model.GroupID(*group)
			o.Group = &id
		}
		o.RewardAddress = ethaddr.Checksum(model.EncodeHex(reward))
		o.ModuleAddress = checksummed
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Checked after, not before: operators are the last statement of the grouped stream, and a
	// rollback landing before it would pair pre-reorg keys with reopened operator rows.
	if err := s.stillOnChain(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

var (
	_ Store    = (*App)(nil)
	_ Snapshot = (*appSnapshot)(nil)
)
