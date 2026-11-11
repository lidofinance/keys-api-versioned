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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/ethaddr"
	"github.com/lidofinance/keys-api-versioned/internal/model"
)

// App reads the schema this service owns and its ingest fills: versions in
// versions, and modules, operators and keys as validity ranges keyed by a
// two-byte module id, with pubkeys and signatures stored as bytes.
//
// A read resolves its pin once and then issues ordinary short queries against rows
// that never change. There is no long transaction and nothing is rewritten
// underneath: that is what makes a dump cost the same whether or not a version is
// being written at the time.
type App struct {
	pool *pgxpool.Pool
	pin  string
	// Log receives debug lines: which version a request resolved to, and how many
	// rows each module's query returned. Optional.
	Log *slog.Logger
}

// logger returns a usable logger whether or not one was set.
func (a *App) logger() *slog.Logger {
	if a.Log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return a.Log
}

// NewApp connects to the app schema with a default pin ("latest", "finalized", a block
// number or a block hash).
func NewApp(ctx context.Context, dsn string, maxConns int32, pin string) (*App, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
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

// NewAppWithPool builds a store over an existing pool, so the ingest and the
// server in one process share connections.
func NewAppWithPool(pool *pgxpool.Pool, pin string) *App {
	return &App{pool: pool, pin: defaultPin(pin)}
}

func defaultPin(pin string) string {
	if pin == "" {
		return "latest"
	}
	return pin
}

// Close is a no-op when the pool is shared; the owner closes it.
func (a *App) Close() { a.pool.Close() }

// Snapshot resolves the pin once and serves the whole request from it.
func (a *App) Snapshot(ctx context.Context, fn func(Snapshot) error) error {
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	meta, versionID, block, status, err := resolveAppPin(ctx, conn, a.pin)
	if err != nil {
		return err
	}
	if meta != nil {
		// Hold a lease on the version for as long as the response takes. A concurrent
		// publish is already harmless — the range predicate below decides what this
		// request sees, whatever the writer does — but retention *deletes*, and a dump
		// of 211 MiB to a slow consumer can outlive the retention window. Without this
		// the rows would go while the stream was still using them.
		release, err := leaseVersion(ctx, conn, block)
		if err != nil {
			return err
		}
		defer release()
	}
	if meta != nil {
		a.logger().Debug("snapshot resolved", "pin", a.pin, "version_block", block,
			"confirmed_at", meta.BlockNumber, "status", status,
			"last_changed", meta.LastChangedBlockHash)
	}
	return fn(&appSnapshot{conn: conn, meta: meta, versionID: versionID, block: block,
		stale: status == StatusStale, log: a.logger()})
}

// PinnedSnapshot serves one request at an explicit pin, which is what a
// `?snapshot=` query parameter resolves to.
func (a *App) PinnedSnapshot(ctx context.Context, pin string, fn func(Snapshot) error) error {
	return (&App{pool: a.pool, pin: defaultPin(pin), Log: a.Log}).Snapshot(ctx, fn)
}

// The pin resolves on block_number — the version's immutable identity — while the
// meta a consumer sees carries the observed head, which is what the canonical
// contract reports and what advances on a quiet cycle.
const appVersionCols = `id, block_number, coalesce(nullif(head_block_number, 0), block_number),
	coalesce(nullif(head_block_hash, ''), block_hash),
	coalesce(nullif(head_timestamp, 0), timestamp),
	last_changed_block_hash, status`

// resolveAppPin turns a pin into a published version, and reports the status it
// landed on so the API can tell a client that what it is reading is known stale.
//
// A miss has to be distinguishable: taken off the chain by a reorg (superseded),
// or fallen out of the retention window (pruned). Labels always resolve to the
// newest version, so they hit neither.
func resolveAppPin(ctx context.Context, conn pinReader, pin string) (*model.ELBlockSnapshot, int64, int64, string, error) {
	var (
		q   string
		arg []any
	)
	switch {
	case pin == "latest":
		// The newest version that is published or stale. A stale head is still
		// served — its rows are internally consistent and a known-stale answer beats
		// an error — with the staleness travelling to the client in a header.
		q = `select ` + appVersionCols + ` from versions
		     where status in ('` + StatusPublished + `', '` + StatusStale + `')
		     order by id desc limit 1`
	case pin == "finalized":
		// The newest published version at or below the finalized block.
		//
		// Resolved by predicate rather than by a label, and that is the difference
		// between a pin that works and one that does not. A label has to be moved by
		// the writer as finality advances — an extra write every cycle and a race over
		// which row currently holds it. Before this the pin was called `safe`, the query
		// was `label = 'safe'`, and nothing ever set the label, so a documented pin
		// answered 425 "not ready" forever.
		//
		// It is `finalized` and not `safe` because those are different blocks in the
		// execution layer's own vocabulary: `safe` is the justified checkpoint and
		// `finalized` is the finalized one. Naming this after the wrong tag would tell a
		// consumer something untrue about what it is holding.
		//
		// What a consumer gets for it: a version a reorg cannot take away, by
		// definition of finality. `finalized` therefore never answers 409, which is the
		// property the pin exists for — `latest` can and does.
		//
		// The finalized block is read from the newest version's observation of it, not
		// from a chain call: the store is what the reader has, and a read replica has no
		// node of its own.
		q = `select ` + appVersionCols + ` from versions
		     where status in ('` + StatusPublished + `', '` + StatusStale + `')
		       and block_number <= (select max(finalized_block_number) from versions)
		     order by id desc limit 1`
	case strings.HasPrefix(pin, "0x"):
		// Exact version. The only pin that survives a reorg unambiguously: a height
		// alone cannot say which branch was meant.
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
	if err != nil {
		return nil, 0, 0, "", fmt.Errorf("resolve pin: %w", err)
	}

	switch status {
	case StatusPublished, StatusStale:
	case StatusBuilding:
		// The writer is still filling this version: not late, not gone, simply not
		// a version yet.
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

// classifyMissingFinalizedPin tells "this source has no finality" apart from "not yet".
//
// They need different answers. A store fed from a file, a devnet before its first
// finalization, or a node without the tag will never produce a safe version, and
// answering 425 "too early" would have a consumer retry forever. A store whose versions
// are simply all newer than the finalized block genuinely is too early — finality catches
// up on its own.
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
	// Nothing published, or nothing final yet. Both are the 425 case.
	return nil
}

func classifyMissingAppPin(ctx context.Context, conn pinReader, pin string) error {
	var oldest int64
	if err := conn.QueryRow(ctx, `select coalesce(min(block_number), 0) from versions`).Scan(&oldest); err != nil {
		return fmt.Errorf("retention window: %w", err)
	}

	if strings.HasPrefix(pin, "0x") {
		// A hash we do not hold. It was either pruned or never seen, and the two
		// are worth telling apart: 410 says "it existed", 404 says "it did not".
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

// appSnapshot serves one request from one resolved version.
type appSnapshot struct {
	conn *pgxpool.Conn
	meta *model.ELBlockSnapshot
	// block is the version's own block number — its identity, not the observed head.
	// Every row predicate uses it, and it is what a cursor names.
	block int64
	// versionID names the version row itself, which the status re-check needs and the
	// row predicates do not. A block number is not unique across a reorg: the version
	// the losing branch published and the one the winning branch publishes after the
	// rollback sit at the same height, and telling them apart by height alone reads a
	// perfectly current version as superseded — or, worse, the other way round.
	versionID int64
	stale     bool

	// modules is the version's module set, read once: four rows that every other
	// query needs, for the address-to-id mapping among other things.
	modules []model.StakingModule
	idByHex map[string]int16
	log     *slog.Logger
}

// debug logs when a logger was provided.
func (s *appSnapshot) debug(msg string, args ...any) {
	if s.log != nil {
		s.log.Debug(msg, args...)
	}
}

// Stale reports that the served version is known not to reflect the chain. The API
// surfaces it as a header, so every JSON body stays byte-identical to the canonical
// contract.
func (s *appSnapshot) Stale() bool { return s.stale }

// PinBlock is the version's identity, which is what a paging cursor has to carry:
// the observed head in the meta moves on every quiet cycle, this does not.
func (s *appSnapshot) PinBlock() int64 { return s.block }

func (s *appSnapshot) ELMeta(context.Context) (*model.ELBlockSnapshot, error) {
	return s.meta, nil
}

// stillOnChain re-checks the pinned version before a query that continues a response
// already in progress.
//
// It exists because of an asymmetry between the two ways rows disappear. Retention is
// held off by the lease this snapshot took, so it cannot delete underneath a reader. A
// reorg rollback must delete: the losing branch's rows have to stop being current, or
// the next version would inherit its keys. And a response is not one query — a dump
// runs one per module, each in its own implicit transaction with its own snapshot. So
// a rollback committing between two of them leaves the remaining modules returning
// nothing, and without this check the handler would finish the JSON and hand the
// consumer a complete-looking answer assembled from a branch that no longer exists.
// That is the exact failure this design is meant to rule out, so the cost — one
// single-row select per module, four on a mainnet dump — is worth paying.
func (s *appSnapshot) stillOnChain(ctx context.Context) error {
	// By id, not by block number. Two versions share a height whenever a reorg is
	// followed by a rebuild — the superseded one from the losing branch and the new one
	// from the winning branch — and a lookup by height then picks between them
	// arbitrarily.
	var status string
	err := s.conn.QueryRow(ctx,
		`select status from versions where id = $1`, s.versionID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		// Gone entirely. The lease should have prevented retention from doing this, so
		// reaching here means the version was dropped some other way; either way the
		// honest answer is that what the consumer pinned is no longer available.
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

// loadModules reads the version's modules once and caches them on the snapshot.
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

// ModuleByRef resolves a module by numeric id or by contract address.
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

	// A reference that is neither an id nor an address is a malformed request, not a
	// module that happens to be missing — the same distinction the canonical service
	// makes, and it answers 400 for it.
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

// looksLikeAddress reports whether a reference has the shape of an address: 0x and forty
// hex digits. Only the shape — whether such a module exists is the caller's question.
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

// moduleID maps a lowercase address to the id the key and operator rows carry.
func (s *appSnapshot) moduleID(ctx context.Context, address string) (int16, bool, error) {
	if err := s.loadModules(ctx); err != nil {
		return 0, false, err
	}
	id, ok := s.idByHex[strings.ToLower(address)]
	return id, ok, nil
}

// keysAtPin is the only read predicate, for every pin including the head.
//
// The tempting optimisation — serving the head from `to_block is null`
// against a partial index — is wrong, and measurably so. A dump issues one query
// per module, so publishing a version between two of those queries moves the set of
// open rows and the response gets stitched together from two versions at a module
// boundary. The range predicate has no such window: closing a row sets to_block
// to the newer version's block, so the row stays visible to a read pinned at an
// earlier one.
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

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("stream keys: %w", err)
	}
	defer rows.Close()

	// The binary fields travel to the renderer as bytes and are hex-encoded
	// straight into the response buffer, so no string is allocated per key.
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

	// Pubkeys arrive as hex from the request and are compared as bytes, so the
	// comparison has one representation and needs no lower-casing.
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
		// This result is marshalled with encoding/json rather than streamed, so it
		// needs the hex form present. One rule, one helper.
		k.EnsureHex()
		out = append(out, k)
	}
	return out, rows.Err()
}

const appOperatorCols = `operator_index, active, name, reward_address, staking_limit,
	stopped_validators, total_signing_keys, used_signing_keys, depositable_validators_count, group_id,
	total_withdrawn_keys`

// Operators returns a module's operators at the pinned version.
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
		// A null column is a module without groups, and it stays nil all the way to
		// the renderer, which then omits the field rather than serving a number.
		if group != nil {
			id := model.GroupID(*group)
			o.Group = &id
		}
		o.RewardAddress = ethaddr.Checksum(model.EncodeHex(reward))
		o.ModuleAddress = checksummed
		out = append(out, o)
	}
	return out, rows.Err()
}

var (
	_ Store    = (*App)(nil)
	_ Snapshot = (*appSnapshot)(nil)
)
