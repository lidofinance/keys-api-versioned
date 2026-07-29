package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// Syncer keeps a store in step with a source.
//
// One cycle costs a handful of contract calls when nothing changed: a block
// header, the module set with its nonces, and one log filter per module. Keys are
// re-read only for modules whose nonce moved, which on mainnet is usually none and
// occasionally one — and since one module holds about 90 % of the keys, scoping the
// rebuild is worth roughly a tenth of the work in the common case.
type Syncer struct {
	source source.Provider
	store  Store
	cfg    SyncConfig
	log    *slog.Logger

	// hooks let the app observe cycles without this package importing metrics.
	OnCycle func(CycleResult)
}

// SyncConfig configures the loop.
type SyncConfig struct {
	// Interval between cycles.
	Interval time.Duration
	// MaxBlockGap forces a full re-read when head runs this far ahead of the
	// version's own block: an audit against a nonce that lied. See freshness.go for
	// why this is thousands of blocks here and thirty in the canonical service.
	MaxBlockGap int64
	// RetainVersions is the retention window. 0 disables pruning.
	RetainVersions int
	// SafeLagVersions is how far behind head the `safe` label sits. Reserved for
	// the label policy; the loop publishes `latest`.
	SafeLagVersions int
	// ReorgSearchDepth is how many stored versions the fork-point walk may inspect.
	ReorgSearchDepth int
}

// CycleResult is what one cycle did, for logs and metrics.
type CycleResult struct {
	Action         Action
	Reason         string
	ChangedModules []string
	HeadBlock      int64
	VersionBlock   int64
	// VersionTimestamp is the published version's own block time: how old the data
	// is. It legitimately climbs while the chain is quiet.
	VersionTimestamp int64
	// HeadTimestamp is the block time this cycle confirmed against. How long since
	// this last moved is the health signal, because it climbs for any reason the loop
	// stops working.
	HeadTimestamp int64
	Rebuilt       bool
	KeysStaged    int64
	KeysChanged   int64
	// ReorgFrom is the block a reorg rolled versions back to, 0 when there was none.
	// It is separate from Action because a reorg does not imply a rebuild: after the
	// rollback the cycle runs the ordinary incremental check, and on mainnet the usual
	// answer is that nothing inside the reorged range changed.
	ReorgFrom int64
	Duration  time.Duration
	Err       error
}

// NewSyncer builds a syncer.
func NewSyncer(src source.Provider, store Store, cfg SyncConfig, log *slog.Logger) *Syncer {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.MaxBlockGap <= 0 {
		// About three hours of blocks. Long enough that the audit is not the
		// dominant cost, short enough to catch a lying signal the same day.
		cfg.MaxBlockGap = 1000
	}
	if cfg.ReorgSearchDepth <= 0 {
		cfg.ReorgSearchDepth = 20
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Syncer{source: src, store: store, cfg: cfg, log: log}
}

// Run cycles until the context is cancelled. A failing cycle is logged and
// retried on the next tick: the published version keeps being served, which is the
// point of publishing versions rather than mutating a live table.
func (s *Syncer) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		result := s.Cycle(ctx)
		if result.Err != nil && !errors.Is(result.Err, context.Canceled) {
			s.log.Error("sync cycle failed", "err", result.Err,
				"head", result.HeadBlock, "version", result.VersionBlock)
		}
		if s.OnCycle != nil {
			s.OnCycle(result)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Cycle runs one polling cycle and applies its decision.
//
// The return value is named so the deferred duration lands in what the caller gets:
// with an unnamed return, `return result` copies the struct before the defer runs
// and every cycle reports zero.
func (s *Syncer) Cycle(ctx context.Context) (result CycleResult) {
	started := time.Now()
	defer func() { result.Duration = time.Since(started) }()

	head, err := s.source.Head(ctx)
	if err != nil {
		result.Err = fmt.Errorf("read head: %w", err)
		return result
	}
	result.HeadBlock = head.BlockNumber
	result.HeadTimestamp = head.Timestamp
	s.log.Debug("cycle: head", "block", head.BlockNumber, "hash", head.BlockHash, "timestamp", head.Timestamp)

	latest, err := s.store.Latest(ctx)
	if err != nil {
		result.Err = err
		return result
	}

	// Nothing published yet: build everything. This is the cold start, and it is
	// the only path that reads every module's keys unconditionally.
	if latest == nil {
		result.Action = ActionRebuild
		result.Reason = "no published version"
		result.VersionTimestamp = head.Timestamp
		// A cold start reads every module and takes minutes, so it says so up front
		// rather than after the first module lands.
		s.log.Info("cold start: no published version, reading every module", "head", head.BlockNumber)
		if err := s.rebuild(ctx, head, nil, &result); err != nil {
			result.Err = err
		} else {
			// Same as the ordinary rebuild path below. Without it a cold start
			// reports VersionBlock 0, so published_version_block and
			// blocks_behind_head stay unset for a cycle after the longest build the
			// service ever does.
			result.VersionBlock = head.BlockNumber
		}
		s.prune(ctx)
		return result
	}
	result.VersionBlock = latest.BlockNumber
	result.VersionTimestamp = latest.Timestamp

	// A head behind the published version means the source moved backwards: a node
	// rolled back, a load balancer handed us a lagging replica, or a restarted
	// source lost its place. None of those is a reason to publish a version that
	// undoes a newer one, so the cycle waits.
	if head.BlockNumber < latest.BlockNumber {
		result.Action = ActionExtend
		result.Reason = fmt.Sprintf("source head %d is behind the published version %d, waiting",
			head.BlockNumber, latest.BlockNumber)
		s.log.Warn("source head is behind the published version",
			"head", head.BlockNumber, "version", latest.BlockNumber)
		return result
	}

	// The cheap read: modules with their nonces. No keys.
	//
	// Read before the reorg check rather than after, because a rollback deletes rows and
	// the module set is what says whether a rollback is the right thing to do at all.
	modules, err := s.source.Modules(ctx, source.Ref{BlockHash: head.BlockHash, BlockNumber: head.BlockNumber})
	if err != nil {
		result.Err = fmt.Errorf("read modules: %w", err)
		return result
	}

	// Is the version still on the chain? A hash mismatch at its own height means
	// the branch it was built on is gone.
	reorged, forkPoint, err := s.detectReorg(ctx, *latest)
	if err != nil {
		result.Err = err
		return result
	}
	if reorged {
		// Every hash in the store disagreeing with the chain is what a reorg looks like,
		// and it is also what a *different chain* looks like — and the two want opposite
		// responses. A reorg wants the rollback below; a different chain wants a refusal,
		// because the rollback deletes rows that are not stale, they are someone else's.
		//
		// The module set tells them apart for nothing: a reorg cannot change which
		// contracts exist, so a stored module set with no address in common with head's is
		// not a fork of the same chain. `chain_<id>` schemas make the common form of this
		// mistake impossible, but a devnet reusing chain id 1 — which happens here — lands
		// in the same schema as mainnet with entirely different addresses, and this is what
		// catches that.
		if foreign, stored, current := foreignModuleSet(latest.ModuleNonces, modules); foreign {
			result.Err = fmt.Errorf("refusing to roll back: the store holds modules %v and the "+
				"chain reports %v, which share nothing — this is a different chain or deployment, "+
				"not a reorg. Check PROVIDERS_URLS and CHAIN_ID; the data is untouched",
				stored, current)
			return result
		}

		rolled, err := s.store.SupersedeFrom(ctx, forkPoint)
		if err != nil {
			result.Err = err
			return result
		}
		s.log.Warn("reorg rollback",
			"fork_point", forkPoint, "versions", rolled.Versions,
			"rows_dropped", rolled.Dropped, "rows_reopened", rolled.Reopened)
		result.ReorgFrom = forkPoint

		// What survived the rollback becomes the version this cycle decides against,
		// which is the whole point of holding versions: a reorg costs a rollback plus
		// the ordinary incremental check, and a fork that changed nothing costs no key
		// reads at all. Rebuilding unconditionally here — which is what this did — cost
		// a full 512 000-key read on mainnet for a one-block fork that changed nothing.
		survivor, err := s.store.Latest(ctx)
		if err != nil {
			result.Err = err
			return result
		}
		if survivor == nil {
			// Nothing left below the fork: the one case that does force a full
			// rebuild, and the reason the retention window should not be narrow.
			result.Action = ActionSupersede
			result.Reason = fmt.Sprintf("reorg below the whole retained window (fork at %d): rebuilding from scratch", forkPoint)
			s.log.Warn("reorg left no version to build on, rebuilding from scratch", "fork_point", forkPoint)
			if err := s.rebuild(ctx, head, nil, &result); err != nil {
				result.Err = err
			} else {
				result.VersionBlock = head.BlockNumber
				result.VersionTimestamp = head.Timestamp
			}
			s.prune(ctx)
			return result
		}
		latest = survivor
	}

	state := HeadState{
		BlockNumber:  head.BlockNumber,
		BlockHash:    head.BlockHash,
		ModuleNonces: nonceMap(modules),
	}
	state.OperatorEvents, err = s.operatorEvents(ctx, modules, latest.BlockNumber, head.BlockNumber)
	if err != nil {
		// A log filter failing is not a reason to stop syncing; it is a reason to
		// treat operator metadata as possibly stale, which is what a rebuild does.
		s.log.Warn("operator event scan failed, assuming a change", "err", err)
		state.OperatorEvents = true
	}

	decision := Evaluate(latest.AsVersion(), state, s.cfg.MaxBlockGap)
	s.log.Debug("cycle: decision", "action", decision.Action, "reason", decision.Reason,
		"changed_modules", decision.ChangedModules, "version", latest.BlockNumber,
		"head", head.BlockNumber, "behind", head.BlockNumber-latest.BlockNumber,
		"operator_events", state.OperatorEvents, "nonces", state.ModuleNonces)
	result.Action = decision.Action
	result.Reason = decision.Reason
	result.ChangedModules = decision.ChangedModules
	if result.ReorgFrom > 0 {
		// The action stays whatever the incremental check decided, because that is what
		// the cycle did. A reorg that ends in "extend" is the expected outcome on
		// mainnet, where forks are one or two blocks and Lido keys rarely change inside
		// them; ReorgFrom is what tells the metrics a reorg happened.
		result.Reason = fmt.Sprintf("reorg rollback to %d, then %s", result.ReorgFrom, decision.Reason)
	}

	switch decision.Action {
	case ActionExtend:
		// Nothing changed, so the version stays and only its observed block moves.
		// lastChangedBlockHash is untouched, which is what lets a consumer skip a
		// refetch.
		// VersionBlock and VersionTimestamp deliberately stay on the version's own
		// block: the data is still from there, and an alert on data age has to say so.
		// Moving them to head here would report a version confirmed-current at head as
		// if it had been rebuilt at head — freshness by relabelling.
		if err := s.store.Extend(ctx, *latest, head, state.ModuleNonces); err != nil {
			result.Err = err
		} else {
			s.log.Debug("cycle: extended", "version", latest.BlockNumber,
				"confirmed_at", head.BlockNumber, "last_changed", latest.LastChangedBlockHash)
		}

	case ActionRebuild, ActionSupersede:
		// A rebuild is the expensive path, so say why it started. Quiet cycles stay
		// silent — they are the common case and the metrics cover them.
		s.log.Info("rebuild starting", "reason", decision.Reason,
			"changed_modules", decision.ChangedModules, "version", latest.BlockNumber, "head", head.BlockNumber)

		// The version being replaced is marked stale first, so that if the rebuild
		// takes minutes — or fails — readers know what they are holding. It keeps
		// being served either way.
		if err := s.store.MarkStale(ctx, *latest); err != nil {
			result.Err = err
			return result
		}
		scope := decision.ChangedModules
		if state.OperatorEvents && len(scope) == 0 {
			// Operators moved with no nonce change: re-read them everywhere, but
			// leave keys alone. Passing an empty scope would mean "everything".
			scope = allModuleAddresses(modules)
		}
		if err := s.rebuild(ctx, head, scope, &result); err != nil {
			result.Err = err
			break
		}
		result.VersionBlock = head.BlockNumber
		result.VersionTimestamp = head.Timestamp
	}

	s.prune(ctx)
	return result
}

// rebuild writes a new version and publishes it.
//
// scope names the modules whose keys must be re-read; nil means all of them.
// Everything outside the scope is inherited from the previous version at no cost,
// because its rows are simply still open.
func (s *Syncer) rebuild(ctx context.Context, head model.ELBlockSnapshot, scope []string, result *CycleResult) error {
	at := source.Ref{BlockHash: head.BlockHash, BlockNumber: head.BlockNumber}

	modules, err := s.source.Modules(ctx, at)
	if err != nil {
		return fmt.Errorf("read modules: %w", err)
	}
	if len(modules) == 0 {
		return errors.New("source returned no modules")
	}

	build, err := s.store.Begin(ctx, head)
	if errors.Is(err, ErrVersionExists) {
		s.log.Debug("version already stored, skipping rebuild", "block", head.BlockNumber)
		return nil
	}
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			build.Abort(ctx)
		}
	}()

	inScope := scopeSet(scope)
	for _, m := range modules {
		contentChanged := false

		if inScope == nil || inScope[m.RawAddress()] {
			// Announced before the work, not after: reading one mainnet module takes
			// tens of seconds and this is the line that says which one is running.
			s.log.Info("module: reading", "module", m.ID, "type", m.Type,
				"nonce", m.Nonce, "address", m.RawAddress())

			moduleStarted := time.Now()
			diff, err := s.writeModuleKeys(ctx, build, at, m)
			if err != nil {
				return err
			}
			keysTook := time.Since(moduleStarted)
			result.KeysStaged += diff.Staged
			result.KeysChanged += diff.Opened + diff.Closed
			contentChanged = diff.Changed()

			operatorsStarted := time.Now()
			operators, err := s.source.Operators(ctx, at, m)
			if err != nil {
				return fmt.Errorf("read operators of module %d: %w", m.ID, err)
			}
			changed, err := build.Operators(ctx, m, operators)
			if err != nil {
				return err
			}
			contentChanged = contentChanged || changed > 0

			s.log.Info("module written",
				"module", m.ID, "type", m.Type, "nonce", m.Nonce,
				"keys_read", diff.Staged, "keys_opened", diff.Opened, "keys_closed", diff.Closed,
				"keys_unchanged", diff.Unchanged,
				"operators", len(operators), "operators_changed", changed,
				"keys_took", keysTook.Round(time.Millisecond).String(),
				"operators_took", time.Since(operatorsStarted).Round(time.Millisecond).String())
		} else {
			s.log.Debug("module: inherited unchanged", "module", m.ID, "nonce", m.Nonce)
		}

		if err := build.Module(ctx, m, contentChanged); err != nil {
			return err
		}
	}

	publishStarted := time.Now()
	if err := build.Publish(ctx, "latest"); err != nil {
		return err
	}
	committed = true
	result.Rebuilt = true
	s.log.Debug("publish: status flipped",
		"took", time.Since(publishStarted).Round(time.Millisecond).String())

	s.log.Info("version published",
		"block", head.BlockNumber, "modules", len(modules),
		"scoped_to", len(inScope), "keys_read", result.KeysStaged, "keys_changed", result.KeysChanged)
	return nil
}

// writeModuleKeys streams one module's keys from the source into the version.
//
// The keys never all exist at once: the source pushes them, the store stages them
// in batches, and the diff runs in the database. A module of half a million keys
// costs a bounded amount of memory on this path.
func (s *Syncer) writeModuleKeys(ctx context.Context, build Build, at source.Ref, m model.StakingModule) (KeyDiff, error) {
	writer, err := build.Keys(ctx, m)
	if err != nil {
		return KeyDiff{}, err
	}

	var (
		staged   int64
		started  = time.Now()
		lastLine = time.Now()
	)
	if err := s.source.Keys(ctx, at, m, func(k model.Key) error {
		staged++
		// A heartbeat every few seconds, so a module that takes two minutes is
		// visibly progressing rather than possibly stuck. Time-based rather than
		// count-based: a slow node is exactly the case that needs it.
		if time.Since(lastLine) > 5*time.Second {
			lastLine = time.Now()
			elapsed := time.Since(started)
			s.log.Debug("module: staging keys", "module", m.ID, "keys", staged,
				"elapsed", elapsed.Round(time.Second).String(),
				"keys/s", int(float64(staged)/elapsed.Seconds()))
		}
		return writer.Add(ctx, k)
	}); err != nil {
		writer.Abort(ctx)
		return KeyDiff{}, fmt.Errorf("read keys of module %d: %w", m.ID, err)
	}

	diff, err := writer.Commit(ctx)
	if err != nil {
		return KeyDiff{}, fmt.Errorf("write keys of module %d: %w", m.ID, err)
	}
	return diff, nil
}

// detectReorg checks whether the stored version is still on the chain, and if not,
// finds the newest stored version that is.
//
// This is the difference between a reorg costing a full resync and costing almost
// nothing: with versions, the fork point is discoverable from the store itself, and
// everything below it is untouched.
func (s *Syncer) detectReorg(ctx context.Context, latest StoredVersion) (bool, int64, error) {
	onChain, err := s.source.BlockHashAt(ctx, latest.BlockNumber)
	if err != nil {
		return false, 0, fmt.Errorf("block hash at %d: %w", latest.BlockNumber, err)
	}
	if onChain == "" || sameHash(onChain, latest.BlockHash) {
		// An empty answer means the node does not have that height yet, which is a
		// lagging endpoint rather than a reorg; the next cycle will settle it.
		return false, 0, nil
	}

	versions, err := s.store.Recent(ctx, s.cfg.ReorgSearchDepth)
	if err != nil {
		return true, latest.BlockNumber, err
	}

	for _, v := range versions {
		if v.BlockNumber >= latest.BlockNumber {
			continue
		}
		hash, err := s.source.BlockHashAt(ctx, v.BlockNumber)
		if err != nil {
			return true, latest.BlockNumber, err
		}
		if sameHash(hash, v.BlockHash) {
			// This version's block survived, so the fork is above it.
			return true, v.BlockNumber + 1, nil
		}
	}

	// The whole retained window is off the chain — the one case that does force a
	// full rebuild. Rolling back to the oldest version we hold makes that explicit
	// rather than leaving stale rows around.
	oldest := latest.BlockNumber
	if len(versions) > 0 {
		oldest = versions[len(versions)-1].BlockNumber
	}
	s.log.Warn("reorg deeper than the retained window", "oldest_version", oldest)
	return true, oldest, nil
}

// foreignModuleSet reports whether a stored module set and the chain's current one have
// no address in common, which no reorg can produce.
//
// Both sides have to be non-empty for the answer to mean anything: an empty stored set is
// a version written before this mattered, not evidence of a different chain, and refusing
// on it would strand a store that is merely old.
//
// The addresses come from the version row's module_nonces map, which is keyed by them, so
// this costs no query.
func foreignModuleSet(stored map[string]int64, current []model.StakingModule) (bool, []string, []string) {
	if len(stored) == 0 || len(current) == 0 {
		return false, nil, nil
	}

	live := make(map[string]bool, len(current))
	currentList := make([]string, 0, len(current))
	for _, m := range current {
		address := strings.ToLower(m.RawAddress())
		live[address] = true
		currentList = append(currentList, address)
	}

	storedList := make([]string, 0, len(stored))
	for address := range stored {
		storedList = append(storedList, address)
		if live[strings.ToLower(address)] {
			return false, nil, nil
		}
	}

	sort.Strings(storedList)
	sort.Strings(currentList)
	return true, storedList, currentList
}

// operatorEvents asks the source whether operator metadata moved in the range.
// A source that cannot answer cheaply does not implement the capability, and the
// signal is simply absent.
func (s *Syncer) operatorEvents(ctx context.Context, modules []model.StakingModule, fromBlock, toBlock int64) (bool, error) {
	watcher, ok := s.source.(source.OperatorWatcher)
	if !ok || fromBlock >= toBlock {
		return false, nil
	}
	for _, m := range modules {
		changed, err := watcher.OperatorsChanged(ctx, m, fromBlock+1, toBlock)
		if err != nil {
			return false, err
		}
		if changed {
			return true, nil
		}
	}
	return false, nil
}

// prune applies retention, logging rather than failing the cycle: retention
// falling behind is a storage problem, not a correctness one.
func (s *Syncer) prune(ctx context.Context) {
	if s.cfg.RetainVersions <= 0 {
		return
	}
	pruned, err := s.store.Prune(ctx, s.cfg.RetainVersions)
	if err != nil {
		s.log.Warn("prune failed", "err", err)
		return
	}
	if pruned.HeldAt > 0 {
		// Warn, not debug: the healthy case (a slow consumer streaming an ageing
		// version) and the unhealthy one (a leaked lease holding retention open
		// forever) look the same for one cycle and differ only in whether this keeps
		// appearing.
		s.log.Warn("retention held back by a reader still streaming an older version",
			"held_at_block", pruned.HeldAt, "oldest_kept_block", pruned.OldestKept)
	}
	if pruned.Versions == 0 && pruned.Rows == 0 {
		s.log.Debug("retention: nothing to drop", "keeping", s.cfg.RetainVersions,
			"oldest_kept_block", pruned.OldestKept)
	}
	if pruned.Versions > 0 || pruned.Rows > 0 {
		s.log.Info("retention applied",
			"keeping", s.cfg.RetainVersions, "oldest_kept_block", pruned.OldestKept,
			"versions_dropped", pruned.Versions, "rows_dropped", pruned.Rows)
	}
}

func nonceMap(modules []model.StakingModule) map[string]int64 {
	out := make(map[string]int64, len(modules))
	for _, m := range modules {
		out[m.RawAddress()] = int64(m.Nonce)
	}
	return out
}

func allModuleAddresses(modules []model.StakingModule) []string {
	out := make([]string, 0, len(modules))
	for _, m := range modules {
		out = append(out, m.RawAddress())
	}
	return out
}

// scopeSet turns a module list into a lookup, where nil means "every module".
func scopeSet(scope []string) map[string]bool {
	if len(scope) == 0 {
		return nil
	}
	set := make(map[string]bool, len(scope))
	for _, address := range scope {
		set[address] = true
	}
	return set
}

func sameHash(a, b string) bool {
	return len(a) == len(b) && equalFoldASCII(a, b)
}

func equalFoldASCII(a, b string) bool {
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
