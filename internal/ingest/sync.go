package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// Syncer keeps a store in step with a source, re-reading keys only for modules whose nonce moved.
type Syncer struct {
	source source.Provider
	store  Store
	cfg    SyncConfig
	log    *slog.Logger

	// scannedTo is the highest block the operator event scan found empty. Without it the scan window
	// grows from the version's block every cycle. Not persisted: a restart pays one wide scan.
	scannedTo int64

	// Zero means never, so the first cycle does both epoch-paced reads.
	lastScanAt      time.Time
	lastFinalizedAt time.Time
	// finalized is reused between epoch reads: a pin one epoch stale is still final.
	finalized model.ELBlockSnapshot

	now func() time.Time
	// wait sleeps between cycles; tests replace it to record the schedule.
	wait func(ctx context.Context, d time.Duration) error

	// hooks let the app observe cycles without this package importing metrics.
	OnCycle func(CycleResult)
	// OnModule fires once per module a rebuild read, before publish: per-module numbers exist only here.
	OnModule func(ModuleResult)
}

type ModuleNonce struct {
	ID      int32
	Address string
	Nonce   int32
}

// ModuleResult carries operators, not a count: the canonical service exports a gauge per operator.
type ModuleResult struct {
	ModuleID int32
	Address  string
	Nonce    int32
	KeysRead int64
	// KeysReread: an operators-only rebuild reports zero keys, which differs from a module with none.
	KeysReread bool
	// Duration is the key read alone (tens of seconds), as the canonical histogram measures.
	Duration  time.Duration
	Operators []model.Operator
}

type SyncConfig struct {
	Interval time.Duration
	// MaxBlockGap forces a full re-read when head runs this far ahead of the version's block: an
	// audit against a nonce that lied.
	MaxBlockGap int64
	// RetainVersions is the retention window. 0 disables pruning.
	RetainVersions int
	// EpochDuration paces the operator event scan and the finalized read. Configured, not fetched
	// (there is no consensus client); zero disables pacing.
	EpochDuration time.Duration
	// BuildAtFinalized reads the registry at the finalized block: no 409s, but data lags ~2 epochs
	// (61-85 blocks on Hoodi). Falls back to head when the source reports no finality.
	BuildAtFinalized bool
	ReorgSearchDepth int
}

type CycleResult struct {
	Action         Action
	Reason         string
	ChangedModules []string
	// Audit with KeysChanged > 0 means a module nonce missed a change.
	Audit            bool
	Modules          []ModuleNonce
	HeadBlock        int64
	VersionBlock     int64
	VersionTimestamp int64
	// HeadTimestamp is the health signal: it stops moving for any reason the loop stops working.
	HeadTimestamp  int64
	Rebuilt        bool
	KeysStaged     int64
	KeysChanged    int64
	FinalizedBlock int64
	// ReorgFrom is the rollback block (0 = none); a reorg does not imply a rebuild.
	ReorgFrom int64
	Duration  time.Duration
	Err       error
	Panicked  bool
}

func NewSyncer(src source.Provider, store Store, cfg SyncConfig, log *slog.Logger) *Syncer {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.MaxBlockGap <= 0 {
		// About three hours of blocks.
		cfg.MaxBlockGap = 1000
	}
	if cfg.ReorgSearchDepth <= 0 {
		cfg.ReorgSearchDepth = 20
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Syncer{source: src, store: store, cfg: cfg, log: log, now: time.Now, wait: sleep}
}

// Consecutive failed cycles back off exponentially from the interval up to maxRetryBackoff:
// against a metered provider a fixed-rate retry spends the quota it is failing on.
const maxRetryBackoff = 5 * time.Minute

// abortTimeout: an abort runs on a context of its own, so a shutdown still marks the row.
const abortTimeout = 10 * time.Second

func (s *Syncer) Run(ctx context.Context) error {
	failures := 0
	for {
		result := s.Cycle(ctx)
		failed := result.Err != nil && !errors.Is(result.Err, context.Canceled)
		if failed {
			failures++
			s.log.Error("sync cycle failed", "err", result.Err,
				"head", result.HeadBlock, "version", result.VersionBlock, "consecutive_failures", failures)
		} else {
			failures = 0
		}
		if s.OnCycle != nil {
			s.OnCycle(result)
		}

		delay := max(s.cfg.Interval-result.Duration, 0)
		if failures > 0 {
			delay = retryDelay(s.cfg.Interval, failures, rand.Float64())
		}
		if err := s.wait(ctx, delay); err != nil {
			return err
		}
	}
}

// retryDelay is interval·2^(failures-1), capped, then jittered into its upper half so replicas
// that failed together do not retry together.
func retryDelay(interval time.Duration, failures int, jitter float64) time.Duration {
	d := interval
	for i := 1; i < failures && d < maxRetryBackoff; i++ {
		d *= 2
	}
	d = min(d, maxRetryBackoff)
	return d/2 + time.Duration(jitter*float64(d/2))
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Named return, or the deferred duration is lost (every cycle reported zero). A panic becomes a
// failed cycle: in the combined mode the API shares this process.
func (s *Syncer) Cycle(ctx context.Context) (result CycleResult) {
	started := time.Now()
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("sync cycle panicked", "panic", r, "stack", string(debug.Stack()))
			result.Err = fmt.Errorf("sync cycle panicked: %v", r)
			result.Panicked = true
		}
		result.Duration = time.Since(started)
	}()

	head, err := s.source.Head(ctx)
	if err != nil {
		result.Err = fmt.Errorf("read head: %w", err)
		return result
	}
	result.HeadBlock = head.BlockNumber
	result.HeadTimestamp = head.Timestamp

	latest, err := s.store.Latest(ctx)
	if err != nil {
		result.Err = err
		return result
	}

	// Head unchanged, so nothing can have changed. Most 5s polls of a 12s chain end here.
	if latest != nil && head.BlockHash != "" && sameHash(head.BlockHash, latest.HeadBlockHash) {
		result.Action = ActionExtend
		result.Reason = fmt.Sprintf("head is still %d, nothing to confirm", head.BlockNumber)
		result.VersionBlock = latest.BlockNumber
		result.VersionTimestamp = latest.Timestamp
		result.FinalizedBlock = s.finalized.BlockNumber
		// Modules left empty: gauges keep their last value, and the version row is keyed by address, so
		// rebuilding ids from it would collapse every module onto one series.
		s.log.Debug("cycle: head unchanged, nothing to do",
			"head", head.BlockNumber, "hash", head.BlockHash, "version", latest.BlockNumber)
		return result
	}

	// Finality only chooses among built versions, never a read point: all reads stay pinned to one hash.
	final := s.finalizedBlock(ctx)
	finalized := final.BlockNumber
	result.FinalizedBlock = finalized

	readAt := head
	if s.cfg.BuildAtFinalized && final.BlockNumber > 0 {
		readAt = final
	}

	s.log.Debug("cycle: head", "block", head.BlockNumber, "hash", head.BlockHash,
		"timestamp", head.Timestamp, "finalized", finalized, "reading_at", readAt.BlockNumber)

	if latest == nil {
		result.Action = ActionRebuild
		result.Reason = "no published version"
		result.VersionTimestamp = head.Timestamp
		s.log.Info("cold start: no published version, reading every module", "head", head.BlockNumber)
		if err := s.rebuild(ctx, readAt, nil, DepthKeys, 0, true, finalized, &result); err != nil {
			result.Err = err
		} else {
			result.VersionBlock = head.BlockNumber
		}
		s.prune(ctx)
		return result
	}
	result.VersionBlock = latest.BlockNumber
	result.VersionTimestamp = latest.Timestamp

	// Head behind the published version: a rolled-back node or a lagging replica. Wait, don't undo it.
	if head.BlockNumber < latest.BlockNumber {
		result.Action = ActionExtend
		result.Reason = fmt.Sprintf("source head %d is behind the published version %d, waiting",
			head.BlockNumber, latest.BlockNumber)
		s.log.Warn("source head is behind the published version",
			"head", head.BlockNumber, "version", latest.BlockNumber)
		return result
	}

	// Modules are read before the reorg check: they decide whether a rollback is right at all.
	modules, err := s.source.Modules(ctx, source.Ref{BlockHash: readAt.BlockHash, BlockNumber: readAt.BlockNumber})
	if err != nil {
		result.Err = fmt.Errorf("read modules: %w", err)
		return result
	}
	result.Modules = moduleNonces(modules)

	// Probe skipped once the version's block is finalized. That is the common case: an unchanged
	// version sinks below finality within ~2 epochs.
	reorged, forkPoint := false, int64(0)
	if finalized > 0 && latest.BlockNumber <= finalized {
		s.log.Debug("reorg probe skipped: the version's block is finalized",
			"version", latest.BlockNumber, "finalized", finalized)
	} else {
		reorged, forkPoint, err = s.detectReorg(ctx, *latest)
		if err != nil {
			result.Err = err
			return result
		}
	}
	if reorged {
		// A reorg and a different chain look alike. A stored module set sharing no address with head's is
		// a different chain (e.g. a devnet reusing chain id 1): refuse rather than delete its rows.
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

		s.scannedTo = 0

		// Decide against the survivor: a one-block fork that changed nothing costs no key reads, where an
		// unconditional rebuild cost a full 512 000-key mainnet read.
		survivor, err := s.store.Latest(ctx)
		if err != nil {
			result.Err = err
			return result
		}
		if survivor == nil {
			result.Action = ActionSupersede
			result.Reason = fmt.Sprintf("reorg below the whole retained window (fork at %d): rebuilding from scratch", forkPoint)
			s.log.Warn("reorg left no version to build on, rebuilding from scratch", "fork_point", forkPoint)
			if err := s.rebuild(ctx, readAt, nil, DepthKeys, 0, true, finalized, &result); err != nil {
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
		BlockNumber:  readAt.BlockNumber,
		BlockHash:    readAt.BlockHash,
		ModuleNonces: nonceMap(modules),
	}
	// Asked after the module read (where a source notices it moved) and consumed, so one deploy causes
	// one rebuild.
	if reporter, ok := s.source.(source.SourceChangeReporter); ok {
		if reason, changed := reporter.SourceChanged(); changed {
			state.SourceChange = reason
		}
	}

	// The event scan runs only when the cheaper signals said nothing changed.
	decision := Evaluate(latest.AsVersion(), state, s.cfg.MaxBlockGap)
	if decision.Action == ActionExtend && !s.dueForEpochRead(s.lastScanAt) {
		s.log.Debug("operator event scan not due yet",
			"last_scan", s.lastScanAt, "epoch", s.cfg.EpochDuration.String())
	} else if decision.Action == ActionExtend {
		s.lastScanAt = s.now()
		from := s.scanFloor(latest.BlockNumber)
		state.OperatorEvents, err = s.operatorEvents(ctx, modules, from, head.BlockNumber)
		switch {
		case err != nil:
			s.log.Warn("operator event scan failed, assuming a change", "err", err)
			state.OperatorEvents = true
		case state.OperatorEvents:
			// The mark moves only after a rebuild that re-reads operators succeeds; advancing now would hide
			// the change until the periodic audit.
			s.log.Debug("operator events found, leaving the scan mark",
				"from", from+1, "to", head.BlockNumber, "mark", s.scannedTo)
		default:
			s.scannedTo = head.BlockNumber
		}
		decision = Evaluate(latest.AsVersion(), state, s.cfg.MaxBlockGap)
	}
	s.log.Debug("cycle: decision", "action", decision.Action, "reason", decision.Reason,
		"changed_modules", decision.ChangedModules, "version", latest.BlockNumber,
		"head", head.BlockNumber, "behind", head.BlockNumber-latest.BlockNumber,
		"operator_events", state.OperatorEvents, "nonces", state.ModuleNonces)
	result.Action = decision.Action
	result.Reason = decision.Reason
	result.ChangedModules = decision.ChangedModules
	result.Audit = decision.Audit
	if result.ReorgFrom > 0 {
		result.Reason = fmt.Sprintf("reorg rollback to %d, then %s", result.ReorgFrom, decision.Reason)
	}

	switch decision.Action {
	case ActionExtend:
		// Version block and timestamp stay on the version's own block: moving them to head would be
		// freshness by relabelling.
		if err := s.store.Extend(ctx, *latest, head, state.ModuleNonces, finalized); err != nil {
			result.Err = err
		} else {
			s.log.Debug("cycle: extended", "version", latest.BlockNumber,
				"confirmed_at", head.BlockNumber, "last_changed", latest.LastChangedBlockHash)
		}

	case ActionRebuild, ActionSupersede:
		s.log.Info("rebuild starting", "reason", decision.Reason, "reads", decision.Depth.String(),
			"changed_modules", decision.ChangedModules, "version", latest.BlockNumber, "head", head.BlockNumber)

		if err := s.store.MarkStale(ctx, *latest); err != nil {
			result.Err = err
			return result
		}
		if err := s.rebuild(ctx, readAt, decision.ChangedModules, decision.Depth, latest.BlockNumber, !decision.Audit, finalized, &result); err != nil {
			result.Err = err
			break
		}
		result.VersionBlock = head.BlockNumber
		result.VersionTimestamp = head.Timestamp

		// The audit is the only check that nonces move with keys: a healthy one writes nothing.
		if decision.Audit && result.KeysChanged > 0 {
			s.log.Error("the periodic audit changed key rows, so a module nonce did not report a change",
				"rows_changed", result.KeysChanged, "keys_read", result.KeysStaged,
				"version", latest.BlockNumber, "head", head.BlockNumber)
		}
	}

	s.prune(ctx)
	return result
}

// rebuild inherits modules outside scope (nil = all) at no cost: their rows simply stay open.
// published is the newest published block (0 = none); rows above it are an unfinished attempt's.
func (s *Syncer) rebuild(ctx context.Context, head model.ELBlockSnapshot, scope []string, depth ReadDepth, published int64, reuse bool, finalized int64, result *CycleResult) error {
	at := source.Ref{BlockHash: head.BlockHash, BlockNumber: head.BlockNumber}

	modules, err := s.source.Modules(ctx, at)
	if err != nil {
		return fmt.Errorf("read modules: %w", err)
	}
	if len(modules) == 0 {
		return errors.New("source returned no modules")
	}
	if len(result.Modules) == 0 {
		result.Modules = moduleNonces(modules)
	}

	leftovers, err := s.leftovers(ctx, published)
	if err != nil {
		return err
	}

	build, err := s.store.Begin(ctx, head)
	if errors.Is(err, ErrVersionExists) {
		var existing ExistingVersionError
		if errors.As(err, &existing) && !existing.Done() {
			return fmt.Errorf("block %d: %w; building again needs a new block", head.BlockNumber, err)
		}
		s.log.Debug("version already stored, skipping rebuild", "block", head.BlockNumber)
		return nil
	}
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
			defer cancel()
			build.Abort(abortCtx)
		}
	}()

	inScope := scopeSet(scope)
	for _, m := range modules {
		read := depth
		if inScope != nil && !inScope[m.RawAddress()] {
			read = DepthModule
		}
		keysKept := false
		if left, ok := leftovers[m.ID]; ok {
			switch {
			case reuse && left.reusable(m):
				// A failed attempt wrote this module's keys at a block still on chain, at today's
				// nonce: a cold mainnet retry skips 40 s of module 1 reads.
				read, keysKept = DepthOperators, true
				s.log.Info("module: keys kept from an unfinished attempt",
					"module", m.ID, "nonce", m.Nonce, "attempt_block", left.Block)
			default:
				// Rows of an attempt that cannot be trusted would be inherited as they are.
				read = DepthKeys
			}
		}
		if read == DepthModule {
			s.log.Debug("module: metadata only, contents inherited",
				"module", m.ID, "nonce", m.Nonce)
			if err := build.Module(ctx, m, false); err != nil {
				return err
			}
			continue
		}

		s.log.Info("module: reading", "module", m.ID, "type", m.Type,
			"nonce", m.Nonce, "address", m.RawAddress(), "reads", read.String())

		var (
			contentChanged bool
			diff           KeyDiff
			keysTook       time.Duration
		)
		if read.reads(DepthKeys) {
			moduleStarted := time.Now()
			diff, err = s.writeModuleKeys(ctx, build, at, m)
			if err != nil {
				return err
			}
			keysTook = time.Since(moduleStarted)
			result.KeysStaged += diff.Staged
			result.KeysChanged += diff.Opened + diff.Closed
			contentChanged = diff.Changed()
		}

		operatorsStarted := time.Now()
		operators, err := s.source.Operators(ctx, at, m)
		if err != nil {
			return fmt.Errorf("read operators of module %d: %w", m.ID, err)
		}
		changed, err := build.Operators(ctx, m, operators)
		if err != nil {
			return err
		}
		contentChanged = contentChanged || changed > 0 || (keysKept && (inScope == nil || inScope[m.RawAddress()]))

		s.log.Info("module written",
			"module", m.ID, "type", m.Type, "nonce", m.Nonce, "reads", read.String(),
			"keys_read", diff.Staged, "keys_opened", diff.Opened, "keys_closed", diff.Closed,
			"keys_unchanged", diff.Unchanged,
			"operators", len(operators), "operators_changed", changed,
			"keys_took", keysTook.Round(time.Millisecond).String(),
			"operators_took", time.Since(operatorsStarted).Round(time.Millisecond).String())

		if s.OnModule != nil {
			s.OnModule(ModuleResult{
				ModuleID: m.ID, Address: m.RawAddress(), Nonce: m.Nonce,
				KeysReread: read.reads(DepthKeys), KeysRead: diff.Staged,
				Duration: keysTook, Operators: operators,
			})
		}

		if err := build.Module(ctx, m, contentChanged); err != nil {
			return err
		}
	}

	publishStarted := time.Now()
	if err := build.Publish(ctx, "latest", finalized); err != nil {
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

func (s *Syncer) leftovers(ctx context.Context, published int64) (map[int32]Leftover, error) {
	reporter, ok := s.store.(LeftoverReporter)
	if !ok {
		return nil, nil
	}
	list, err := reporter.Leftovers(ctx, published)
	if err != nil {
		return nil, fmt.Errorf("rows of unfinished attempts: %w", err)
	}
	out := make(map[int32]Leftover, len(list))
	canonical := map[int64]bool{}
	for _, left := range list {
		if left.Complete {
			onChain, seen := canonical[left.Block]
			if !seen {
				hash, err := s.source.BlockHashAt(ctx, left.Block)
				if err != nil {
					return nil, fmt.Errorf("block hash at %d: %w", left.Block, err)
				}
				onChain = hash != "" && sameHash(hash, left.BlockHash)
				canonical[left.Block] = onChain
			}
			left.Canonical = onChain
		}
		out[left.ModuleID] = left
	}
	return out, nil
}

// writeModuleKeys: the diff runs in the database, so memory stays bounded for any module size.
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

func (s *Syncer) detectReorg(ctx context.Context, latest StoredVersion) (bool, int64, error) {
	onChain, err := s.source.BlockHashAt(ctx, latest.BlockNumber)
	if err != nil {
		return false, 0, fmt.Errorf("block hash at %d: %w", latest.BlockNumber, err)
	}
	if onChain == "" || sameHash(onChain, latest.BlockHash) {
		// Empty means the node lacks that height yet: a lagging endpoint, not a reorg.
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
			return true, v.BlockNumber + 1, nil
		}
	}

	oldest := latest.BlockNumber
	if len(versions) > 0 {
		oldest = versions[len(versions)-1].BlockNumber
	}
	s.log.Warn("reorg deeper than the retained window", "oldest_version", oldest)
	return true, oldest, nil
}

// foreignModuleSet: an empty side means an old version, not a different chain.
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

// finalizedBlock yields 0 without the capability; losing finality must not stop syncing.
func (s *Syncer) finalizedBlock(ctx context.Context) model.ELBlockSnapshot {
	reporter, ok := s.source.(source.FinalityReporter)
	if !ok {
		return model.ELBlockSnapshot{}
	}
	if !s.dueForEpochRead(s.lastFinalizedAt) {
		return s.finalized
	}
	block, err := reporter.FinalizedBlock(ctx)
	if err != nil {
		s.log.Warn("could not read the finalized block; keeping the previous one",
			"previous", s.finalized.BlockNumber, "err", err)
		return s.finalized
	}
	s.lastFinalizedAt = s.now()
	s.finalized = block
	return block
}

func (s *Syncer) dueForEpochRead(last time.Time) bool {
	if s.cfg.EpochDuration <= 0 || last.IsZero() {
		return true
	}
	return s.now().Sub(last) >= s.cfg.EpochDuration
}

func (s *Syncer) scanFloor(versionBlock int64) int64 {
	if s.scannedTo > versionBlock {
		return s.scannedTo
	}
	return versionBlock
}

// operatorEvents: a grouped watcher answers for all modules with one eth_getLogs.
func (s *Syncer) operatorEvents(ctx context.Context, modules []model.StakingModule, fromBlock, toBlock int64) (bool, error) {
	if fromBlock >= toBlock {
		return false, nil
	}
	if grouped, ok := s.source.(source.GroupedOperatorWatcher); ok {
		return grouped.OperatorsChangedAny(ctx, modules, fromBlock+1, toBlock)
	}

	watcher, ok := s.source.(source.OperatorWatcher)
	if !ok {
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
		// Warn: a slow consumer and a leaked lease look the same for one cycle; only repetition tells.
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

func moduleNonces(modules []model.StakingModule) []ModuleNonce {
	out := make([]ModuleNonce, 0, len(modules))
	for _, m := range modules {
		out = append(out, ModuleNonce{ID: m.ID, Address: m.RawAddress(), Nonce: m.Nonce})
	}
	return out
}

func nonceMap(modules []model.StakingModule) map[string]int64 {
	out := make(map[string]int64, len(modules))
	for _, m := range modules {
		out[m.RawAddress()] = int64(m.Nonce)
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
