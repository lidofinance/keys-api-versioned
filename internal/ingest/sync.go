package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"slices"
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

	// scannedTo is the block up to which every module's operators are known current: the scan found
	// nothing up to it, or a rebuild read every module's operators there. A rebuild that left some
	// modules' operators unread does not move it, so the next scan still covers its blocks. Not
	// persisted: a restart scans from the version's block.
	scannedTo int64
	// scannedHash is the hash at scannedTo: blocks above the version can be replaced by a reorg the
	// version survives, and an empty scan of them then vouches for nothing.
	scannedHash string

	// sourceChange is kept until a rebuild publishes: the source reports a change once, and a failed
	// rebuild must not lose it.
	sourceChange string
	// routerFields holds, per module address, the router fields of the published module row; nil
	// until loaded from the store or set by a rebuild.
	routerFields map[string]string

	// Zero means never, so the first cycle does both epoch-paced reads.
	lastScanAt      time.Time
	lastFinalizedAt time.Time
	// finalized is reused between epoch reads: a pin one epoch stale is still final.
	finalized model.ELBlockSnapshot
	// retentionHeldAt is the last prune's HeldAt, reported on every cycle.
	retentionHeldAt int64
	// confirmedHash: the version the last reorg probe found on chain; finalHash: one known on chain at or
	// below finality. In memory only, so a restart probes again.
	confirmedHash, finalHash string
	// auditedAt is the block of the last rebuild that read every key from the chain, loaded from the
	// published version on start.
	auditedAt int64
	// holdSettled: this process has decided whether readers wait for an overdue full read.
	holdSettled bool

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
	HeadTimestamp int64
	Rebuilt       bool
	KeysStaged    int64
	// KeysKept: deposited keys a rebuild left as they were without reading them.
	KeysKept       int64
	KeysChanged    int64
	FinalizedBlock int64
	// ReorgFrom is the rollback block (0 = none); a reorg does not imply a rebuild.
	ReorgFrom int64
	Duration  time.Duration
	Err       error
	Panicked  bool
	// RetentionHeldAt is the last prune's HeldAt (0 = not held). UnfinishedBuilds counts versions left
	// in building, -1 when the store cannot say.
	RetentionHeldAt  int64
	UnfinishedBuilds int64
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
		if fenced := (ErrFenced{}); errors.As(result.Err, &fenced) {
			// Another writer holds the lock: retrying here only fails again until the ping notices.
			return result.Err
		}
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
		result.RetentionHeldAt = s.retentionHeldAt
		result.UnfinishedBuilds = s.unfinishedBuilds(ctx)
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

	// Read point unchanged, so nothing can have changed. Most 5s polls of a 12s chain end here; at
	// finalized, every poll of an epoch does.
	unchanged := func(readAt model.ELBlockSnapshot) bool {
		if latest == nil || readAt.BlockHash == "" || !sameHash(readAt.BlockHash, latest.HeadBlockHash) {
			return false
		}
		result.Action = ActionExtend
		result.Reason = fmt.Sprintf("block %d is still the read point, nothing to confirm", readAt.BlockNumber)
		result.VersionBlock = latest.BlockNumber
		result.VersionTimestamp = latest.Timestamp
		result.FinalizedBlock = s.finalized.BlockNumber
		// Modules left empty: gauges keep their last value, and the version row is keyed by address, so
		// rebuilding ids from it would collapse every module onto one series.
		s.log.Debug("cycle: read point unchanged, nothing to do",
			"head", head.BlockNumber, "read_at", readAt.BlockNumber, "hash", readAt.BlockHash,
			"version", latest.BlockNumber)
		return true
	}
	if !s.cfg.BuildAtFinalized && unchanged(head) {
		return result
	}

	// Finality only chooses among built versions, never a read point: all reads stay pinned to one hash.
	final := s.finalizedBlock(ctx)
	finalized := final.BlockNumber
	result.FinalizedBlock = finalized

	readAt := head
	if s.cfg.BuildAtFinalized && final.BlockNumber > 0 {
		readAt = final
		// The version is confirmed at the block whose nonces were compared, not at a head nobody read:
		// otherwise a finality stall would look current to the confirmed-age alert.
		result.HeadTimestamp = readAt.Timestamp
	}
	if s.cfg.BuildAtFinalized && unchanged(readAt) {
		return result
	}

	s.log.Debug("cycle: head", "block", head.BlockNumber, "hash", head.BlockHash,
		"timestamp", head.Timestamp, "finalized", finalized, "reading_at", readAt.BlockNumber)

	if latest == nil {
		result.Action = ActionRebuild
		result.Reason = "no published version"
		result.VersionTimestamp = readAt.Timestamp
		s.log.Info("cold start: no published version, reading every module", "head", head.BlockNumber)
		if err := s.rebuild(ctx, readAt, nil, nil, DepthKeys, 0, true, finalized, &result); err != nil {
			result.Err = err
		} else {
			result.VersionBlock = readAt.BlockNumber
		}
		s.prune(ctx)
		return result
	}
	result.VersionBlock = latest.BlockNumber
	result.VersionTimestamp = latest.Timestamp
	if s.auditedAt == 0 {
		s.auditedAt = latest.AuditedBlock
		if s.auditedAt == 0 {
			s.auditedAt = latest.BlockNumber
		}
	}
	if !s.holdSettled {
		if err := s.settleHold(ctx, readAt.BlockNumber, latest.BlockNumber); err != nil {
			result.Err = err
			return result
		}
	}

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

	// Skipped only once a probe found the version on chain at or below finality: one orphaned while the
	// writer was down is caught on the first cycle back.
	reorged, forkPoint := false, int64(0)
	if finalized > 0 && latest.BlockNumber <= finalized && sameHash(s.finalHash, latest.BlockHash) {
		s.log.Debug("reorg probe skipped: the version was found on chain once final",
			"version", latest.BlockNumber, "finalized", finalized)
	} else {
		s.confirmedHash = ""
		reorged, forkPoint, err = s.detectReorg(ctx, *latest)
		if err != nil {
			result.Err = err
			return result
		}
		if sameHash(s.confirmedHash, latest.BlockHash) && finalized > 0 && latest.BlockNumber <= finalized {
			s.finalHash = latest.BlockHash
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

		s.scannedTo, s.scannedHash = 0, ""
		s.routerFields = nil

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
			if err := s.rebuild(ctx, readAt, modules, nil, DepthKeys, 0, true, finalized, &result); err != nil {
				result.Err = err
			} else {
				result.VersionBlock = readAt.BlockNumber
				result.VersionTimestamp = readAt.Timestamp
			}
			s.prune(ctx)
			return result
		}
		latest = survivor
		// A full read on the lost branch vouches for nothing on this one.
		if s.auditedAt > latest.BlockNumber {
			s.auditedAt = latest.BlockNumber
		}
	}

	// A version built at head while finality was unknown: a finalized block below it confirms nothing,
	// and relabelling the version with that block would date its rows too early. Probed above meanwhile.
	if readAt.BlockNumber < latest.BlockNumber {
		result.Action = ActionExtend
		result.HeadTimestamp = 0
		result.Reason = fmt.Sprintf("finalized block %d is below the version %d, waiting",
			readAt.BlockNumber, latest.BlockNumber)
		s.log.Debug("waiting for finality to reach the version", "finalized", readAt.BlockNumber,
			"version", latest.BlockNumber)
		return result
	}

	// Without a mark (a restart, a rollback) the version's block is the floor, fixed now: a rebuild on
	// this cycle that reads no operators moves the version, and must not move the floor with it.
	if s.scannedTo == 0 {
		s.scannedTo, s.scannedHash = latest.BlockNumber, latest.BlockHash
	}
	s.loadRouterFields(ctx, *latest)
	state := HeadState{
		BlockNumber:    readAt.BlockNumber,
		BlockHash:      readAt.BlockHash,
		ModuleNonces:   nonceMap(modules),
		ModulesChanged: s.routerFieldChanges(modules),
		AuditedAt:      s.auditedAt,
	}
	// Asked after the module read, where a source notices it moved. Kept until a rebuild publishes, so
	// one deploy causes one successful rebuild.
	if reporter, ok := s.source.(source.SourceChangeReporter); ok {
		if reason, changed := reporter.SourceChanged(); changed {
			if s.sourceChange != "" {
				reason = s.sourceChange + "; " + reason
			}
			s.sourceChange = reason
		}
	}
	state.SourceChange = s.sourceChange

	// The event scan runs only when the cheaper signals said nothing changed.
	decision := Evaluate(latest.AsVersion(), state, s.cfg.MaxBlockGap)
	if decision.Action == ActionExtend && !s.dueForEpochRead(s.lastScanAt) {
		s.log.Debug("operator event scan not due yet",
			"last_scan", s.lastScanAt, "epoch", s.cfg.EpochDuration.String())
	} else if decision.Action == ActionExtend {
		s.lastScanAt = s.now()
		s.checkScanMark(ctx, *latest)
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
			s.scannedTo, s.scannedHash = head.BlockNumber, head.BlockHash
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
		if err := s.store.Extend(ctx, *latest, readAt, state.ModuleNonces, finalized); err != nil {
			result.Err = err
		} else {
			s.log.Debug("cycle: extended", "version", latest.BlockNumber,
				"confirmed_at", readAt.BlockNumber, "last_changed", latest.LastChangedBlockHash)
		}

	case ActionRebuild, ActionSupersede:
		s.log.Info("rebuild starting", "reason", decision.Reason, "reads", decision.Depth.String(),
			"changed_modules", decision.ChangedModules, "version", latest.BlockNumber, "head", head.BlockNumber)

		if err := s.store.MarkStale(ctx, *latest); err != nil {
			result.Err = err
			return result
		}
		scope := decision.ChangedModules
		if decision.Audit {
			scope = nil
		}
		if err := s.rebuild(ctx, readAt, modules, scope, decision.Depth, latest.BlockNumber, !decision.Audit, finalized, &result); err != nil {
			result.Err = err
			break
		}
		result.VersionBlock = readAt.BlockNumber
		result.VersionTimestamp = readAt.Timestamp

		// The audit is the only check that nonces move with keys: a healthy one writes nothing. One that
		// coincides with a nonce change writes that change, so it proves nothing either way.
		if decision.Audit && len(decision.ChangedModules) == 0 && result.KeysChanged > 0 {
			s.log.Error("the periodic audit changed key rows, so a module nonce did not report a change",
				"rows_changed", result.KeysChanged, "keys_read", result.KeysStaged,
				"version", latest.BlockNumber, "head", head.BlockNumber)
		}
	}

	s.prune(ctx)
	return result
}

// rebuild inherits modules outside scope (nil = all) at no cost: their rows simply stay open.
// modules is the cycle's read at head (nil = read them here). published is the newest published
// block (0 = none); rows above it are an unfinished attempt's.
func (s *Syncer) rebuild(ctx context.Context, head model.ELBlockSnapshot, modules []model.StakingModule, scope []string, depth ReadDepth, published int64, reuse bool, finalized int64, result *CycleResult) error {
	at := source.Ref{BlockHash: head.BlockHash, BlockNumber: head.BlockNumber}

	var err error
	if modules == nil {
		if modules, err = s.source.Modules(ctx, at); err != nil {
			return fmt.Errorf("read modules: %w", err)
		}
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
	// The loop below visits only modules the router lists. An attempt's rows for one it no longer lists
	// (registered on a dropped branch) would be published as they are.
	if gone := goneModules(leftovers, modules); len(gone) > 0 {
		s.log.Warn("dropping an unfinished attempt: it wrote modules the router no longer lists", "modules", gone)
		if _, err := s.store.SupersedeFrom(ctx, published+1); err != nil {
			return err
		}
		leftovers = nil
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
	// Whether this rebuild read every key from the chain, which is what the block-gap audit counts from.
	everyKey := inScope == nil && depth.reads(DepthKeys)
	// Whether it read every module's operators, which is what the operator event scan counts from.
	everyOperator := true
	vetted, canRevet := build.(VettedFromOperators)
	if _, derived := s.source.(source.StakingLimitVetting); !derived {
		canRevet = false
	}
	for _, m := range modules {
		read := depth
		if inScope != nil && !inScope[m.RawAddress()] {
			read = DepthModule
		}
		if left, ok := leftovers[m.ID]; ok {
			switch {
			case reuse && left.reusable(m):
				// A failed attempt wrote this module's keys at a block still on chain, at today's
				// nonce: a cold mainnet retry skips 40 s of module 1 reads.
				read = DepthOperators
				s.log.Info("module: keys kept from an unfinished attempt",
					"module", m.ID, "nonce", m.Nonce, "attempt_block", left.Block)
			default:
				// Rows of an attempt that cannot be trusted would be inherited as they are.
				read = DepthKeys
			}
		}
		everyKey = everyKey && read.reads(DepthKeys)
		everyOperator = everyOperator && read.reads(DepthOperators)
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
			// Only over rows a published version holds, never an unfinished attempt's, and never in the
			// audit, which exists to check the rows this would trust.
			var settled map[int32]int32
			if _, attempted := leftovers[m.ID]; reuse && !attempted && published > 0 && finalized > 0 {
				if settled, err = s.settledKeys(ctx, build, m, min(finalized, published)); err != nil {
					return err
				}
			}
			diff, err = s.writeModuleKeys(ctx, build, at, m, settled)
			if err != nil {
				return err
			}
			keysTook = time.Since(moduleStarted)
			result.KeysStaged += diff.Staged
			result.KeysKept += diff.Kept
			everyKey = everyKey && diff.Kept == 0
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
		// CSM and CMv2 can lower an operator's vetted count without moving the nonce. The keys were not
		// read, so their flags follow the new staking limit here, without a key read.
		var revetted int64
		if canRevet && changed > 0 && !read.reads(DepthKeys) {
			if revetted, err = vetted.VettedFromOperators(ctx, m); err != nil {
				return err
			}
			result.KeysChanged += revetted
		}
		// An unfinished attempt's rows are already open, so this build's own diff against them is empty.
		// The attempt wrote them only because they differ from the published version.
		_, attempted := leftovers[m.ID]
		contentChanged = contentChanged || changed > 0 || revetted > 0 || attempted

		s.log.Info("module written",
			"module", m.ID, "type", m.Type, "nonce", m.Nonce, "reads", read.String(),
			"keys_read", diff.Staged, "keys_kept", diff.Kept, "keys_opened", diff.Opened, "keys_closed", diff.Closed,
			"keys_unchanged", diff.Unchanged, "keys_revetted", revetted,
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

	if marker, ok := build.(AuditMarker); ok {
		audited := s.auditedAt
		if everyKey {
			audited = head.BlockNumber
		}
		marker.SetAuditedBlock(audited)
	}
	publishStarted := time.Now()
	if err := build.Publish(ctx, "latest", finalized); err != nil {
		return err
	}
	committed = true
	result.Rebuilt = true
	if everyKey {
		s.auditedAt = head.BlockNumber
	}
	if everyOperator {
		if head.BlockNumber >= s.scannedTo {
			s.scannedTo, s.scannedHash = head.BlockNumber, head.BlockHash
		}
	}
	// Read at a final block: no reorg can take it, so no probe needs to vouch for it.
	if finalized > 0 && head.BlockNumber <= finalized {
		s.finalHash = head.BlockHash
	}
	// Every rebuild writes every module row, so a pending source change is now applied.
	s.sourceChange = ""
	s.routerFields = routerFieldsOf(modules)
	s.log.Debug("publish: status flipped",
		"took", time.Since(publishStarted).Round(time.Millisecond).String())

	s.log.Info("version published",
		"block", head.BlockNumber, "modules", len(modules),
		"scoped_to", len(inScope), "keys_read", result.KeysStaged, "keys_changed", result.KeysChanged)
	return nil
}

// settleHold: a writer that comes up with the full read overdue may have been down long enough that
// the newest version is far behind, so readers get 425 until the full read publishes.
func (s *Syncer) settleHold(ctx context.Context, block, version int64) error {
	holder, ok := s.store.(ServingHold)
	if !ok {
		s.holdSettled = true
		return nil
	}
	due := s.cfg.MaxBlockGap > 0 && block-s.auditedAt >= s.cfg.MaxBlockGap
	if err := holder.Hold(ctx, due); err != nil {
		return err
	}
	s.holdSettled = true
	if due {
		s.log.Warn("full read overdue on start: latest and finalized answer 425 until it publishes",
			"audited_at", s.auditedAt, "version", version, "block", block, "limit", s.cfg.MaxBlockGap)
	}
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

func goneModules(leftovers map[int32]Leftover, modules []model.StakingModule) []int32 {
	listed := make(map[int32]bool, len(modules))
	for _, m := range modules {
		listed[m.ID] = true
	}
	var gone []int32
	for id := range leftovers {
		if !listed[id] {
			gone = append(gone, id)
		}
	}
	slices.Sort(gone)
	return gone
}

// settledKeys is nil when either side cannot read or keep a prefix.
func (s *Syncer) settledKeys(ctx context.Context, build Build, m model.StakingModule, finalized int64) (map[int32]int32, error) {
	settler, ok := build.(SettledKeys)
	if !ok {
		return nil, nil
	}
	if _, ok := s.source.(source.TailKeyReader); !ok {
		return nil, nil
	}
	return settler.SettledKeys(ctx, m, finalized)
}

// writeModuleKeys: the diff runs in the database, so memory stays bounded for any module size.
// settled (may be nil) is each operator's deposited prefix the store already holds; only the keys
// after it are read.
func (s *Syncer) writeModuleKeys(ctx context.Context, build Build, at source.Ref, m model.StakingModule, settled map[int32]int32) (KeyDiff, error) {
	writer, err := build.Keys(ctx, m)
	if err != nil {
		return KeyDiff{}, err
	}

	var (
		staged   int64
		started  = time.Now()
		lastLine = time.Now()
	)
	add := func(k model.Key) error {
		staged++
		if time.Since(lastLine) > 5*time.Second {
			lastLine = time.Now()
			elapsed := time.Since(started)
			s.log.Debug("module: staging keys", "module", m.ID, "keys", staged,
				"elapsed", elapsed.Round(time.Second).String(),
				"keys/s", int(float64(staged)/elapsed.Seconds()))
		}
		return writer.Add(ctx, k)
	}

	tail, canRead := s.source.(source.TailKeyReader)
	keeper, canKeep := writer.(PrefixKeeper)
	if len(settled) > 0 && canRead && canKeep {
		applied, err := tail.KeysFrom(ctx, at, m, settled, add)
		if err != nil {
			writer.Abort(ctx)
			return KeyDiff{}, fmt.Errorf("read keys of module %d: %w", m.ID, err)
		}
		keeper.KeepPrefix(applied)
	} else if err := s.source.Keys(ctx, at, m, add); err != nil {
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
	if onChain == "" {
		// The node lacks that height yet: a lagging endpoint, not a reorg.
		return false, 0, nil
	}
	if sameHash(onChain, latest.BlockHash) {
		s.confirmedHash = latest.BlockHash
		return false, 0, nil
	}

	// Only served versions anchor: a superseded one matches again when the chain returns to its
	// branch, and would keep the losing branch's version at the same height. Failed attempts and
	// earlier rollbacks leave rows of that kind, which must not use up the search.
	versions, err := s.store.Recent(ctx, latest.BlockNumber, s.cfg.ReorgSearchDepth)
	if err != nil {
		return true, latest.BlockNumber, err
	}

	for _, v := range versions {
		hash, err := s.source.BlockHashAt(ctx, v.BlockNumber)
		if err != nil {
			return true, latest.BlockNumber, err
		}
		if sameHash(hash, v.BlockHash) {
			return true, v.BlockNumber + 1, nil
		}
	}

	// Every served version is off chain. With the search cut short, older ones went unchecked: they go
	// too, rather than becoming the survivor.
	oldest := latest.BlockNumber
	if len(versions) > 0 {
		oldest = versions[len(versions)-1].BlockNumber
	}
	if len(versions) == s.cfg.ReorgSearchDepth {
		oldest = 0
	}
	s.log.Warn("no served version is on chain, rebuilding from scratch", "searched", len(versions), "from", oldest)
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

// checkScanMark costs one block read per scan, and only while the mark is above the version: the
// reorg probe already vouches for the version's own block.
func (s *Syncer) checkScanMark(ctx context.Context, latest StoredVersion) {
	_, grouped := s.source.(source.GroupedOperatorWatcher)
	_, single := s.source.(source.OperatorWatcher)
	if s.scannedTo <= latest.BlockNumber || !grouped && !single {
		return
	}
	hash, err := s.source.BlockHashAt(ctx, s.scannedTo)
	if err == nil && hash != "" && sameHash(hash, s.scannedHash) {
		return
	}
	s.log.Info("the scan mark is not on chain, scanning from the version",
		"mark", s.scannedTo, "version", latest.BlockNumber, "err", err)
	s.scannedTo, s.scannedHash = latest.BlockNumber, latest.BlockHash
}

// scanFloor is not the version's block once a mark exists: a rebuild scoped to other modules moves
// the version without reading these operators.
func (s *Syncer) scanFloor(versionBlock int64) int64 {
	if s.scannedTo > 0 {
		return s.scannedTo
	}
	return versionBlock
}

// loadRouterFields reads the published module rows once per process (and after a rollback). Without
// the capability, or on an error, the first rebuild sets the baseline.
func (s *Syncer) loadRouterFields(ctx context.Context, latest StoredVersion) {
	if s.routerFields != nil {
		return
	}
	reader, ok := s.store.(PublishedModules)
	if !ok {
		return
	}
	modules, err := reader.PublishedModules(ctx, latest)
	if err != nil {
		s.log.Warn("could not read the published module rows; router field changes wait for a rebuild", "err", err)
		return
	}
	s.routerFields = routerFieldsOf(modules)
}

// routerFieldChanges is nil while the published rows are unknown. A module absent from them is left
// to the nonce rule, which already sees it.
func (s *Syncer) routerFieldChanges(modules []model.StakingModule) []string {
	if s.routerFields == nil {
		return nil
	}
	var changed []string
	for _, m := range modules {
		address := strings.ToLower(m.RawAddress())
		if published, ok := s.routerFields[address]; ok && published != routerFieldsKey(m) {
			changed = append(changed, m.RawAddress())
		}
	}
	return changed
}

func routerFieldsOf(modules []model.StakingModule) map[string]string {
	out := make(map[string]string, len(modules))
	for _, m := range modules {
		out[strings.ToLower(m.RawAddress())] = routerFieldsKey(m)
	}
	return out
}

// routerFieldsKey is the module row minus the nonce, which has a rule of its own, and minus
// lastChangedBlockHash, which the writer derives.
func routerFieldsKey(m model.StakingModule) string {
	return fmt.Sprint(m.ID, "|", m.Type, "|", m.Name, "|", m.ModuleFee, "|", m.TreasuryFee, "|",
		m.TargetShare, "|", m.Status, "|", m.LastDepositAt, "|", m.LastDepositBlock, "|",
		m.ExitedValidatorsCount, "|", m.Active, "|", m.WithdrawalCredentialsType)
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
	s.retentionHeldAt = pruned.HeldAt
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

// unfinishedBuilds runs after the cycle, when no build of this writer is open, so every building
// row is one a dead writer left.
func (s *Syncer) unfinishedBuilds(ctx context.Context) int64 {
	counter, ok := s.store.(BuildCounter)
	if !ok {
		return -1
	}
	countCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	n, err := counter.UnfinishedBuilds(countCtx)
	if err != nil {
		return -1
	}
	return n
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
