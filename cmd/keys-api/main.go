// Command keys-api syncs a registry into a versioned schema and serves the keys-api surface from it.
// KEYS_API_MODE=server|ingest splits the halves: a version goes live by a status flip, not row arrival.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lidofinance/keys-api-versioned/internal/api"
	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/logmask"
	"github.com/lidofinance/keys-api-versioned/internal/metrics"
	"github.com/lidofinance/keys-api-versioned/internal/secrets"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
	"github.com/lidofinance/keys-api-versioned/internal/source/static"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// version is this service's own, for metrics and logs. /v1/status reports api.ContractVersion.
const version = "0.1.0-poc"

// commit is set at build time: -ldflags "-X main.commit=<sha>".
var commit = "unknown"

const metricsNamespace = "keys_api"

func main() {
	masker := &logmask.Masker{}
	log := slog.New(logmask.NewHandler(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}), masker))

	if err := run(log, masker); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, masker *logmask.Masker) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Before anything reads the environment, so the file is the source for every envOr below.
	// A broken file is logged and ignored: the environment still holds a working configuration.
	secretsPath := envOr("SECRETS_FILE_PATH", secrets.DefaultPath)
	appliedSecrets, ignoredSecrets, rejectedSecrets, err := secrets.LoadIntoEnvChecked(secretsPath, checkSecret)
	addSecrets(masker, appliedSecrets)
	if err != nil {
		log.Error("secrets file unusable, reading configuration from the environment", "path", secretsPath, "err", err)
	}
	for key, err := range rejectedSecrets {
		log.Error("secrets file: value rejected, keeping the environment's", "path", secretsPath, "key", key, "err", err)
	}
	if len(ignoredSecrets) > 0 {
		log.Warn("secrets file: only credentials are read from it, other keys are ignored",
			"path", secretsPath, "ignored", strings.Join(ignoredSecrets, ","))
	}
	if mode, loose := secrets.LooseMode(secretsPath); loose {
		log.Warn("secrets file is readable by group or others", "path", secretsPath, "mode", mode.String())
	}
	if len(appliedSecrets) > 0 {
		log.Info("configuration: secrets file over the environment", "path", secretsPath)
	} else {
		log.Info("configuration: the environment", "path", secretsPath, "reason", "no secrets file")
	}

	if err := validateEnv(); err != nil {
		return err
	}

	var setEndpoints func(string) error

	mode := envOr("KEYS_API_MODE", "all")
	if mode != "all" && mode != "server" && mode != "ingest" {
		return fmt.Errorf("KEYS_API_MODE must be all, server or ingest, got %q", mode)
	}

	dsn := envOr("DATABASE_URL", "")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}

	// Validated at startup: a bad default pin would answer 400 to every request, which reads like a
	// client problem.
	var pin, extPin string
	if mode != "ingest" {
		var err error
		if pin, err = pinOption("KEYS_API_PIN", "latest"); err != nil {
			return err
		}
		if extPin, err = pinOption("KEYS_API_EXT_PIN", defaultExtPin(envOr("KEYS_API_SOURCE", "lido-core"))); err != nil {
			return err
		}
	}

	m := metrics.New(metricsNamespace)

	chainID := int64(envInt("CHAIN_ID", 1))

	// build_info's `network` label is the chain id: canonical dashboards filter on it.
	m.SetBuildInfo("keys-api-versioned", version,
		envOr("KEYS_API_ENV", "unknown"), strconv.FormatInt(chainID, 10))
	m.SetBuildCommit(metricsNamespace, version, commit)

	// One schema per chain: otherwise a store from another chain looks like a reorg deeper than the
	// window and is silently rebuilt from scratch.
	pgStore, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{
		MaxConns: int32(databaseMaxConns()),
		Source:   envOr("KEYS_API_SOURCE", "lido-core"),
		ChainID:  chainID,
		// The API comes up anyway: liveness passes, readiness says 503 until a version is served.
		Lazy: true,
	})
	if err != nil {
		return err
	}
	defer pgStore.Close()
	m.BindPoolStats(metricsNamespace, func() metrics.PoolStats {
		s := pgStore.Pool().Stat()
		return metrics.PoolStats{
			Acquired: s.AcquiredConns(), Idle: s.IdleConns(), Total: s.TotalConns(), Max: s.MaxConns(),
			Acquires: s.AcquireCount(), EmptyAcquires: s.EmptyAcquireCount(),
			CanceledAcquires: s.CanceledAcquireCount(), AcquireWait: s.AcquireDuration(),
		}
	})

	// chain_id and schema on every line: a mainnet node against a Hoodi store looks healthy in the log.
	schema := pgStore.Schema()
	if schema == "" {
		schema = "from-dsn"
	}
	log = log.With("chain_id", chainID, "schema", schema)

	log.Info("keys-api starting",
		"mode", mode, "source", envOr("KEYS_API_SOURCE", "lido-core"),
		"app_version", version, "contract_version", api.ContractVersion, "commit", commit, "providers", redactedProviders(),
		"pin", pin, "ext_pin", extPin)

	var (
		newSyncer    func() *ingest.Syncer
		syncerSource source.Provider
	)
	if mode != "server" {
		provider, rotateEndpoints, err := buildSource(ctx, chainID, log, m)
		if err != nil {
			return err
		}
		setEndpoints = rotateEndpoints

		// A node on the wrong chain returns plausible, wrong keys. Endpoint sources are checked per
		// endpoint by the writer before it starts (verifyChain); this covers a file.
		_, perEndpoint := provider.(source.ChainVerifier)
		if identifier, ok := provider.(source.ChainIdentifier); ok && !perEndpoint {
			actual, err := identifier.ChainID(ctx)
			if err != nil {
				log.Warn("could not verify the chain id", "err", err)
			} else if actual != 0 && actual != chainID {
				return fmt.Errorf("CHAIN_ID is %d but the source reports %d", chainID, actual)
			}
		}

		syncerSource = provider
		cfg := ingest.SyncConfig{
			Interval:         time.Duration(envInt("UPDATE_KEYS_INTERVAL_MS", 5000)) * time.Millisecond,
			MaxBlockGap:      int64(envInt("MAX_BLOCKS_OVERLAP", 1000)),
			RetainVersions:   envInt("RETAIN_VERSIONS", 20),
			BuildAtFinalized: envBool("KEYS_API_BUILD_AT_FINALIZED", false),
			EpochDuration:    epochDuration(),
		}
		newSyncer = func() *ingest.Syncer {
			syncer := ingest.NewSyncer(provider, pgStore, cfg, log)
			syncer.OnCycle = func(r ingest.CycleResult) { observeCycle(m, r) }
			syncer.OnModule = func(r ingest.ModuleResult) { observeModule(m, r) }
			return syncer
		}
	}

	errs := make(chan error, 3)

	syncDone := make(chan struct{})
	// Runs before the deferred pool close: a build cut short by shutdown still needs it to abort.
	defer func() {
		stop()
		select {
		case <-syncDone:
		case <-time.After(15 * time.Second):
			log.Warn("ingest did not stop within 15s; closing the database pool anyway")
		}
	}()

	if newSyncer == nil {
		close(syncDone)
	} else {
		go func() {
			defer close(syncDone)
			if err := verifyChain(ctx, syncerSource, log); err != nil {
				errs <- err
				return
			}
			errs <- runWriter(ctx, pgStore, newSyncer, log)
		}()
	}

	var server *http.Server
	if mode != "ingest" {
		reader := store.NewAppWithPool(pgStore.Pool(), pin)
		reader.Log = log
		handler := api.New(reader, api.Config{
			ChainID:        chainID,
			AppVersion:     api.ContractVersion,
			ExtPin:         extPin,
			Log:            log,
			MetricsHandler: m.Handler(),
			Observe:        m.ObserveRequest,

			MaxStreams:        maxStreams(),
			StreamMaxDuration: time.Duration(envInt("STREAM_MAX_DURATION_MS", int(10*time.Minute/time.Millisecond))) * time.Millisecond,
		}).Handler()

		server = newHTTPServer(":"+envOr("PORT", "3000"), m.CountInFlight(handler))

		go func() {
			log.Info("http listening", "addr", server.Addr, "mode", mode)
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- err
				return
			}
			errs <- nil
		}()
	}

	// Rotation, once everything that could consume a credential exists. Watched even when the file
	// was missing or broken at boot: one that appears later is still read.
	restart := make(chan error, 1)
	interval := time.Duration(envInt("SECRETS_POLL_INTERVAL_IN_SECONDS", int(secrets.DefaultPollInterval/time.Second))) * time.Second
	watcher := secrets.NewWatcher(secretsPath, interval,
		func(values map[string]string) error {
			values, _ = secrets.Split(values)
			addSecrets(masker, values)
			return applyRotatedSecrets(values, setEndpoints, restart, log, m)
		},
		func(err error) {
			log.Error("secrets rotation", "err", err)
			m.SecretsReloads.WithLabelValues("failure").Inc()
		})
	go watcher.Run(ctx)

	var exitErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errs:
		if err != nil && !errors.Is(err, context.Canceled) {
			stop()
			return err
		}
	case exitErr = <-restart:
		log.Warn("shutting down for a restart", "reason", exitErr)
	}

	if server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Warn("http shutdown", "err", err)
		}
	}
	return exitErr
}

// verifyChain refuses an endpoint on another chain, and waits with backoff while none answers
// rather than starting unverified; the API keeps serving meanwhile.
func verifyChain(ctx context.Context, provider any, log *slog.Logger) error {
	verifier, ok := provider.(source.ChainVerifier)
	if !ok {
		return nil
	}
	for wait := chainRetryStart; ; wait = min(2*wait, 30*time.Second) {
		verified, err := verifier.VerifyChain(ctx)
		if err != nil {
			return err
		}
		if verified > 0 {
			return nil
		}
		log.Warn("no endpoint has confirmed the chain id yet, retrying", "in", wait.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

var chainRetryStart = time.Second

// runWriter waits for the writer lock without holding up the API: in KEYS_API_MODE=all a rollout
// keeps the old pod, and its lock, until the new one is ready. Only the writer applies the schema.
func runWriter(ctx context.Context, pgStore *ingest.PGStore, newSyncer func() *ingest.Syncer, log *slog.Logger) error {
	if err := pgStore.WaitForDatabase(ctx, func(err error, next time.Duration) {
		log.Warn("database unreachable, retrying", "err", err, "in", next.String())
	}); err != nil {
		return err
	}
	return pgStore.RunExclusive(ctx, ingest.WriterLockOptions{
		OnWait: func() { log.Warn("another writer holds the writer lock for this schema; waiting for it to stop") },
		OnStop: func(err error) { log.Error("writer stopped; trying the writer lock again", "err", err) },
	}, func(ctx context.Context) error {
		log.Info("writer lock acquired")
		if err := pgStore.Migrate(ctx); err != nil {
			return err
		}
		epoch, err := pgStore.TakeEpoch(ctx)
		if err != nil {
			return err
		}
		log.Info("schema applied", "writer_epoch", epoch)
		log.Info("ingest starting",
			"source", envOr("KEYS_API_SOURCE", "lido-core"),
			"interval_ms", envInt("UPDATE_KEYS_INTERVAL_MS", 5000),
			"retain_versions", envInt("RETAIN_VERSIONS", 20),
			"epoch", epochDuration().String(),
			"metered", meteredEndpoints())
		// A syncer per lock: what it holds in memory is only true while no other writer wrote.
		return newSyncer().Run(ctx)
	})
}

// newHTTPServer has no WriteTimeout: a full dump is 200 MiB, so streams move a per-write deadline
// instead. 16 KiB of headers is Node's default, the canonical service's.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

// addSecrets registers the credentials from the environment and the secrets file with the log masker.
func addSecrets(masker *logmask.Masker, fileValues map[string]string) {
	masker.Add(logmask.FromURLs(os.Getenv("PROVIDERS_URLS"))...)
	masker.Add(logmask.FromDSN(os.Getenv("DATABASE_URL"))...)
	for key, value := range fileValues {
		switch key {
		case "PROVIDERS_URLS":
			masker.Add(logmask.FromURLs(value)...)
		case "DATABASE_URL":
			masker.Add(logmask.FromDSN(value)...)
		default:
			// A number in the file is configuration, like a chain id; masking it would blank out logs.
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				masker.Add(value)
			}
		}
	}
}

// applyRotatedSecrets compares with the environment, which the file was loaded into at boot.
// PROVIDERS_URLS applies live once it validates (the client copies endpoints per request);
// DATABASE_URL asks for a restart, since the pool holds connections made with the old one.
func applyRotatedSecrets(
	values map[string]string,
	setEndpoints func(string) error,
	restart chan<- error,
	log *slog.Logger,
	m *metrics.Metrics,
) error {
	if value, ok := values["DATABASE_URL"]; ok && value != os.Getenv("DATABASE_URL") {
		// The restart reads the file at boot: a value boot would reject stops every replica at once.
		for key, value := range values {
			if err := checkSecret(key, value); err != nil {
				return fmt.Errorf("%s: %w; not restarting for the new DATABASE_URL", key, err)
			}
		}
		m.SecretsReloads.WithLabelValues("restart_required").Inc()
		select {
		case restart <- errors.New("DATABASE_URL changed in the secrets file; exiting so the orchestrator restarts with it"):
		default:
		}
		return nil
	}

	value, ok := values["PROVIDERS_URLS"]
	if !ok || value == os.Getenv("PROVIDERS_URLS") || setEndpoints == nil {
		return nil
	}
	if err := validateProviderURLs(value); err != nil {
		return fmt.Errorf("PROVIDERS_URLS: %w; keeping the previous endpoints", err)
	}
	if err := setEndpoints(value); err != nil {
		return fmt.Errorf("PROVIDERS_URLS: %w", err)
	}
	if err := os.Setenv("PROVIDERS_URLS", value); err != nil {
		return err
	}
	log.Info("applied rotated secrets", "keys", "PROVIDERS_URLS")
	m.SecretsReloads.WithLabelValues("success").Inc()
	return nil
}

// checkSecret drops the DSN parse error: it quotes the connection string.
func checkSecret(key, value string) error {
	switch key {
	case "DATABASE_URL":
		if _, err := pgxpool.ParseConfig(value); err != nil {
			return errors.New("not a usable Postgres connection string")
		}
	case "PROVIDERS_URLS":
		return validateProviderURLs(value)
	}
	return nil
}

// validateProviderURLs names entries by position: the URL itself carries the provider key.
func validateProviderURLs(list string) error {
	n := 0
	for _, raw := range strings.Split(list, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		n++
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("entry %d is not an http(s) URL with a host", n)
		}
	}
	if n == 0 {
		return errors.New("no endpoints")
	}
	return nil
}

// buildSource returns a nil endpoint setter for a source without endpoints: "nothing to rotate"
// differs from "rotation did nothing".
func buildSource(ctx context.Context, chainID int64, log *slog.Logger, m *metrics.Metrics) (source.Provider, func(string) error, error) {
	switch name := envOr("KEYS_API_SOURCE", "lido-core"); name {
	case "lido-core":
		urls := envOr("PROVIDERS_URLS", "")
		if urls == "" {
			return nil, nil, errors.New("PROVIDERS_URLS is required for KEYS_API_SOURCE=lido-core")
		}
		if err := validateProviderURLs(urls); err != nil {
			return nil, nil, fmt.Errorf("PROVIDERS_URLS: %w", err)
		}

		// No compiled-in per-chain addresses: devnets are the main use case.
		locator := envOr("LIDO_LOCATOR", "")
		if locator == "" {
			return nil, nil, fmt.Errorf("LIDO_LOCATOR is required for KEYS_API_SOURCE=lido-core "+
				"(chain %d): the Lido Locator address is what everything else is resolved from, "+
				"and the deployed-contracts page at docs.lido.fi is the reference", chainID)
		}

		client := eth.NewClient(urls, eth.Options{
			ChainID:     chainID,
			Concurrency: envInt("PROVIDER_CONCURRENT_REQUESTS", 8),
			BatchSize:   envInt("PROVIDER_JSON_RPC_MAX_BATCH_SIZE", 100),
			Timeout:     time.Duration(envInt("PROVIDER_TIMEOUT_MS", 120_000)) * time.Millisecond,
			Multicall:   envBool("PROVIDER_MULTICALL", true),
			// Metered: per-call billing makes one aggregate3 cheaper than a batch of the same reads.
			Metered:           meteredEndpoints(),
			MulticallAddress:  envOr("PROVIDER_MULTICALL_ADDRESS", eth.MulticallAddress),
			MulticallMaxCalls: envInt("PROVIDER_MULTICALL_MAX_CALLS", 0),
			MulticallMaxCost:  envInt("PROVIDER_MULTICALL_MAX_KEYS", 0),
			Log:               log,
			Observe: func(r eth.RequestInfo) {
				m.ObserveRPC(r.Method, r.Result, r.Endpoint, r.Calls, r.Duration)
			},
		})
		m.BindRPCStats(metricsNamespace, func() metrics.RPCStats {
			s := client.Stats()
			return metrics.RPCStats{
				Requests: s.Requests, Calls: s.Calls,
				Aggregates: s.Aggregates, Splits: s.Splits, Fallbacks: s.Fallbacks,
				Unclassified: s.Unclassified,
			}
		})
		provider := lidocore.NewProvider(client, locator, lidocore.Options{
			KeysBatch:       uint64(envInt("KEYS_BATCH_SIZE", lidocore.KeysBatchSize)),
			CallsPerRequest: envInt("KEYS_CALLS_PER_REQUEST", lidocore.CallsPerRequest),
			KeysPerRequest:  uint64(envInt("KEYS_PER_REQUEST", lidocore.KeysPerRequest)),
			// Defaults to the client's limit, whose semaphore is the real bound anyway.
			Concurrency:    envInt("KEYS_READ_CONCURRENCY", envInt("PROVIDER_CONCURRENT_REQUESTS", 8)),
			Log:            log,
			OnRouterChange: func(previous, current string) { m.RouterChanges.Inc() },
			OnContractVersion: func(previous, current int64) {
				m.RouterVersion.Set(float64(current))
				m.VersionChanges.Inc()
			},
		})
		log.Info("source: lido core",
			"locator", locator, "module_types", strings.Join(provider.SupportedTypes(), ","))
		m.BindAggregation(metricsNamespace, client.Aggregation)
		return provider, client.SetURLs, nil

	case "static":
		path := envOr("KEYS_API_STATIC_FILE", "")
		if path == "" {
			return nil, nil, errors.New("KEYS_API_STATIC_FILE is required for KEYS_API_SOURCE=static")
		}
		provider := static.New(path)
		if _, err := provider.Head(ctx); err != nil {
			return nil, nil, fmt.Errorf("static source: %w", err)
		}
		log.Info("source: static file", "path", path)
		return provider, nil, nil

	default:
		return nil, nil, fmt.Errorf("unknown KEYS_API_SOURCE %q", name)
	}
}

// Default true: off on a hosted provider costs ~5x compute units; on against an own node, 0.99 s vs
// 0.87 s per quiet cycle. One function so the startup log and the client cannot disagree.
func meteredEndpoints() bool { return envBool("PROVIDER_METERED", true) }

// epochDuration is computed, not fetched (there is no consensus client). Zero from either variable
// disables epoch pacing.
func epochDuration() time.Duration {
	seconds := envInt("SECONDS_PER_SLOT", 12)
	slots := envInt("SLOTS_PER_EPOCH", 32)
	if seconds <= 0 || slots <= 0 {
		return 0
	}
	return time.Duration(seconds*slots) * time.Second
}

func observeCycle(m *metrics.Metrics, r ingest.CycleResult) {
	outcome := "ok"
	if r.Err != nil {
		outcome = "error"
	}
	action := string(r.Action)
	if action == "" {
		action = "none"
	}

	m.CyclesTotal.WithLabelValues(action, outcome).Inc()
	if r.Panicked {
		m.CyclePanics.Inc()
	}
	m.CycleDuration.Observe(r.Duration.Seconds())
	m.RetentionHeldAt.Set(float64(r.RetentionHeldAt))
	if r.UnfinishedBuilds >= 0 {
		m.UnfinishedBuilds.Set(float64(r.UnfinishedBuilds))
	}
	m.KeysRead.Add(float64(r.KeysStaged))
	m.KeysChanged.Add(float64(r.KeysChanged))

	if r.HeadBlock > 0 {
		m.HeadBlock.Set(float64(r.HeadBlock))
	}
	if r.VersionBlock > 0 {
		m.VersionBlock.Set(float64(r.VersionBlock))
		if r.HeadBlock > 0 {
			m.BlocksBehind.Set(float64(r.HeadBlock - r.VersionBlock))
		}
	}
	// VersionAge climbs whenever the chain is quiet; ConfirmedAge climbs only when the loop stops
	// working. Alert on ConfirmedAge.
	if r.VersionTimestamp > 0 {
		// A file source has no block time; the age stays unset rather than alarming.
		m.VersionAge.Set(float64(time.Now().Unix() - r.VersionTimestamp))
	}
	if r.Err == nil && r.HeadTimestamp > 0 {
		m.ConfirmedAge.Set(float64(time.Now().Unix() - r.HeadTimestamp))
	}
	if r.Rebuilt {
		m.RebuildsTotal.Inc()
	}
	// A healthy audit re-reads every key and writes nothing; divergence means a nonce missed a change.
	if r.Audit {
		m.AuditsTotal.Inc()
		if r.KeysChanged > 0 {
			m.AuditDivergenceTotal.Add(float64(r.KeysChanged))
		}
	}
	if r.ReorgFrom > 0 || r.Action == ingest.ActionSupersede {
		m.ReorgsTotal.Inc()
	}

	// Stale means a rebuild is in flight over a version we no longer trust; a cycle
	// that ended in a publish has cleared it.
	stale := 0.0
	if r.Action != ingest.ActionExtend && !r.Rebuilt && r.Err != nil {
		stale = 1
	}
	m.VersionStale.Set(stale)

	observeCanonicalCycle(m, r, outcome)
}

// Copied exactly from the canonical service, spaces included: dashboards select on the string.
const canonicalJobName = "Update Staking Router Modules keys"

// Canonical last_block_number and last_update_timestamp are the confirmed head, not the version's
// own block (that is published_version_block, which has no canonical counterpart).
func observeCanonicalCycle(m *metrics.Metrics, r ingest.CycleResult, outcome string) {
	result := "success"
	if outcome != "ok" {
		result = "error"
	}
	m.ObserveJob(canonicalJobName, result, r.Duration)

	if r.Err != nil {
		return
	}
	if r.HeadBlock > 0 {
		m.LastBlockNumber.Set(float64(r.HeadBlock))
	}
	if r.HeadTimestamp > 0 {
		m.LastUpdate.Set(float64(r.HeadTimestamp))
	}
	for _, module := range r.Modules {
		m.SetNonce(module.ID, module.Nonce)
	}
}

func observeModule(m *metrics.Metrics, r ingest.ModuleResult) {
	// Only when keys were read: an operators-only rebuild would record a zero-key update.
	if r.KeysReread {
		m.ObserveModuleUpdate(r.Address, r.KeysRead, r.Duration)
	}
	for _, op := range r.Operators {
		unused := op.TotalSigningKeys - op.UsedSigningKeys
		if unused < 0 {
			unused = 0
		}
		m.SetOperatorKeys(r.ModuleID, op.Index, op.UsedSigningKeys, unused)
	}
}

// redactedProviders logs hosts only: a hosted provider's URL carries its API key in the path.
func redactedProviders() string {
	var hosts []string
	for _, raw := range strings.Split(envOr("PROVIDERS_URLS", ""), ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		host := raw
		if parsed, err := url.Parse(raw); err == nil && parsed.Host != "" {
			host = parsed.Host
		}
		hosts = append(hosts, host)
	}
	if len(hosts) == 0 {
		return "none"
	}
	return strings.Join(hosts, ",")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// pinOption accepts only a label: a fixed block or hash is pruned within minutes, then every
// request answers 410.
func pinOption(name, def string) (string, error) {
	pin := envOr(name, def)
	if pin != "latest" && pin != "finalized" {
		return "", fmt.Errorf("%s must be latest or finalized, got %q", name, pin)
	}
	return pin, nil
}

// defaultExtPin: a file source never records finality and the pin would answer 501, so it serves latest.
func defaultExtPin(source string) string {
	if source == "static" {
		return "latest"
	}
	return "finalized"
}

// envLimits: a typo or a negative used to fall back to the default without a line in the log.
var envLimits = map[string][2]int64{
	"CHAIN_ID":                         {0, math.MaxInt64},
	"DATABASE_MAX_CONNS":               {1, 1000},
	"PORT":                             {1, 65535},
	"UPDATE_KEYS_INTERVAL_MS":          {1, 86_400_000},
	"MAX_BLOCKS_OVERLAP":               {1, math.MaxInt32},
	"RETAIN_VERSIONS":                  {0, 1_000_000},
	"SECONDS_PER_SLOT":                 {0, 3600},
	"SLOTS_PER_EPOCH":                  {0, 1 << 16},
	"SECRETS_POLL_INTERVAL_IN_SECONDS": {1, 86_400},
	"PROVIDER_CONCURRENT_REQUESTS":     {1, 1024},
	"PROVIDER_JSON_RPC_MAX_BATCH_SIZE": {1, 10_000},
	"PROVIDER_TIMEOUT_MS":              {1, 3_600_000},
	"PROVIDER_MULTICALL_MAX_CALLS":     {0, 100_000},
	"PROVIDER_MULTICALL_MAX_KEYS":      {0, 10_000_000},
	"KEYS_BATCH_SIZE":                  {1, 100_000},
	"KEYS_CALLS_PER_REQUEST":           {1, 100_000},
	"KEYS_PER_REQUEST":                 {1, 10_000_000},
	"KEYS_READ_CONCURRENCY":            {1, 1024},
	"STREAM_MAX_CONCURRENT":            {1, 1000},
	"STREAM_MAX_DURATION_MS":           {1000, 86_400_000},
}

var envBools = []string{"KEYS_API_BUILD_AT_FINALIZED", "PROVIDER_METERED", "PROVIDER_MULTICALL"}

// validateEnv does not echo values: a credential pasted into the wrong variable stays out of the log.
func validateEnv() error {
	var errs []string
	for key, limits := range envLimits {
		v := os.Getenv(key)
		if v == "" {
			continue
		}
		if n, err := strconv.ParseInt(v, 10, 64); err != nil || n < limits[0] || n > limits[1] {
			errs = append(errs, fmt.Sprintf("%s must be an integer from %d to %d", key, limits[0], limits[1]))
		}
	}
	for _, key := range envBools {
		if v := os.Getenv(key); v != "" {
			if _, err := strconv.ParseBool(v); err != nil {
				errs = append(errs, key+" must be true or false")
			}
		}
	}
	// Streams at the pool size leave readiness, /v1/status and the writer nothing to acquire. Checked on
	// the default too: half of a pool of 1 rounds up to the whole pool.
	if envOr("KEYS_API_MODE", "all") != "ingest" && maxStreams() >= databaseMaxConns() {
		errs = append(errs, "STREAM_MAX_CONCURRENT must be below DATABASE_MAX_CONNS, which must be at least 2 to serve")
	}
	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return errors.New(strings.Join(errs, "; "))
}

func databaseMaxConns() int { return envInt("DATABASE_MAX_CONNS", 10) }

// maxStreams defaults to half the pool: the other half serves short reads, probes and the writer.
func maxStreams() int { return envInt("STREAM_MAX_CONCURRENT", max(1, databaseMaxConns()/2)) }

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return parsed
}

func logLevel() slog.Level {
	switch strings.ToLower(envOr("LOG_LEVEL", "info")) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
