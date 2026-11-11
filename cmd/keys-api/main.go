// Command keys-api syncs a registry into a versioned schema and serves the keys-api surface from it.
// KEYS_API_MODE=server|ingest splits the halves: a version goes live by a status flip, not row arrival.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/api"
	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/metrics"
	"github.com/lidofinance/keys-api-versioned/internal/secrets"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
	"github.com/lidofinance/keys-api-versioned/internal/source/static"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// appVersion is read by consumers from /v1/status, so it stays semver, not a build hash.
const appVersion = "0.1.0-poc"

const metricsNamespace = "keys_api"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))

	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Before anything reads the environment, so the file is the source for every envOr below.
	// A broken file is logged and ignored: the environment still holds a working configuration.
	secretsPath := envOr("SECRETS_FILE_PATH", secrets.DefaultPath)
	appliedSecrets, err := secrets.LoadIntoEnv(secretsPath)
	if err != nil {
		log.Error("secrets file unusable, reading configuration from the environment", "path", secretsPath, "err", err)
	}
	if len(appliedSecrets) > 0 {
		log.Info("configuration: secrets file over the environment", "path", secretsPath)
	} else {
		log.Info("configuration: the environment", "path", secretsPath, "reason", "no secrets file")
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
	m.SetBuildInfo("keys-api-versioned", appVersion,
		envOr("KEYS_API_ENV", "unknown"), strconv.FormatInt(chainID, 10))

	// One schema per chain: otherwise a store from another chain looks like a reorg deeper than the
	// window and is silently rebuilt from scratch.
	pgStore, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{
		MaxConns: int32(envInt("DATABASE_MAX_CONNS", 10)),
		Source:   envOr("KEYS_API_SOURCE", "lido-core"),
		ChainID:  chainID,
	})
	if err != nil {
		return err
	}
	defer pgStore.Close()

	// chain_id and schema on every line: a mainnet node against a Hoodi store looks healthy in the log.
	schema := pgStore.Schema()
	if schema == "" {
		schema = "from-dsn"
	}
	log = log.With("chain_id", chainID, "schema", schema)

	log.Info("keys-api starting",
		"mode", mode, "source", envOr("KEYS_API_SOURCE", "lido-core"),
		"app_version", appVersion, "providers", redactedProviders(),
		"pin", pin, "ext_pin", extPin)

	// Only the writer applies the schema; a replica with missing tables is a deploy-order problem.
	if mode != "server" {
		if err := pgStore.Migrate(ctx); err != nil {
			return err
		}
		log.Info("schema applied")
	}

	var syncer *ingest.Syncer
	if mode != "server" {
		provider, rotateEndpoints, err := buildSource(ctx, chainID, log, m)
		if err != nil {
			return err
		}
		setEndpoints = rotateEndpoints

		// A node on the wrong chain returns plausible, wrong keys.
		if identifier, ok := provider.(source.ChainIdentifier); ok {
			actual, err := identifier.ChainID(ctx)
			if err != nil {
				log.Warn("could not verify the chain id", "err", err)
			} else if actual != 0 && actual != chainID {
				return fmt.Errorf("CHAIN_ID is %d but the source reports %d", chainID, actual)
			}
		}

		syncer = ingest.NewSyncer(provider, pgStore, ingest.SyncConfig{
			Interval:         time.Duration(envInt("UPDATE_KEYS_INTERVAL_MS", 5000)) * time.Millisecond,
			MaxBlockGap:      int64(envInt("MAX_BLOCKS_OVERLAP", 1000)),
			RetainVersions:   envInt("RETAIN_VERSIONS", 20),
			BuildAtFinalized: envBool("KEYS_API_BUILD_AT_FINALIZED", false),
			EpochDuration:    epochDuration(),
		}, log)
		syncer.OnCycle = func(r ingest.CycleResult) { observeCycle(m, r) }
		syncer.OnModule = func(r ingest.ModuleResult) { observeModule(m, r) }
	}

	errs := make(chan error, 2)

	if syncer != nil {
		go func() {
			log.Info("ingest starting",
				"source", envOr("KEYS_API_SOURCE", "lido-core"),
				"interval_ms", envInt("UPDATE_KEYS_INTERVAL_MS", 5000),
				"retain_versions", envInt("RETAIN_VERSIONS", 20),
				"epoch", epochDuration().String(),
				"metered", meteredEndpoints())
			errs <- syncer.Run(ctx)
		}()
	}

	var server *http.Server
	if mode != "ingest" {
		reader := store.NewAppWithPool(pgStore.Pool(), pin)
		reader.Log = log
		handler := api.New(reader, api.Config{
			ChainID:        chainID,
			AppVersion:     appVersion,
			ExtPin:         extPin,
			Log:            log,
			MetricsHandler: m.Handler(),
			Observe:        m.ObserveRequest,
		}).Handler()

		server = &http.Server{
			Addr:              ":" + envOr("PORT", "3000"),
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			// No write timeout: a full dump is 200 MiB and a slow consumer is not an error.
		}

		go func() {
			log.Info("http listening", "addr", server.Addr, "mode", mode)
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- err
				return
			}
			errs <- nil
		}()
	}

	// Rotation, once everything that could consume a credential exists.
	if len(appliedSecrets) > 0 {
		interval := time.Duration(envInt("SECRETS_POLL_INTERVAL_IN_SECONDS", int(secrets.DefaultPollInterval/time.Second))) * time.Second
		watcher := secrets.NewWatcher(secretsPath, interval,
			func(values map[string]string) error {
				return applyRotatedSecrets(values, appliedSecrets, setEndpoints, log, m)
			},
			func(err error) {
				log.Error("secrets rotation", "err", err)
				m.SecretsReloads.WithLabelValues("failure").Inc()
			})
		go watcher.Run(ctx)
	}

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errs:
		if err != nil && !errors.Is(err, context.Canceled) {
			stop()
			return err
		}
	}

	if server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Warn("http shutdown", "err", err)
		}
	}
	return nil
}

// applyRotatedSecrets: PROVIDERS_URLS applies live (the client copies endpoints per request). Other
// keys need a restart and are not recorded as applied, so they keep being reported.
func applyRotatedSecrets(
	values, applied map[string]string,
	setEndpoints func(string) error,
	log *slog.Logger,
	m *metrics.Metrics,
) error {
	var live, needRestart []string
	for key, value := range values {
		if value == applied[key] {
			continue
		}
		if key == "PROVIDERS_URLS" && setEndpoints != nil {
			if err := setEndpoints(value); err != nil {
				return fmt.Errorf("PROVIDERS_URLS: %w", err)
			}
			applied[key] = value
			if err := os.Setenv(key, value); err != nil {
				return err
			}
			live = append(live, key)
			continue
		}
		needRestart = append(needRestart, key)
	}

	if len(live) > 0 {
		sort.Strings(live)
		log.Info("applied rotated secrets", "keys", strings.Join(live, ","))
		m.SecretsReloads.WithLabelValues("success").Inc()
	}
	if len(needRestart) > 0 {
		sort.Strings(needRestart)
		log.Warn("rotated secrets need a restart to take effect", "keys", strings.Join(needRestart, ","))
		m.SecretsReloads.WithLabelValues("restart_required").Inc()
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

		// No compiled-in per-chain addresses: devnets are the main use case.
		locator := envOr("LIDO_LOCATOR", "")
		if locator == "" {
			return nil, nil, fmt.Errorf("LIDO_LOCATOR is required for KEYS_API_SOURCE=lido-core "+
				"(chain %d): the Lido Locator address is what everything else is resolved from, "+
				"and the deployed-contracts page at docs.lido.fi is the reference", chainID)
		}

		client := eth.NewClient(urls, eth.Options{
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
	m.CycleDuration.Observe(r.Duration.Seconds())
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
