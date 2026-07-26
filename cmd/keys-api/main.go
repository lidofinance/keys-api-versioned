// Command keys-api is the service: it syncs a registry into its own versioned
// schema and serves the keys-api surface from published versions.
//
// One process runs both halves by default, which is what makes it easy to drop in.
// They are separable: KEYS_API_MODE=server serves without syncing, and
// KEYS_API_MODE=ingest syncs without serving, so several read replicas can sit
// behind one writer. That separation is only possible because a version becomes
// current through a status change rather than through the arrival of its rows.
//
// Configuration is environment-only, and the names that exist in the canonical
// service are reused, so an existing deployment's variables mostly carry over:
//
//	PROVIDERS_URLS                    comma-separated execution endpoints
//	CHAIN_ID                          reported in /v1/status; verified against the node
//	DATABASE_URL                      Postgres DSN
//	PORT                              listen port (default 3000)
//	KEYS_API_MODE                     all (default) | server | ingest
//	KEYS_API_SOURCE                   lido-core (default) | static
//	KEYS_API_STATIC_FILE              registry JSON, for KEYS_API_SOURCE=static
//	LIDO_LOCATOR                      locator address; defaults per chain id
//	UPDATE_KEYS_INTERVAL_MS           cycle interval (default 5000)
//	MAX_BLOCKS_OVERLAP                full audit re-read when this far behind (default 1000)
//	RETAIN_VERSIONS                   retention window in versions (default 20)
//	PROVIDER_JSON_RPC_MAX_BATCH_SIZE  calls per JSON-RPC request (default 100)
//	PROVIDER_CONCURRENT_REQUESTS      in-flight requests (default 8)
//	KEYS_BATCH_SIZE                   keys per contract call (default 1100)
//	KEYS_CALLS_PER_REQUEST            key reads packed into one request (default 200)
//	KEYS_PER_REQUEST                  keys one request may return (default 2200)
//	KEYS_READ_CONCURRENCY             request groups in flight per module read
//	                                  (defaults to PROVIDER_CONCURRENT_REQUESTS)
//	PROVIDER_MULTICALL                pack reads into Multicall3 (default true)
//	PROVIDER_MULTICALL_ADDRESS        override the standard deployment address
//	PROVIDER_MULTICALL_MAX_CALLS      reads per aggregate (default 200)
//	PROVIDER_MULTICALL_MAX_KEYS       keys one aggregate may return (default 1000)
//	LOG_LEVEL                         debug | info (default) | warn | error
//
// Multicall3 is a contract, not a node feature, so it needs no support from the
// endpoint — but what an endpoint tolerates in one eth_call does vary, and a hosted
// provider caps it lower than a node of our own. Nothing has to be configured for
// that: presence is probed, a refused aggregate is bisected and the budget lowered
// to what worked, and an endpoint that refuses even one aggregated read turns
// aggregation off for the process. PROVIDER_MULTICALL=false is the manual escape
// hatch, and with it off the ingest behaves as it did before aggregation existed.
//
// LOG_LEVEL=debug is worth knowing about: it adds one line per JSON-RPC request, a
// staging heartbeat every five seconds during a long module read, the freshness
// decision with the nonces it was made from, and the resolved pin plus row counts
// per served request. A cold sync at info level says nothing for two minutes at a
// time, which makes a slow node look like a hang.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/api"
	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/metrics"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
	"github.com/lidofinance/keys-api-versioned/internal/source/static"
	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// appVersion is reported in /v1/status. Consumers read it, so it stays a plain
// semantic version rather than a build hash.
const appVersion = "0.1.0-poc"

// locatorByChain holds the Lido Locator per chain — the one address everything
// else is resolved from. These are the two the canonical service carries; any other
// chain, devnets included, sets LIDO_LOCATOR explicitly rather than getting a
// guess.
var locatorByChain = map[int64]string{
	1:      "0xc1d0b3de6792bf6b4b37eccdcc24e45978cfd2eb", // mainnet
	560048: "0xe2ef9536daaaebff5b1c130957ab3e80056b06d8", // hoodi
}

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

	mode := envOr("KEYS_API_MODE", "all")
	if mode != "all" && mode != "server" && mode != "ingest" {
		return fmt.Errorf("KEYS_API_MODE must be all, server or ingest, got %q", mode)
	}

	dsn := envOr("DATABASE_URL", "")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}

	m := metrics.New("keys_api")

	pgStore, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{
		MaxConns: int32(envInt("DATABASE_MAX_CONNS", 10)),
		Source:   envOr("KEYS_API_SOURCE", "lido-core"),
	})
	if err != nil {
		return err
	}
	defer pgStore.Close()

	// The schema is applied by whoever writes. A read-only replica does not create
	// tables — if they are missing, that is a deployment order problem and it should
	// surface as one.
	if mode != "server" {
		if err := pgStore.Migrate(ctx); err != nil {
			return err
		}
		log.Info("schema applied")
	}

	chainID := int64(envInt("CHAIN_ID", 1))

	var syncer *ingest.Syncer
	if mode != "server" {
		provider, err := buildSource(ctx, chainID, log)
		if err != nil {
			return err
		}

		// A node on the wrong chain is the failure worth catching at startup: every
		// key it returns would be plausible and wrong.
		if identifier, ok := provider.(source.ChainIdentifier); ok {
			actual, err := identifier.ChainID(ctx)
			if err != nil {
				log.Warn("could not verify the chain id", "err", err)
			} else if actual != 0 && actual != chainID {
				return fmt.Errorf("CHAIN_ID is %d but the source reports %d", chainID, actual)
			}
		}

		syncer = ingest.NewSyncer(provider, pgStore, ingest.SyncConfig{
			Interval:       time.Duration(envInt("UPDATE_KEYS_INTERVAL_MS", 5000)) * time.Millisecond,
			MaxBlockGap:    int64(envInt("MAX_BLOCKS_OVERLAP", 1000)),
			RetainVersions: envInt("RETAIN_VERSIONS", 20),
		}, log)
		syncer.OnCycle = func(r ingest.CycleResult) { observeCycle(m, r) }
	}

	errs := make(chan error, 2)

	if syncer != nil {
		go func() {
			log.Info("ingest starting",
				"source", envOr("KEYS_API_SOURCE", "lido-core"),
				"interval_ms", envInt("UPDATE_KEYS_INTERVAL_MS", 5000),
				"retain_versions", envInt("RETAIN_VERSIONS", 20))
			errs <- syncer.Run(ctx)
		}()
	}

	var server *http.Server
	if mode != "ingest" {
		// The reader shares the writer's pool: one process, one set of connections.
		reader := store.NewAppWithPool(pgStore.Pool(), envOr("KEYS_API_PIN", "latest"))
		reader.Log = log
		handler := api.New(reader, api.Config{
			ChainID:        chainID,
			AppVersion:     appVersion,
			Log:            log,
			MetricsHandler: m.Handler(),
			Observe:        m.ObserveRequest,
		}).Handler()

		server = &http.Server{
			Addr:              ":" + envOr("PORT", "3000"),
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			// No write timeout: a full dump is 200 MiB and a slow consumer is not an
			// error. The canonical service's 60-second stream timeout is the thing
			// this deliberately does not copy.
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

// buildSource picks the registry reader. This is the one place that knows which
// implementations exist; everything downstream sees source.Provider.
func buildSource(ctx context.Context, chainID int64, log *slog.Logger) (source.Provider, error) {
	switch name := envOr("KEYS_API_SOURCE", "lido-core"); name {
	case "lido-core":
		urls := envOr("PROVIDERS_URLS", "")
		if urls == "" {
			return nil, errors.New("PROVIDERS_URLS is required for KEYS_API_SOURCE=lido-core")
		}

		locator := envOr("LIDO_LOCATOR", locatorByChain[chainID])
		if locator == "" {
			return nil, fmt.Errorf("no known locator for chain %d: set LIDO_LOCATOR", chainID)
		}

		client := eth.NewClient(urls, eth.Options{
			Concurrency:       envInt("PROVIDER_CONCURRENT_REQUESTS", 8),
			BatchSize:         envInt("PROVIDER_JSON_RPC_MAX_BATCH_SIZE", 100),
			Timeout:           time.Duration(envInt("PROVIDER_TIMEOUT_MS", 120_000)) * time.Millisecond,
			Multicall:         envBool("PROVIDER_MULTICALL", true),
			MulticallAddress:  envOr("PROVIDER_MULTICALL_ADDRESS", eth.MulticallAddress),
			MulticallMaxCalls: envInt("PROVIDER_MULTICALL_MAX_CALLS", 0),
			MulticallMaxCost:  envInt("PROVIDER_MULTICALL_MAX_KEYS", 0),
			// At LOG_LEVEL=debug this prints one line per JSON-RPC request, which is
			// how a slow sync becomes legible instead of silent.
			Log: log,
		})
		provider := lidocore.NewProvider(client, locator, lidocore.Options{
			KeysBatch:       uint64(envInt("KEYS_BATCH_SIZE", lidocore.KeysBatchSize)),
			CallsPerRequest: envInt("KEYS_CALLS_PER_REQUEST", lidocore.CallsPerRequest),
			KeysPerRequest:  uint64(envInt("KEYS_PER_REQUEST", lidocore.KeysPerRequest)),
			// How many request groups a module read keeps in flight. It defaults to the
			// client's own limit because that is the ceiling anyway — the semaphore in
			// the client is what actually bounds requests — and is separable for the
			// case where the two want different values: many groups queued against a
			// node that tolerates few concurrent requests.
			Concurrency: envInt("KEYS_READ_CONCURRENCY", envInt("PROVIDER_CONCURRENT_REQUESTS", 8)),
			Log:         log,
		})
		log.Info("source: lido core",
			"locator", locator, "module_types", strings.Join(provider.SupportedTypes(), ","))
		return provider, nil

	case "static":
		path := envOr("KEYS_API_STATIC_FILE", "")
		if path == "" {
			return nil, errors.New("KEYS_API_STATIC_FILE is required for KEYS_API_SOURCE=static")
		}
		provider := static.New(path)
		if _, err := provider.Head(ctx); err != nil {
			return nil, fmt.Errorf("static source: %w", err)
		}
		log.Info("source: static file", "path", path)
		return provider, nil

	default:
		return nil, fmt.Errorf("unknown KEYS_API_SOURCE %q", name)
	}
}

// observeCycle turns a cycle result into metrics. It lives here rather than in the
// ingest package so that package keeps no metrics dependency.
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
	// Two different ages, and only one of them is an alert.
	//
	// VersionAge is how old the data is. It climbs whenever the chain is quiet,
	// because a version that nothing changed is not rebuilt — so alerting on it would
	// page someone for the registry being calm.
	//
	// ConfirmedAge is how long since a cycle last confirmed the version against the
	// chain. It stays near the poll interval while the loop is healthy and climbs the
	// moment it is not, whatever the cause: a stalled node, a failing rebuild, a dead
	// writer. That is the one to alert on.
	if r.VersionTimestamp > 0 {
		// A source may have no block time at all (a file does not), in which case the
		// age would be meaningless rather than alarming, so it stays unset.
		m.VersionAge.Set(float64(time.Now().Unix() - r.VersionTimestamp))
	}
	if r.Err == nil && r.HeadTimestamp > 0 {
		m.ConfirmedAge.Set(float64(time.Now().Unix() - r.HeadTimestamp))
	}
	if r.Rebuilt {
		m.RebuildsTotal.Inc()
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
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
