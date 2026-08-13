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
//	CHAIN_ID                          reported in /v1/status; verified against the node,
//	                                  and names the schema the store lives in
//	DATABASE_URL                      Postgres DSN
//	PORT                              listen port (default 3000)
//	KEYS_API_MODE                     all (default) | server | ingest
//	KEYS_API_SOURCE                   lido-core (default) | static
//	KEYS_API_STATIC_FILE              registry JSON, for KEYS_API_SOURCE=static
//	LIDO_LOCATOR                      Lido Locator address; required for lido-core
//	UPDATE_KEYS_INTERVAL_MS           cycle interval (default 5000)
//	MAX_BLOCKS_OVERLAP                full audit re-read when this far behind (default 1000)
//	RETAIN_VERSIONS                   retention window in versions (default 20)
//	KEYS_API_BUILD_AT_FINALIZED       read the registry at the finalized block instead of
//	                                  the head (default false); costs ~13 min of freshness
//	                                  and buys versions a reorg cannot take away
//	KEYS_API_PIN                      default pin on /v1 and /v2: latest (default) or
//	                                  finalized
//	KEYS_API_EXT_PIN                  default pin on /ext/v1: finalized (default, latest
//	                                  for a source with no finality), overridden per
//	                                  request by ?snapshot=
//	PROVIDER_JSON_RPC_MAX_BATCH_SIZE  calls per JSON-RPC request (default 100)
//	PROVIDER_CONCURRENT_REQUESTS      in-flight requests (default 8)
//	KEYS_BATCH_SIZE                   keys per contract call (default 1100)
//	KEYS_CALLS_PER_REQUEST            key reads packed into one request (default 200)
//	KEYS_PER_REQUEST                  keys one request may return (default 2200)
//	KEYS_READ_CONCURRENCY             request groups in flight per module read
//	                                  (defaults to PROVIDER_CONCURRENT_REQUESTS)
//	PROVIDER_MULTICALL                pack reads into Multicall3 (default true)
//	PROVIDER_METERED                  the endpoints bill per call, not per round trip
//	                                  (default true); aggregates every multi-call read.
//	                                  Set false only against a node of our own, where the
//	                                  plain path is marginally faster
//	SECONDS_PER_SLOT                  slot time, for the epoch length (default 12)
//	SLOTS_PER_EPOCH                   slots in an epoch, for the epoch length (default 32)
//	PROVIDER_MULTICALL_ADDRESS        override the standard deployment address
//	PROVIDER_MULTICALL_MAX_CALLS      reads per aggregate (default 200)
//	PROVIDER_MULTICALL_MAX_KEYS       keys one aggregate may return (default 1000)
//	KEYS_API_ENV                      the `env` label on build_info (default unknown)
//	LOG_LEVEL                         debug | info (default) | warn | error
//
// /metrics carries two families. This service's own series are under keys_api_*, and
// the canonical service's are exported under the canonical names as well, so its
// dashboards and alerts work here unchanged — see internal/metrics for what differs.
// keys_api_rpc_requests_total is the one to watch on a metered endpoint: it counts
// round trips by method, which is what a provider bills.
//
// Multicall3 is a contract, not a node feature, so it needs no support from the
// endpoint — but what an endpoint tolerates in one eth_call does vary, and a hosted
// provider caps it lower than a node of our own. Nothing has to be configured for
// that: presence is probed, a refused aggregate is bisected and the budget lowered
// to what worked, and an endpoint that refuses even one aggregated read turns
// aggregation off for the process. PROVIDER_MULTICALL=false is the manual escape
// hatch, and with it off the ingest behaves as it did before aggregation existed.
//
// Every line carries chain_id and the schema the store lives in, because an
// investigation reads arbitrary lines rather than the startup one — and "which chain was
// this?" is the question a misconfigured deployment makes expensive to answer. The
// endpoint hosts are logged once at startup, without paths: a hosted provider's URL
// carries its API key there.
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
	"net/url"
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

// metricsNamespace prefixes this service's own series. The canonical service's are
// exported alongside them under their own names — see internal/metrics.
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

	mode := envOr("KEYS_API_MODE", "all")
	if mode != "all" && mode != "server" && mode != "ingest" {
		return fmt.Errorf("KEYS_API_MODE must be all, server or ingest, got %q", mode)
	}

	dsn := envOr("DATABASE_URL", "")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}

	// The two serving defaults, resolved before anything starts: a pin that does not
	// parse would otherwise answer 400 to every request, which reads like a client
	// problem rather than a deployment one.
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

	// build_info is the canonical service's, and its `network` label is the chain: a
	// dashboard filters on it, so it is the chain id rather than a deployment name.
	m.SetBuildInfo("keys-api-versioned", appVersion,
		envOr("KEYS_API_ENV", "unknown"), strconv.FormatInt(chainID, 10))

	// The store lives in a schema named after the chain, so a database filled from one
	// chain cannot be opened as another. Without it, that mistake is silent and
	// destructive: the loop reads the stored version's height on the new chain, finds a
	// different hash, calls it a reorg deeper than the whole window, and rebuilds from
	// scratch. See chainSchema in internal/ingest for the full argument.
	pgStore, err := ingest.NewPGStore(ctx, dsn, ingest.PGOptions{
		MaxConns: int32(envInt("DATABASE_MAX_CONNS", 10)),
		Source:   envOr("KEYS_API_SOURCE", "lido-core"),
		ChainID:  chainID,
	})
	if err != nil {
		return err
	}
	defer pgStore.Close()

	// Which chain and which schema, on every line from here on.
	//
	// An investigation reads arbitrary lines — a cycle decision, an RPC failure, a reorg
	// warning — and correlating each one back to a startup line is the step that gets
	// skipped. Two attributes on the base logger cost nothing and make "which chain was
	// this?" answerable from any line, which is the question that took the longest to
	// answer the one time this went wrong: a mainnet node against a Hoodi store looks
	// entirely healthy in the log.
	schema := pgStore.Schema()
	if schema == "" {
		schema = "from-dsn"
	}
	log = log.With("chain_id", chainID, "schema", schema)

	log.Info("keys-api starting",
		"mode", mode, "source", envOr("KEYS_API_SOURCE", "lido-core"),
		"app_version", appVersion, "providers", redactedProviders(),
		// Which version each surface serves by default is the first thing to check when
		// a consumer reports data older than its own node's head.
		"pin", pin, "ext_pin", extPin)

	// The schema is applied by whoever writes. A read-only replica does not create
	// tables — if they are missing, that is a deployment order problem and it should
	// surface as one.
	if mode != "server" {
		if err := pgStore.Migrate(ctx); err != nil {
			return err
		}
		log.Info("schema applied")
	}

	var syncer *ingest.Syncer
	if mode != "server" {
		provider, err := buildSource(ctx, chainID, log, m)
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
			// Default head, so upgrading changes nothing a consumer can see. Flipping
			// this is what a deployment does when it wants data that cannot be reorged
			// away and can afford to be a finality behind.
			BuildAtFinalized: envBool("KEYS_API_BUILD_AT_FINALIZED", false),
			// One epoch, computed rather than fetched: this service has no consensus
			// client, and it needs a duration rather than a head. Mainnet's 12 × 32 is
			// the default; a devnet with a shorter slot says so, and either way the
			// number in the log is the one being used.
			EpochDuration: epochDuration(),
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
				// Logged because it paces two reads and is derived from two other
				// variables: "why has it not scanned for six minutes" should be
				// answerable from the startup line.
				"epoch", epochDuration().String(),
				"metered", meteredEndpoints())
			errs <- syncer.Run(ctx)
		}()
	}

	var server *http.Server
	if mode != "ingest" {
		// The reader shares the writer's pool: one process, one set of connections.
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
func buildSource(ctx context.Context, chainID int64, log *slog.Logger, m *metrics.Metrics) (source.Provider, error) {
	switch name := envOr("KEYS_API_SOURCE", "lido-core"); name {
	case "lido-core":
		urls := envOr("PROVIDERS_URLS", "")
		if urls == "" {
			return nil, errors.New("PROVIDERS_URLS is required for KEYS_API_SOURCE=lido-core")
		}

		// No per-chain defaults in the binary. An address compiled in means a redeploy
		// to correct a wrong one, and a map of two chains quietly asserts that those are
		// the only chains this service runs on — which is false: devnets are the case it
		// is used on most. It comes from the environment, or the service refuses to start.
		locator := envOr("LIDO_LOCATOR", "")
		if locator == "" {
			return nil, fmt.Errorf("LIDO_LOCATOR is required for KEYS_API_SOURCE=lido-core "+
				"(chain %d): the Lido Locator address is what everything else is resolved from, "+
				"and the deployed-contracts page at docs.lido.fi is the reference", chainID)
		}

		client := eth.NewClient(urls, eth.Options{
			Concurrency: envInt("PROVIDER_CONCURRENT_REQUESTS", 8),
			BatchSize:   envInt("PROVIDER_JSON_RPC_MAX_BATCH_SIZE", 100),
			Timeout:     time.Duration(envInt("PROVIDER_TIMEOUT_MS", 120_000)) * time.Millisecond,
			Multicall:   envBool("PROVIDER_MULTICALL", true),
			// Metered flips the aggregation rule: a provider that bills per call makes one
			// aggregate3 cheaper than a JSON-RPC batch of the same reads, which against a
			// node of our own is the slower of the two. A flag rather than a list of known
			// provider hosts — the list would go stale, and a deployment knows what it
			// pays for.
			//
			// Default true, because the two mistakes cost differently. Forgetting it on a
			// hosted provider spends five times the compute units on every cycle forever,
			// which is what emptied a 2 000 000/day key five hours into the day and took
			// both writers down with it. Leaving it on against a node of our own costs the
			// dispatcher: 0.99 s against 0.87 s on a quiet cycle, measured. One is an
			// outage, the other is 120 ms.
			Metered:           meteredEndpoints(),
			MulticallAddress:  envOr("PROVIDER_MULTICALL_ADDRESS", eth.MulticallAddress),
			MulticallMaxCalls: envInt("PROVIDER_MULTICALL_MAX_CALLS", 0),
			MulticallMaxCost:  envInt("PROVIDER_MULTICALL_MAX_KEYS", 0),
			// At LOG_LEVEL=debug this prints one line per JSON-RPC request, which is
			// how a slow sync becomes legible instead of silent.
			Log: log,
			// And at every level, one metric per request. The debug line answers
			// "what is this sync waiting on" for someone watching; this answers "how
			// many requests is this deployment spending" for someone who was not.
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
			// How many request groups a module read keeps in flight. It defaults to the
			// client's own limit because that is the ceiling anyway — the semaphore in
			// the client is what actually bounds requests — and is separable for the
			// case where the two want different values: many groups queued against a
			// node that tolerates few concurrent requests.
			Concurrency: envInt("KEYS_READ_CONCURRENCY", envInt("PROVIDER_CONCURRENT_REQUESTS", 8)),
			Log:         log,
			// A router replacement is rare and consequential, so it gets a counter of
			// its own rather than being left in the log: the registry changing
			// completely is otherwise indistinguishable from a bug.
			OnRouterChange: func(previous, current string) { m.RouterChanges.Inc() },
			// The version is a gauge because its value is the fact — which shape the
			// tuple has — and a counter alongside it because "it changed" is the event.
			OnContractVersion: func(previous, current int64) {
				m.RouterVersion.Set(float64(current))
				m.VersionChanges.Inc()
			},
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
// meteredEndpoints reports whether the endpoints bill per call rather than per round
// trip, which is what decides between one aggregate3 and N plain eth_calls.
//
// Default true, because the two mistakes cost differently. Forgetting it on a hosted
// provider spends five times the compute units on every cycle forever — that is what
// emptied a 2 000 000/day key five hours into the day and took both writers down with
// it. Leaving it on against a node of our own costs the dispatcher it executes instead
// of answering directly: 0.99 s against 0.87 s on a quiet cycle, measured. One of those
// is an outage and the other is 120 ms.
//
// A function rather than two envBool calls, because there were two and they disagreed:
// the startup line said metered=false while the client was built metered. A log that
// contradicts the behaviour is worse than no log.
func meteredEndpoints() bool { return envBool("PROVIDER_METERED", true) }

// epochDuration is one beacon epoch: slot time times slots per epoch.
//
// Multiplied here rather than read from a consensus API because what the loop wants is a
// duration, not a head — and adding a CL dependency to obtain two constants would be the
// larger cost. Both are the chain's own parameters, so a devnet with a two-second slot
// configures them and nothing else changes.
//
// Zero from either variable disables the epoch pacing, which restores the previous
// behaviour of reading the finalized block and scanning operator events on every cycle.
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
	// The audit is the only check on the assumption every cheap signal rests on — that a
	// module's nonce moves whenever its keys do. A healthy audit re-reads half a million
	// keys and writes nothing, which is a verifiable statement rather than a belief; one
	// that writes rows says a key was added or removed without the counter moving, and
	// nothing else in the service would ever notice.
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

// canonicalJobName is what the canonical service labels this work with. Copied
// exactly, spaces included: a dashboard selects on the string.
const canonicalJobName = "Update Staking Router Modules keys"

// observeCanonicalCycle fills the canonical service's per-cycle series.
//
// last_block_number and last_update_timestamp are the head this cycle confirmed
// against, which is what the canonical el_meta row holds after an iteration — not the
// published version's own block. The two differ here in a way they cannot there,
// because a version that nothing changed is kept rather than rewritten; the
// version's own block is published_version_block, which has no canonical
// counterpart.
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

// observeModule fills the canonical per-module series from a rebuild.
func observeModule(m *metrics.Metrics, r ingest.ModuleResult) {
	// Only when the keys were actually read: an operators-only rebuild would otherwise
	// record a key update of zero keys, which reads as a module that has none.
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

// redactedProviders lists the endpoint hosts, without paths.
//
// Which node the service is talking to is the other half of "which chain" during an
// investigation, and it is the half that catches a variable picked out of the wrong
// place — a .env file, say. Hosts only, because a hosted provider's URL carries its API
// key in the path and a log line is the wrong place for it. Same reduction the JSON-RPC
// client already does for its own per-request lines.
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

// pinOption reads a surface's default pin and accepts only a label.
//
// A block number or a hash is a reasonable pin for one request and a bad one for a whole
// deployment: retention deletes that version within minutes and the service would then
// answer 410 to everything. A misspelt label is worth refusing here too — as a default it
// turns every request into a 400 about a value the consumer never sent.
func pinOption(name, def string) (string, error) {
	pin := envOr(name, def)
	if pin != "latest" && pin != "finalized" {
		return "", fmt.Errorf("%s must be latest or finalized, got %q", name, pin)
	}
	return pin, nil
}

// defaultExtPin is the pin /ext/v1 serves from when the deployment does not say.
//
// finalized, except for a source that cannot produce it: a file-backed registry records
// no finalized block ever, and there the pin answers 501 by design. Defaulting to it
// would take the whole extended surface down over a value the operator did not choose,
// so that source falls back to the head — where the file's contents are, in any case,
// the only thing there is.
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
