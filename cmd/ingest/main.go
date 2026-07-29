// Command ingest reads the registry from an execution node and reports what it
// found, without writing anything.
//
// The service itself (cmd/keys-api) does the writing. This stays as the probe for
// the read half: it answers "can we reach the chain, do the module types resolve,
// how fast do keys come back" in one command, which is what you want when a node is
// misbehaving and you do not want to involve a database.
//
// It is also where ingest throughput is measured, because it reports round trips
// per module alongside the keys. A cold sync is bound by requests rather than
// bytes, so the comparison worth running against a given endpoint is
// -multicall=false against the default, and the number to read is calls per
// request: 1.0 means every read cost its own round trip.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
	"github.com/lidofinance/keys-api-versioned/internal/source/lidocore"
)

// Mainnet Lido Locator: the address the canonical service carries for chain 1.
// No compiled-in address: see the note in cmd/keys-api. The flag or the environment
// supplies it, or the probe says which one it wanted.

func main() {
	var (
		rpc         = flag.String("rpc", envOr("PROVIDERS_URLS", ""), "comma separated execution endpoints")
		locator     = flag.String("locator", envOr("LIDO_LOCATOR", ""), "Lido Locator address (required)")
		modulesOnly = flag.Bool("modules-only", false, "stop after reading modules and nonces")
		moduleID    = flag.Int("module", 0, "read one module id only; 0 means every module")
		withOps     = flag.Bool("operators", false, "also print operator counts per module")
		batchSize   = flag.Int("rpc-batch", 20, "calls per JSON-RPC request")
		concurrency = flag.Int("rpc-concurrency", 4, "in-flight JSON-RPC requests")
		keysBatch   = flag.Uint64("keys-batch", lidocore.KeysBatchSize, "keys per contract call")
		keysWorkers = flag.Int("keys-concurrency", 0, "request groups in flight per module; 0 follows -rpc-concurrency")
		callsPerReq = flag.Int("calls-per-request", lidocore.CallsPerRequest, "key reads packed into one request")
		keysPerReq  = flag.Uint64("keys-per-request", lidocore.KeysPerRequest, "keys one request may return")
		multicall   = flag.Bool("multicall", true, "pack reads into one Multicall3.aggregate3 per request")
		mcAddress   = flag.String("multicall-address", eth.MulticallAddress, "Multicall3 address")
		mcMaxCalls  = flag.Int("multicall-max-calls", 0, "reads per aggregate; 0 uses the default")
		mcMaxCost   = flag.Int("multicall-max-cost", 0, "keys one aggregate may return; 0 uses the default")
		verbose     = flag.Bool("v", false, "debug logging: one line per JSON-RPC request")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if *rpc == "" {
		log.Error("no execution endpoint: pass -rpc or set PROVIDERS_URLS")
		os.Exit(2)
	}
	if *locator == "" {
		log.Error("no locator: pass -locator or set LIDO_LOCATOR",
			"reference", "the deployed-contracts page at docs.lido.fi")
		os.Exit(2)
	}

	ctx := context.Background()
	client := eth.NewClient(*rpc, eth.Options{
		Concurrency:       *concurrency,
		BatchSize:         *batchSize,
		Timeout:           2 * time.Minute,
		Multicall:         *multicall,
		MulticallAddress:  *mcAddress,
		MulticallMaxCalls: *mcMaxCalls,
		MulticallMaxCost:  *mcMaxCost,
		Log:               log,
	})
	readWorkers := *keysWorkers
	if readWorkers <= 0 {
		readWorkers = *concurrency
	}
	provider := lidocore.NewProvider(client, *locator, lidocore.Options{
		KeysBatch:       *keysBatch,
		CallsPerRequest: *callsPerReq,
		KeysPerRequest:  *keysPerReq,
		Concurrency:     readWorkers,
		Log:             log,
	})

	head, err := provider.Head(ctx)
	if err != nil {
		log.Error("read head", "err", err)
		os.Exit(1)
	}
	log.Info("head", "block", head.BlockNumber, "hash", head.BlockHash)

	at := source.Ref{BlockHash: head.BlockHash, BlockNumber: head.BlockNumber}

	started := time.Now()
	modules, err := provider.Modules(ctx, at)
	if err != nil {
		log.Error("read modules", "err", err)
		os.Exit(1)
	}
	log.Info("modules read", "count", len(modules), "took", time.Since(started).Round(time.Millisecond).String())

	for _, m := range modules {
		fmt.Printf("  id %d  %-22s %-22s nonce %-7d wc %d  %s\n",
			m.ID, m.Name, m.Type, m.Nonce, m.WithdrawalCredentialsType, m.RawAddress())
	}

	if *modulesOnly {
		return
	}

	for _, m := range modules {
		if *moduleID != 0 && int(m.ID) != *moduleID {
			continue
		}

		if *withOps {
			started := time.Now()
			operators, err := provider.Operators(ctx, at, m)
			if err != nil {
				log.Error("read operators", "module", m.ID, "err", err)
				continue
			}
			log.Info("operators read", "module", m.ID, "count", len(operators),
				"took", time.Since(started).Round(time.Millisecond).String())
		}

		started := time.Now()
		before := client.Stats()
		var (
			count  int
			used   int
			vetted int
			sample model.Key
		)
		err := provider.Keys(ctx, at, m, func(k model.Key) error {
			if count == 0 {
				sample = k
			}
			count++
			if k.Used {
				used++
			}
			if k.Vetted {
				vetted++
			}
			return nil
		})
		if err != nil {
			log.Error("read keys", "module", m.ID, "err", err)
			continue
		}

		took := time.Since(started)
		spent := since(before, client.Stats())
		log.Info("keys read",
			"module", m.ID,
			"keys", count,
			"used", used,
			"vetted", vetted,
			"took", took.Round(time.Millisecond).String(),
			"keys/s", int(float64(count)/took.Seconds()),
			// Round trips are the figure to watch: this module's cost is latency ×
			// requests far more than it is bytes.
			"requests", spent.Requests,
			"calls", spent.Calls,
			"aggregates", spent.Aggregates,
			"aggregates_split", spent.Splits,
			"aggregate_fallbacks", spent.Fallbacks)
		if count > 0 {
			fmt.Printf("    first key: operator %d index %d used %t vetted %t\n      %s\n      %s\n",
				sample.OperatorIndex, sample.Index, sample.Used, sample.Vetted,
				model.EncodeHex(sample.PubkeyBin), model.EncodeHex(sample.SignatureBin))
		}
	}

	total := client.Stats()
	log.Info("rpc totals",
		"requests", total.Requests, "calls", total.Calls,
		"calls_per_request", ratio(total.Calls, total.Requests),
		"aggregates", total.Aggregates, "aggregates_split", total.Splits,
		"aggregate_fallbacks", total.Fallbacks)
}

// ratio renders calls per request to one decimal, which is the packing figure: 1.0
// means every read cost its own round trip.
func ratio(calls, requests int64) string {
	if requests == 0 {
		return "0"
	}
	return fmt.Sprintf("%.1f", float64(calls)/float64(requests))
}

// since is the difference between two counter snapshots, so a per-module line
// reports that module's cost rather than the process total.
func since(before, after eth.Stats) eth.Stats {
	return eth.Stats{
		Requests:   after.Requests - before.Requests,
		Calls:      after.Calls - before.Calls,
		Aggregates: after.Aggregates - before.Aggregates,
		Splits:     after.Splits - before.Splits,
		Fallbacks:  after.Fallbacks - before.Fallbacks,
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
