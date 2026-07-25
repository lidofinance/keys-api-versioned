// Command ingest reads the registry from an execution node and reports what it
// found, without writing anything.
//
// The service itself (cmd/keys-api) does the writing. This stays as the probe for
// the read half: it answers "can we reach the chain, do the module types resolve,
// how fast do keys come back" in one command, which is what you want when a node is
// misbehaving and you do not want to involve a database.
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
const mainnetLocator = "0xc1d0b3de6792bf6b4b37eccdcc24e45978cfd2eb"

func main() {
	var (
		rpc         = flag.String("rpc", envOr("PROVIDERS_URLS", ""), "comma separated execution endpoints")
		locator     = flag.String("locator", envOr("LIDO_LOCATOR", mainnetLocator), "Lido Locator address")
		modulesOnly = flag.Bool("modules-only", false, "stop after reading modules and nonces")
		moduleID    = flag.Int("module", 0, "read one module id only; 0 means every module")
		withOps     = flag.Bool("operators", false, "also print operator counts per module")
		batchSize   = flag.Int("rpc-batch", 20, "calls per JSON-RPC request")
		concurrency = flag.Int("rpc-concurrency", 4, "in-flight JSON-RPC requests")
		keysBatch   = flag.Uint64("keys-batch", lidocore.KeysBatchSize, "keys per contract call")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *rpc == "" {
		log.Error("no execution endpoint: pass -rpc or set PROVIDERS_URLS")
		os.Exit(2)
	}

	ctx := context.Background()
	client := eth.NewClient(*rpc, eth.Options{
		Concurrency: *concurrency,
		BatchSize:   *batchSize,
		Timeout:     2 * time.Minute,
	})
	provider := lidocore.NewProvider(client, *locator, lidocore.Options{
		KeysBatch:   *keysBatch,
		Concurrency: *concurrency,
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
		log.Info("keys read",
			"module", m.ID,
			"keys", count,
			"used", used,
			"vetted", vetted,
			"took", took.Round(time.Millisecond).String(),
			"keys/s", int(float64(count)/took.Seconds()))
		if count > 0 {
			fmt.Printf("    first key: operator %d index %d used %t vetted %t\n      %s\n      %s\n",
				sample.OperatorIndex, sample.Index, sample.Used, sample.Vetted,
				model.EncodeHex(sample.PubkeyBin), model.EncodeHex(sample.SignatureBin))
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
