// Package lidocore reads the Lido core registry — staking router modules — from
// an execution node.
//
// This is the only package in the service that knows what a staking router is.
// It implements source.Provider, and the call sequence is the canonical
// lido-keys-api's own, using its ABI files verbatim:
//
//	Locator.stakingRouter → StakingRouter.getStakingModules → per module
//	IStakingModule.getType → dispatch to the reader registered for that type
//
// Two levels of pluggability, and they are different sizes:
//
//   - A new module family inside Lido core is a ModuleReader, registered by the
//     type string getType returns. Curated and community are two of these.
//   - A different registry entirely — stVaults, whose keys come from vaults
//     rather than from staking modules — is a sibling of this package. It reads
//     its own contracts and fills the same schema through the same interface.
//     Nothing above source changes.
package lidocore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// Provider reads the registry from an execution node.
type Provider struct {
	client  *eth.Client
	locator string
	readers map[string]ModuleReader
	log     *slog.Logger

	// routerCache holds the resolved staking router address. The locator is
	// upgradeable, so it is keyed by nothing and refreshed on error rather than
	// pinned forever; in practice it changes about once a year.
	routerMu sync.Mutex
	router   string
}

// ModuleReader reads one family of module contracts: operators, keys, and the
// log topics that signal operator metadata changes.
//
// The two implementations here cover what mainnet has today — the curated
// registry (NOR) and the CSM-shaped contracts used by community-onchain-v1 and
// curated-onchain-v2 — which is the same split the canonical service makes.
type ModuleReader interface {
	// Types lists the module types this reader handles, as getType returns them.
	Types() []string
	// Operators reads every operator of the module at a block.
	Operators(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string) ([]model.Operator, error)
	// Keys streams the module's keys. Operators are passed in because both
	// implementations need their limits, and reading them twice is wasteful.
	Keys(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string, operators []model.Operator, fn func(model.Key) error) error
	// OperatorEventTopics are the events that change operator metadata without
	// moving the module nonce.
	OperatorEventTopics() []string
}

// Options configure pacing and which readers are available.
type Options struct {
	// KeysBatch is how many keys one contract call asks for.
	KeysBatch uint64
	// CallsPerRequest is how many key reads are packed into one request group, and
	// KeysPerRequest caps how many keys those may return in total. Together they
	// decide how much of the per-operator round-trip cost is paid: mainnet's 731
	// operators cost 731 requests at 1, and a few dozen at 200.
	CallsPerRequest int
	KeysPerRequest  uint64
	// Concurrency bounds in-flight request groups per module.
	Concurrency int
	// Readers overrides the default reader set. Empty means curated + community.
	Readers []ModuleReader
	// Log receives debug lines for the chain reads: which contracts, how many
	// operators, how long each stage took.
	Log *slog.Logger
}

// NewProvider builds a provider over an execution client.
func NewProvider(client *eth.Client, locator string, o Options) *Provider {
	if o.KeysBatch == 0 {
		o.KeysBatch = KeysBatchSize
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}

	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	readers := o.Readers
	if len(readers) == 0 {
		readers = []ModuleReader{
			&CuratedReader{
				KeysBatch: o.KeysBatch, CallsPerRequest: o.CallsPerRequest,
				KeysPerRequest: o.KeysPerRequest, Concurrency: o.Concurrency, Log: o.Log,
			},
			&CommunityReader{
				KeysBatch: o.KeysBatch, CallsPerRequest: o.CallsPerRequest,
				KeysPerRequest: o.KeysPerRequest, Concurrency: o.Concurrency, Log: o.Log,
			},
		}
	}

	p := &Provider{client: client, locator: strings.ToLower(locator), readers: map[string]ModuleReader{}, log: o.Log}
	for _, r := range readers {
		for _, t := range r.Types() {
			p.readers[t] = r
		}
	}
	return p
}

func (p *Provider) Name() string { return "execution-layer" }

// SupportedTypes lists the module types this provider can read, for startup logs.
func (p *Provider) SupportedTypes() []string {
	out := make([]string, 0, len(p.readers))
	for t := range p.readers {
		out = append(out, t)
	}
	return out
}

// ChainID reports the chain the endpoints are on, so a misconfigured node is
// caught at startup rather than by a consumer noticing wrong keys.
func (p *Provider) ChainID(ctx context.Context) (int64, error) {
	return p.client.ChainID(ctx)
}

// Head returns the block a cycle should read at.
func (p *Provider) Head(ctx context.Context) (model.ELBlockSnapshot, error) {
	number, hash, err := p.client.Head(ctx)
	if err != nil {
		return model.ELBlockSnapshot{}, err
	}
	ts, err := p.client.BlockTimestamp(ctx, hash)
	if err != nil {
		return model.ELBlockSnapshot{}, err
	}
	return model.ELBlockSnapshot{
		BlockNumber:          number.Int64(),
		BlockHash:            hash,
		Timestamp:            ts,
		LastChangedBlockHash: hash,
	}, nil
}

// BlockHashAt returns the canonical hash at a height.
func (p *Provider) BlockHashAt(ctx context.Context, block int64) (string, error) {
	return p.client.BlockHashByNumber(ctx, big.NewInt(block))
}

// stakingRouter resolves the router address from the locator.
func (p *Provider) stakingRouter(ctx context.Context, at eth.BlockRef) (string, error) {
	p.routerMu.Lock()
	cached := p.router
	p.routerMu.Unlock()
	if cached != "" {
		return cached, nil
	}

	out, err := p.client.CallOne(ctx, at, eth.Call{
		To: p.locator, ABI: &eth.LocatorABI, Method: "stakingRouter",
	})
	if err != nil {
		return "", err
	}
	addr := addressOf(out[0])
	if addr == "" {
		return "", fmt.Errorf("unexpected stakingRouter type %T", out[0])
	}

	p.routerMu.Lock()
	p.router = addr
	p.routerMu.Unlock()
	p.log.Debug("staking router resolved", "locator", p.locator, "router", addr)
	return addr, nil
}

// Modules returns the staking modules at a block with their type and nonce.
//
// Nonce comes from the module contract, not the router: the router has no
// per-module nonce getter, and the module's own getNonce is what the canonical
// service reads too.
func (p *Provider) Modules(ctx context.Context, at source.Ref) ([]model.StakingModule, error) {
	ref := blockRef(at)

	router, err := p.stakingRouter(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("staking router: %w", err)
	}

	out, err := p.client.CallOne(ctx, ref, eth.Call{
		To: router, ABI: &eth.StakingRouterABI, Method: "getStakingModules",
	})
	if err != nil {
		return nil, fmt.Errorf("getStakingModules: %w", err)
	}

	raw, err := decodeStakingModules(out[0])
	if err != nil {
		return nil, err
	}

	// One getType and one getNonce per module, batched into as few requests as
	// the client's batch size allows.
	calls := make([]eth.Call, 0, len(raw)*2)
	for _, m := range raw {
		calls = append(calls, eth.Call{To: m.address, ABI: &eth.StakingModuleABI, Method: "getType"})
	}
	for _, m := range raw {
		calls = append(calls, eth.Call{To: m.address, ABI: &eth.RegistryABI, Method: "getNonce"})
	}
	results, err := p.client.CallBatch(ctx, ref, calls)
	if err != nil {
		return nil, fmt.Errorf("module type and nonce: %w", err)
	}

	started := time.Now()
	modules := make([]model.StakingModule, 0, len(raw))
	for i, m := range raw {
		moduleType, err := decodeModuleType(results[i][0])
		if err != nil {
			return nil, err
		}
		nonce, ok := results[len(raw)+i][0].(*big.Int)
		if !ok {
			return nil, fmt.Errorf("unexpected nonce type %T", results[len(raw)+i][0])
		}

		module := model.StakingModule{
			Nonce:                     int32(nonce.Int64()),
			Type:                      moduleType,
			ID:                        m.id,
			ModuleFee:                 m.moduleFee,
			TreasuryFee:               m.treasuryFee,
			TargetShare:               m.targetShare,
			Status:                    m.status,
			Name:                      m.name,
			LastDepositAt:             m.lastDepositAt,
			LastDepositBlock:          m.lastDepositBlock,
			ExitedValidatorsCount:     m.exitedValidatorsCount,
			Active:                    m.status == 0,
			WithdrawalCredentialsType: m.withdrawalCredentialsType,
			// The writer decides this per version, by comparing content. Carrying
			// the read block here would reproduce the canonical service's problem:
			// re-reading a module would look like a change.
			LastChangedBlockHash: "",
		}
		module.SetRawAddress(m.address)
		modules = append(modules, module)

		p.log.Debug("module read", "id", module.ID, "type", module.Type,
			"nonce", module.Nonce, "address", m.address, "active", module.Active)
	}
	p.log.Debug("modules read", "count", len(modules),
		"decode_took", time.Since(started).Round(time.Millisecond).String())
	return modules, nil
}

// reader resolves the implementation for a module, or reports that we have none.
func (p *Provider) reader(m model.StakingModule) (ModuleReader, error) {
	r, ok := p.readers[m.Type]
	if !ok {
		return nil, source.ErrUnsupportedModule{ModuleID: m.ID, ModuleType: m.Type, Address: m.RawAddress()}
	}
	return r, nil
}

// Operators reads the operators of one module.
func (p *Provider) Operators(ctx context.Context, at source.Ref, m model.StakingModule) ([]model.Operator, error) {
	r, err := p.reader(m)
	if err != nil {
		return nil, err
	}
	return r.Operators(ctx, p.client, blockRef(at), m.RawAddress())
}

// Keys streams the keys of one module. Operators are read first because both
// readers need their vetted and deposited counts to set the per-key flags.
func (p *Provider) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	r, err := p.reader(m)
	if err != nil {
		return err
	}
	ref := blockRef(at)

	started := time.Now()
	operators, err := r.Operators(ctx, p.client, ref, m.RawAddress())
	if err != nil {
		return fmt.Errorf("operators of module %d: %w", m.ID, err)
	}

	var keys int
	for _, op := range operators {
		keys += int(op.TotalSigningKeys)
	}
	p.log.Debug("reading keys", "module", m.ID, "type", m.Type,
		"operators", len(operators), "keys_expected", keys,
		"operators_took", time.Since(started).Round(time.Millisecond).String())

	return r.Keys(ctx, p.client, ref, m.RawAddress(), operators, fn)
}

// OperatorsChanged reports whether operator metadata moved in a block range.
//
// Nonce covers keys and deposits but not a rename or a reward-address change, so
// without this a version would keep serving a stale operator name until the next
// key change. The topics come from the reader, because each module family emits
// its own events.
func (p *Provider) OperatorsChanged(ctx context.Context, m model.StakingModule, fromBlock, toBlock int64) (bool, error) {
	if fromBlock > toBlock {
		return false, nil
	}
	r, err := p.reader(m)
	if err != nil {
		return false, err
	}
	topics := r.OperatorEventTopics()
	if len(topics) == 0 {
		return false, nil
	}

	changed, err := p.client.HasLogs(ctx, m.RawAddress(), topics, fromBlock, toBlock)
	if err == nil {
		p.log.Debug("operator event scan", "module", m.ID,
			"from", fromBlock, "to", toBlock, "changed", changed, "topics", topics)
	}
	return changed, err
}

// blockRef pins reads to the branch the cycle started on.
func blockRef(at source.Ref) eth.BlockRef {
	if at.BlockHash != "" {
		return eth.BlockRef{Hash: at.BlockHash}
	}
	if at.BlockNumber > 0 {
		return eth.BlockRef{Number: big.NewInt(at.BlockNumber)}
	}
	return eth.BlockRef{Tag: "latest"}
}

// KeysBatchSize matches the canonical KEYS_BATCH_SIZE.
const KeysBatchSize = 1100

// operatorBatchSize is how many per-operator call pairs go into one request
// group; it mirrors the canonical REGISTRY_OPERATORS_BATCH_SIZE.
const operatorBatchSize = 200

// CallsPerRequest and KeysPerRequest bound one group of key reads.
//
// The pair exists because two different things can make a group too big for a
// node: too many calls (each one costs base gas and a bounds check) or too many
// keys returned (each key is five storage words plus its share of the response).
// 200 calls of one key each and two calls of 1100 keys each are both a fraction of
// geth's 50M eth_call gas cap; either limit alone would allow a group that is not.
//
// Neither is a correctness bound — a node that refuses a group has it bisected —
// so these are the starting point, and the number a provider actually tolerates is
// what E9 measures.
const (
	CallsPerRequest = 200
	KeysPerRequest  = 2 * KeysBatchSize
)

var (
	_ source.Provider        = (*Provider)(nil)
	_ source.OperatorWatcher = (*Provider)(nil)
	_ source.ChainIdentifier = (*Provider)(nil)
)
