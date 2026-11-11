// Package lidocore reads Lido core staking router modules from an execution node, dispatching
// each module to the ModuleReader registered for its getType.
package lidocore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/eth"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

type Provider struct {
	client  *eth.Client
	locator string
	readers map[string]ModuleReader
	log     *slog.Logger

	// router is re-checked every cycle, not pinned for the process life (see readModules).
	routerMu sync.Mutex
	router   string

	onRouterChange func(previous, current string)

	// version is the router's contract version (0 before the first read); it decides how the module
	// tuple is decoded.
	version   atomic.Int64
	onVersion func(previous, current int64)

	// sourceChange: a replaced router or a new contract version can leave every nonce untouched, so
	// this is the only thing that makes the loop re-read.
	changeMu     sync.Mutex
	sourceChange string
}

func (p *Provider) SourceChanged() (string, bool) {
	p.changeMu.Lock()
	defer p.changeMu.Unlock()
	reason := p.sourceChange
	p.sourceChange = ""
	return reason, reason != ""
}

// noteSourceChange joins reasons: a router replacement usually bumps the contract version too.
func (p *Provider) noteSourceChange(reason string) {
	p.changeMu.Lock()
	defer p.changeMu.Unlock()
	if p.sourceChange == "" {
		p.sourceChange = reason
		return
	}
	p.sourceChange += "; " + reason
}

// ModuleReader reads one family of module contracts (curated registry, CSM-shaped).
type ModuleReader interface {
	Types() []string
	Operators(ctx context.Context, c *eth.Client, at eth.BlockRef, m model.StakingModule) ([]model.Operator, error)
	Keys(ctx context.Context, c *eth.Client, at eth.BlockRef, moduleAddress string, operators []model.Operator, fn func(model.Key) error) error
	// OperatorEventTopics: ABI event names, or full signatures for events no loaded ABI declares.
	OperatorEventTopics() []string
}

// operatorEventAddresser: a reader whose operator metadata partly lives outside the module contract.
type operatorEventAddresser interface {
	OperatorEventAddresses(ctx context.Context, c *eth.Client, m model.StakingModule, toBlock int64) ([]string, error)
}

func (p *Provider) operatorEventAddresses(ctx context.Context, r ModuleReader, m model.StakingModule, toBlock int64) ([]string, error) {
	if a, ok := r.(operatorEventAddresser); ok {
		return a.OperatorEventAddresses(ctx, p.client, m, toBlock)
	}
	return []string{m.RawAddress()}, nil
}

type Options struct {
	KeysBatch uint64
	// Mainnet's 731 operators cost 731 requests at CallsPerRequest=1, a few dozen at 200.
	CallsPerRequest int
	KeysPerRequest  uint64
	Concurrency     int
	// Empty Readers means curated + community.
	Readers []ModuleReader
	Log     *slog.Logger
	// OnRouterChange fires when the locator answers with a different staking router (a deploy).
	OnRouterChange func(previous, current string)
	// OnContractVersion fires every cycle that read the router version; previous is 0 on the first.
	OnContractVersion func(previous, current int64)
}

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

	p := &Provider{
		client: client, locator: strings.ToLower(locator),
		readers: map[string]ModuleReader{}, log: o.Log,
		onRouterChange: o.OnRouterChange, onVersion: o.OnContractVersion,
	}
	for _, r := range readers {
		for _, t := range r.Types() {
			p.readers[t] = r
		}
	}
	return p
}

func (p *Provider) Name() string { return "execution-layer" }

func (p *Provider) SupportedTypes() []string {
	out := make([]string, 0, len(p.readers))
	for t := range p.readers {
		out = append(out, t)
	}
	return out
}

func (p *Provider) ChainID(ctx context.Context) (int64, error) {
	return p.client.ChainID(ctx)
}

// FinalizedBlock is a header read, never a point state is read at.
func (p *Provider) FinalizedBlock(ctx context.Context) (model.ELBlockSnapshot, error) {
	number, hash, timestamp, err := p.client.FinalizedBlock(ctx)
	if err != nil || number == 0 {
		return model.ELBlockSnapshot{}, err
	}
	return model.ELBlockSnapshot{
		BlockNumber: number,
		BlockHash:   strings.ToLower(hash),
		Timestamp:   timestamp,
	}, nil
}

func (p *Provider) Head(ctx context.Context) (model.ELBlockSnapshot, error) {
	number, hash, timestamp, err := p.client.Head(ctx)
	if err != nil {
		return model.ELBlockSnapshot{}, err
	}
	return model.ELBlockSnapshot{
		BlockNumber:          number.Int64(),
		BlockHash:            hash,
		Timestamp:            timestamp,
		LastChangedBlockHash: hash,
	}, nil
}

func (p *Provider) BlockHashAt(ctx context.Context, block int64) (string, error) {
	return p.client.BlockHashByNumber(ctx, big.NewInt(block))
}

// stakingRouter is the cold path only; afterwards the re-check travels with the module read.
func (p *Provider) stakingRouter(ctx context.Context, at eth.BlockRef) (string, error) {
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
	p.log.Debug("staking router resolved", "locator", p.locator, "router", addr)
	return addr, nil
}

func (p *Provider) cachedRouter() string {
	p.routerMu.Lock()
	defer p.routerMu.Unlock()
	return p.router
}

func (p *Provider) adoptRouter(addr string) (previous string, changed bool) {
	p.routerMu.Lock()
	previous, p.router = p.router, addr
	p.routerMu.Unlock()
	return previous, previous != "" && previous != addr
}

// Nonce comes from the module's own getNonce: the router has no per-module nonce getter.
func (p *Provider) Modules(ctx context.Context, at source.Ref) ([]model.StakingModule, error) {
	ref := blockRef(at)

	_, out, err := p.readModules(ctx, ref)
	if err != nil {
		return nil, err
	}

	raw, err := decodeStakingModules(out, p.version.Load())
	if err != nil {
		return nil, err
	}

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
			// Set per version by the writer comparing content; the read block would make a re-read look
			// like a change.
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

// readModules re-checks the locator in the same batch: a deploy that replaces the router otherwise
// leaves a healthy-looking, silently frozen registry. A changed router costs one re-read, once.
func (p *Provider) readModules(ctx context.Context, ref eth.BlockRef) (string, any, error) {
	router := p.cachedRouter()
	if router == "" {
		resolved, err := p.stakingRouter(ctx, ref)
		if err != nil {
			return "", nil, fmt.Errorf("staking router: %w", err)
		}
		p.adoptRouter(resolved)
		out, err := p.readRouter(ctx, ref, resolved)
		if err != nil {
			return "", nil, err
		}
		return resolved, out, nil
	}

	results, err := p.client.CallBatch(ctx, ref, []eth.Call{
		{To: p.locator, ABI: &eth.LocatorABI, Method: "stakingRouter"},
		{To: router, ABI: &eth.StakingRouterABI, Method: "getContractVersion"},
		{To: router, ABI: &eth.StakingRouterABI, Method: "getStakingModules"},
	})
	if err != nil {
		return "", nil, fmt.Errorf("locator, contract version and getStakingModules: %w", err)
	}

	current := addressOf(results[0][0])
	if current == "" {
		return "", nil, fmt.Errorf("unexpected stakingRouter type %T", results[0][0])
	}
	if current == router {
		if err := p.adoptVersion(results[1][0]); err != nil {
			return "", nil, err
		}
		return router, results[2][0], nil
	}

	previous, _ := p.adoptRouter(current)
	p.log.Warn("the locator now points at a different staking router, re-reading the module set",
		"locator", p.locator, "previous", previous, "current", current)
	p.noteSourceChange(fmt.Sprintf(
		"the locator now points at staking router %s instead of %s", current, previous))
	if p.onRouterChange != nil {
		p.onRouterChange(previous, current)
	}

	out, err := p.readRouter(ctx, ref, current)
	if err != nil {
		return "", nil, fmt.Errorf("at the new router %s: %w", current, err)
	}
	return current, out, nil
}

func (p *Provider) readRouter(ctx context.Context, ref eth.BlockRef, router string) (any, error) {
	results, err := p.client.CallBatch(ctx, ref, []eth.Call{
		{To: router, ABI: &eth.StakingRouterABI, Method: "getContractVersion"},
		{To: router, ABI: &eth.StakingRouterABI, Method: "getStakingModules"},
	})
	if err != nil {
		return nil, fmt.Errorf("contract version and getStakingModules: %w", err)
	}
	if err := p.adoptVersion(results[0][0]); err != nil {
		return nil, err
	}
	return results[1][0], nil
}

// adoptVersion reads the version every cycle: below v4 there is no withdrawal credentials type
// and 1 is implied, so assuming a shape is wrong in a way that looks like data.
func (p *Provider) adoptVersion(raw any) error {
	value, ok := raw.(*big.Int)
	if !ok {
		return fmt.Errorf("unexpected getContractVersion type %T", raw)
	}
	current := value.Int64()
	previous := p.version.Swap(current)
	if previous == current {
		return nil
	}
	if previous == 0 {
		p.log.Info("staking router contract version", "version", current,
			"withdrawal_credentials_type_from_contract", current >= srVersionWithWCType)
		return p.reportVersion(previous, current)
	}
	p.log.Warn("the staking router contract version changed under a running service",
		"previous", previous, "current", current,
		"withdrawal_credentials_type_from_contract", current >= srVersionWithWCType)
	p.noteSourceChange(fmt.Sprintf(
		"the staking router contract version went from %d to %d", previous, current))
	return p.reportVersion(previous, current)
}

func (p *Provider) reportVersion(previous, current int64) error {
	if p.onVersion != nil {
		p.onVersion(previous, current)
	}
	return nil
}

// srVersionWithWCType matches the canonical service's SR_VERSION.
const srVersionWithWCType = 4

func (p *Provider) reader(m model.StakingModule) (ModuleReader, error) {
	r, ok := p.readers[m.Type]
	if !ok {
		return nil, source.ErrUnsupportedModule{ModuleID: m.ID, ModuleType: m.Type, Address: m.RawAddress()}
	}
	return r, nil
}

func (p *Provider) Operators(ctx context.Context, at source.Ref, m model.StakingModule) ([]model.Operator, error) {
	r, err := p.reader(m)
	if err != nil {
		return nil, err
	}
	return r.Operators(ctx, p.client, blockRef(at), m)
}

// Operators are read first: readers need vetted and deposited counts to set per-key flags.
func (p *Provider) Keys(ctx context.Context, at source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	r, err := p.reader(m)
	if err != nil {
		return err
	}
	ref := blockRef(at)

	started := time.Now()
	operators, err := r.Operators(ctx, p.client, ref, m)
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

// OperatorsChanged covers what the nonce does not: renames and reward-address changes.
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

	addresses, err := p.operatorEventAddresses(ctx, r, m, toBlock)
	if err != nil {
		return false, err
	}
	changed, err := p.client.HasAnyLogs(ctx, addresses, topics, fromBlock, toBlock)
	if err == nil {
		p.log.Debug("operator event scan", "module", m.ID,
			"from", fromBlock, "to", toBlock, "changed", changed, "topics", topics)
	}
	return changed, err
}

// OperatorsChangedAny issues one eth_getLogs per reader family, not per module: topics differ by
// family, so a union would ask contracts about events they cannot emit.
func (p *Provider) OperatorsChangedAny(ctx context.Context, modules []model.StakingModule, fromBlock, toBlock int64) (bool, error) {
	if fromBlock > toBlock || len(modules) == 0 {
		return false, nil
	}

	// Grouped in module order, so the request is reproducible rather than map-ordered.
	type group struct {
		topics    []string
		addresses []string
	}
	var (
		groups []*group
		byType = map[string]*group{}
	)
	for _, m := range modules {
		r, err := p.reader(m)
		if err != nil {
			// No reader means no keys in any version either, so nothing here to go stale.
			p.log.Debug("operator event scan: no reader for module", "module", m.ID, "type", m.Type)
			continue
		}
		topics := r.OperatorEventTopics()
		if len(topics) == 0 {
			continue
		}
		// Keyed by topic set: curated and SimpleDVT share one reader and belong in one request.
		key := strings.Join(topics, ",")
		g, ok := byType[key]
		if !ok {
			g = &group{topics: topics}
			byType[key] = g
			groups = append(groups, g)
		}
		addresses, err := p.operatorEventAddresses(ctx, r, m, toBlock)
		if err != nil {
			return false, err
		}
		g.addresses = append(g.addresses, addresses...)
	}

	for _, g := range groups {
		changed, err := p.client.HasAnyLogs(ctx, g.addresses, g.topics, fromBlock, toBlock)
		if err != nil {
			return false, err
		}
		p.log.Debug("operator event scan", "addresses", g.addresses,
			"from", fromBlock, "to", toBlock, "changed", changed, "topics", g.topics)
		if changed {
			return true, nil
		}
	}
	return false, nil
}

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

// operatorBatchSize mirrors the canonical REGISTRY_OPERATORS_BATCH_SIZE.
const operatorBatchSize = 200

// Two limits because calls and returned keys both cost gas: either alone allows a group over
// geth's 50M eth_call cap. Not a correctness bound — a refused group is bisected.
const (
	CallsPerRequest = 200
	KeysPerRequest  = 2 * KeysBatchSize
)

var (
	_ source.Provider               = (*Provider)(nil)
	_ source.OperatorWatcher        = (*Provider)(nil)
	_ source.GroupedOperatorWatcher = (*Provider)(nil)
	_ source.SourceChangeReporter   = (*Provider)(nil)
	_ source.ChainIdentifier        = (*Provider)(nil)
)
