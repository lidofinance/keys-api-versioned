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
	"sync/atomic"
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

	// router is the resolved staking router address, re-checked every cycle rather
	// than pinned for the life of the process — see Modules for how that costs no
	// extra round trip.
	routerMu sync.Mutex
	router   string

	// onRouterChange is called when the locator starts pointing somewhere else, so
	// the event is alertable rather than only present in a log line.
	onRouterChange func(previous, current string)

	// version is the staking router's own contract version, read with the module set
	// and zero before the first read. It decides how the module tuple is interpreted,
	// which is the thing a contract upgrade changes under a running service.
	version atomic.Int64
	// onVersion is called with every contract version observed, so an upgrade is a
	// metric rather than a line in a log.
	onVersion func(previous, current int64)

	// sourceChange holds the reason the last read came from somewhere else, until the
	// sync loop consumes it. Both changes it records — a replaced router, a new
	// contract version — can leave every nonce untouched, so this is the only thing
	// that makes the loop re-read.
	changeMu     sync.Mutex
	sourceChange string
}

// SourceChanged reports and clears the reason the registry's provenance changed.
func (p *Provider) SourceChanged() (string, bool) {
	p.changeMu.Lock()
	defer p.changeMu.Unlock()
	reason := p.sourceChange
	p.sourceChange = ""
	return reason, reason != ""
}

// noteSourceChange records a reason for the next cycle to act on. Several in one
// cycle are joined rather than overwritten: a deploy that replaces the router usually
// changes the contract version too, and the log line should say both happened.
func (p *Provider) noteSourceChange(reason string) {
	p.changeMu.Lock()
	defer p.changeMu.Unlock()
	if p.sourceChange == "" {
		p.sourceChange = reason
		return
	}
	p.sourceChange += "; " + reason
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
	// OnRouterChange fires when the locator starts answering with a different staking
	// router — a protocol deploy, seen from the outside. It exists so the event can be
	// a counter and an alert rather than a log line someone has to be looking at.
	OnRouterChange func(previous, current string)
	// OnContractVersion fires with the staking router's contract version on every
	// cycle that read it, previous being 0 on the first. The same reasoning as
	// OnRouterChange: an upgrade changes how the registry has to be read, and a
	// service that notices it only in a log has not noticed it.
	OnContractVersion func(previous, current int64)
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

// FinalizedBlock reports the newest block the node considers final, which is what makes
// `?snapshot=finalized` answerable. A header read; never a point state is read at.
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

// stakingRouter resolves the router address from the locator. Used on the cold path
// only: once an address is known, the re-check travels with the module read.
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

// cachedRouter returns the address resolved so far, empty before the first read.
func (p *Provider) cachedRouter() string {
	p.routerMu.Lock()
	defer p.routerMu.Unlock()
	return p.router
}

// adoptRouter stores a resolved address and reports whether it replaced a different
// one, which is the case a running service has to notice by itself.
func (p *Provider) adoptRouter(addr string) (previous string, changed bool) {
	p.routerMu.Lock()
	previous, p.router = p.router, addr
	p.routerMu.Unlock()
	return previous, previous != "" && previous != addr
}

// Modules returns the staking modules at a block with their type and nonce.
//
// Nonce comes from the module contract, not the router: the router has no
// per-module nonce getter, and the module's own getNonce is what the canonical
// service reads too.
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

// readModules reads the module set, and re-checks the locator in the same request.
//
// The router address is not pinned for the life of the process. The locator is a
// proxy whose component addresses live in its implementation, so a deploy that
// replaces the router changes what the locator answers — and a service holding the
// old address keeps reading a router nobody uses any more, reporting a registry that
// is quietly frozen. That is the failure this avoids: it is silent, it looks healthy
// from every metric, and today the only cure was restarting the pod.
//
// It costs nothing per cycle. The module read is a request that goes out anyway, and
// a JSON-RPC batch carries up to PROVIDER_JSON_RPC_MAX_BATCH_SIZE calls — so the
// locator read travels with it as a second call in the same round trip. The canonical
// service pays a separate request for the same information every iteration.
//
// getStakingModules is asked of the address known at the start, because the answer to
// "which router" arrives in the same response. When they disagree the module set just
// read came from the old router, so it is discarded and re-read at the new one: one
// extra request, in the cycle where the router actually changed, once.
func (p *Provider) readModules(ctx context.Context, ref eth.BlockRef) (string, any, error) {
	router := p.cachedRouter()
	if router == "" {
		// Cold path: nothing to verify against yet, so the resolve is ordered before
		// the reads that depend on it.
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

	// The locator moved. Warn rather than debug: a router replacement is a protocol
	// deploy, and this line is what connects "the registry changed completely" to it.
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

// readRouter reads the contract version and the module set from one router, in one
// request.
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

// adoptVersion stores the router's contract version and reports a change.
//
// Read every cycle, with the module set and in the same request, for the same reason
// the locator is: an upgrade happens under a service that is already running. The
// version is what says how to read the module tuple — before v4 there is no
// withdrawal credentials type and 1 is implied — so a service that assumes a shape
// instead of asking is wrong in a way that looks like data rather than like an error.
//
// A change is not a failure. What it must not be is invisible.
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

// srVersionWithWCType is the staking router version from which the module tuple
// carries withdrawalCredentialsType. Below it the field is absent or unset and type 1
// is implied — the same version, and the same decision, as the canonical service's
// SR_VERSION.
const srVersionWithWCType = 4

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

// OperatorsChangedAny reports whether operator metadata moved in a block range for
// any of the modules, in one request per module family.
//
// The grouping is by reader, not by module: `address` in a log filter takes a list,
// but the topics have to be the ones that family actually emits — curated modules
// have three, the CSM-shaped ones have one, and a union across both would ask each
// contract about events it cannot emit. Mainnet's four modules are two families, so
// a quiet cycle costs two eth_getLogs instead of four.
//
// The scan short-circuits on the first family that reports a change, because the
// caller wants one boolean and a rebuild is triggered either way.
func (p *Provider) OperatorsChangedAny(ctx context.Context, modules []model.StakingModule, fromBlock, toBlock int64) (bool, error) {
	if fromBlock > toBlock || len(modules) == 0 {
		return false, nil
	}

	// Grouped in the order the modules arrive, so the request a cycle makes is
	// reproducible from the module set rather than from map iteration.
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
			// A module type with no reader is one this provider cannot read at all; its
			// keys are not in any version either, so it has no operator metadata here to
			// go stale. Skipped rather than failing the scan, which is what the
			// per-module path does by never being called for it.
			p.log.Debug("operator event scan: no reader for module", "module", m.ID, "type", m.Type)
			continue
		}
		topics := r.OperatorEventTopics()
		if len(topics) == 0 {
			continue
		}
		// Keyed by the topic set rather than by the module type: two types served by one
		// reader — which is what curated and SimpleDVT are — belong in one request.
		key := strings.Join(topics, ",")
		g, ok := byType[key]
		if !ok {
			g = &group{topics: topics}
			byType[key] = g
			groups = append(groups, g)
		}
		g.addresses = append(g.addresses, m.RawAddress())
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
	_ source.Provider               = (*Provider)(nil)
	_ source.OperatorWatcher        = (*Provider)(nil)
	_ source.GroupedOperatorWatcher = (*Provider)(nil)
	_ source.SourceChangeReporter   = (*Provider)(nil)
	_ source.ChainIdentifier        = (*Provider)(nil)
)
