// Package static is a source.Provider backed by a JSON file, re-read on every Head: bump a
// module's nonce in the file and the loop rebuilds that module only.
package static

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/source"
)

// File is the on-disk shape. Block is optional.
type File struct {
	ChainID int64    `json:"chainId"`
	Block   int64    `json:"block"`
	Modules []Module `json:"modules"`
}

type Module struct {
	ID                        int32  `json:"id"`
	Address                   string `json:"address"`
	Type                      string `json:"type"`
	Name                      string `json:"name"`
	Nonce                     int32  `json:"nonce"`
	Status                    int32  `json:"status"`
	ModuleFee                 int32  `json:"moduleFee"`
	TreasuryFee               int32  `json:"treasuryFee"`
	TargetShare               int32  `json:"targetShare"`
	ExitedValidatorsCount     int32  `json:"exitedValidatorsCount"`
	WithdrawalCredentialsType int32  `json:"withdrawalCredentialsType"`

	Operators []Operator `json:"operators"`
}

type Operator struct {
	Index         int32  `json:"index"`
	Name          string `json:"name"`
	RewardAddress string `json:"rewardAddress"`
	Active        bool   `json:"active"`
	StakingLimit  int32  `json:"stakingLimit"`
	UsedKeys      int32  `json:"usedKeys"`
	// Leave TotalWithdrawnKeys out for curated-onchain-v1, where the canonical service serves null.
	TotalWithdrawnKeys *int32 `json:"totalWithdrawnKeys,omitempty"`
	Keys               []Key  `json:"keys"`
}

// An empty Signature is derived from the pubkey so the wire shape stays valid.
type Key struct {
	Key       string `json:"key"`
	Signature string `json:"signature"`
}

type Provider struct {
	path string

	mu    sync.Mutex
	block int64
	file  *File
	hash  string
}

func New(path string) *Provider { return &Provider{path: path} }

func (p *Provider) Name() string { return "static-file" }

func (p *Provider) ChainID(context.Context) (int64, error) {
	f, _, err := p.load()
	if err != nil {
		return 0, err
	}
	return f.ChainID, nil
}

// load derives the block from the file: mtime as height (monotone across restarts), content
// hash as identity (changes exactly when the content does).
func (p *Provider) load() (*File, string, error) {
	raw, err := os.ReadFile(p.path)
	if err != nil {
		return nil, "", fmt.Errorf("read %s: %w", p.path, err)
	}
	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, "", fmt.Errorf("parse %s: %w", p.path, err)
	}

	info, err := os.Stat(p.path)
	if err != nil {
		return nil, "", fmt.Errorf("stat %s: %w", p.path, err)
	}

	sum := sha256.Sum256(raw)
	contentHash := "0x" + hex.EncodeToString(sum[:])

	p.mu.Lock()
	defer p.mu.Unlock()
	p.hash = contentHash
	p.block = info.ModTime().Unix()
	p.file = &f
	return &f, contentHash, nil
}

func (p *Provider) Head(context.Context) (model.ELBlockSnapshot, error) {
	f, hash, err := p.load()
	if err != nil {
		return model.ELBlockSnapshot{}, err
	}

	p.mu.Lock()
	block := p.block
	p.mu.Unlock()
	if f.Block > 0 {
		block = f.Block
	}

	return model.ELBlockSnapshot{
		BlockNumber:          block,
		BlockHash:            hash,
		Timestamp:            0, // a file has no block time; the writer stamps its own
		LastChangedBlockHash: hash,
	}, nil
}

// BlockHashAt: a file has one branch, so any other height is "" ("not on this chain").
func (p *Provider) BlockHashAt(_ context.Context, block int64) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if block == p.block {
		return p.hash, nil
	}
	return "", nil
}

func (p *Provider) Modules(context.Context, source.Ref) ([]model.StakingModule, error) {
	f, _, err := p.load()
	if err != nil {
		return nil, err
	}

	out := make([]model.StakingModule, 0, len(f.Modules))
	for _, m := range f.Modules {
		wcType := m.WithdrawalCredentialsType
		if wcType == 0 {
			wcType = 1
		}
		module := model.StakingModule{
			Nonce:                     m.Nonce,
			Type:                      m.Type,
			ID:                        m.ID,
			ModuleFee:                 m.ModuleFee,
			TreasuryFee:               m.TreasuryFee,
			TargetShare:               m.TargetShare,
			Status:                    m.Status,
			Name:                      m.Name,
			ExitedValidatorsCount:     m.ExitedValidatorsCount,
			Active:                    m.Status == 0,
			WithdrawalCredentialsType: wcType,
		}
		module.SetRawAddress(strings.ToLower(m.Address))
		out = append(out, module)
	}
	return out, nil
}

func (p *Provider) Operators(_ context.Context, _ source.Ref, m model.StakingModule) ([]model.Operator, error) {
	src, err := p.moduleByAddress(m.RawAddress())
	if err != nil {
		return nil, err
	}

	out := make([]model.Operator, 0, len(src.Operators))
	for _, op := range src.Operators {
		out = append(out, model.Operator{
			Name:                       op.Name,
			RewardAddress:              strings.ToLower(op.RewardAddress),
			StakingLimit:               op.StakingLimit,
			TotalSigningKeys:           int32(len(op.Keys)),
			UsedSigningKeys:            op.UsedKeys,
			Index:                      op.Index,
			Active:                     op.Active,
			ModuleAddress:              src.Address,
			DepositableValidatorsCount: max(op.StakingLimit-op.UsedKeys, 0),
			TotalWithdrawnKeys:         op.TotalWithdrawnKeys,
		})
	}
	return out, nil
}

func (p *Provider) Keys(_ context.Context, _ source.Ref, m model.StakingModule, fn func(model.Key) error) error {
	src, err := p.moduleByAddress(m.RawAddress())
	if err != nil {
		return err
	}

	for _, op := range src.Operators {
		for i, k := range op.Keys {
			index := int32(i)

			pubkey, err := model.DecodeHex(k.Key)
			if err != nil {
				return fmt.Errorf("operator %d key %d: %w", op.Index, index, err)
			}
			signature, err := model.DecodeHex(k.Signature)
			if err != nil {
				return fmt.Errorf("operator %d signature %d: %w", op.Index, index, err)
			}
			if len(signature) == 0 {
				sum := sha256.Sum256(pubkey)
				signature = append(append(append(signature, sum[:]...), sum[:]...), sum[:]...)
			}

			if err := fn(model.Key{
				PubkeyBin:     pubkey,
				SignatureBin:  signature,
				OperatorIndex: op.Index,
				Used:          index < op.UsedKeys,
				ModuleAddress: src.Address,
				Index:         index,
				Vetted:        index < op.StakingLimit,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Provider) moduleByAddress(address string) (*Module, error) {
	p.mu.Lock()
	f := p.file
	p.mu.Unlock()
	if f == nil {
		var err error
		f, _, err = p.load()
		if err != nil {
			return nil, err
		}
	}

	for i := range f.Modules {
		if strings.EqualFold(f.Modules[i].Address, address) {
			m := f.Modules[i]
			m.Address = strings.ToLower(m.Address)
			return &m, nil
		}
	}
	return nil, fmt.Errorf("module %s is not in %s", address, p.path)
}

var (
	_ source.Provider        = (*Provider)(nil)
	_ source.ChainIdentifier = (*Provider)(nil)
)
