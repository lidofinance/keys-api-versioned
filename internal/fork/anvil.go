//go:build fork

// Package fork runs the whole service against an anvil fork (-tags fork; skips without
// FORK_UPSTREAM_RPC and TEST_DATABASE_URL). anvil runs --no-mining; tests mine and call Cycle.
package fork

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type Anvil struct {
	URL string
	// ForkBlock and below came from upstream and cannot be rewritten.
	ForkBlock int64

	tb    testing.TB
	cmd   *exec.Cmd
	out   *lockedBuffer
	rpcID atomic.Int64
	http  *http.Client
}

type AnvilOpts struct {
	Upstream string
	// Block overrides the pinned fork block; a lower one builds a lagging node.
	Block int64
	Args  []string
}

func StartAnvil(tb testing.TB, o AnvilOpts) *Anvil {
	tb.Helper()

	if o.Upstream == "" {
		o.Upstream = Upstream(tb)
	}
	if o.Block == 0 {
		o.Block = ForkBlock()
	}

	port := freePort(tb)
	args := []string{
		"--fork-url", o.Upstream,
		"--fork-block-number", strconv.FormatInt(o.Block, 10),
		"--auto-impersonate",
		"--no-mining",
		"--port", strconv.Itoa(port),
	}
	args = append(args, o.Args...)

	a := &Anvil{
		URL:       fmt.Sprintf("http://127.0.0.1:%d", port),
		ForkBlock: o.Block,
		tb:        tb,
		out:       &lockedBuffer{},
		http:      &http.Client{Timeout: 120 * time.Second},
	}
	a.cmd = exec.Command("anvil", args...)
	a.cmd.Stdout = a.out
	a.cmd.Stderr = a.out
	if err := a.cmd.Start(); err != nil {
		tb.Fatalf("start anvil: %v (is foundry installed?)", err)
	}

	tb.Cleanup(func() {
		_ = a.cmd.Process.Kill()
		_ = a.cmd.Wait()
		// Only the tail on failure: anvil prints private keys and a line per RPC call.
		if tb.Failed() {
			tb.Logf("last anvil lines:\n%s", a.out.Tail(40))
		}
	})

	a.waitReady(o.Block)
	return a
}

// A fork spends its first seconds pulling the block from upstream.
func (a *Anvil) waitReady(block int64) {
	a.tb.Helper()

	deadline := time.Now().Add(90 * time.Second)
	for {
		if raw, err := a.TryRPC("eth_blockNumber"); err == nil {
			var hex string
			if json.Unmarshal(raw, &hex) == nil {
				if n, err := strconv.ParseInt(strings.TrimPrefix(hex, "0x"), 16, 64); err == nil && n == block {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			a.tb.Fatalf("anvil did not reach block %d in 90s:\n%s", block, a.out.String())
		}
		if a.cmd.ProcessState != nil {
			a.tb.Fatalf("anvil exited early:\n%s", a.out.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (a *Anvil) RPC(method string, params ...any) json.RawMessage {
	a.tb.Helper()
	raw, err := a.TryRPC(method, params...)
	if err != nil {
		a.tb.Fatalf("%s: %v", method, err)
	}
	return raw
}

func (a *Anvil) TryRPC(method string, params ...any) (json.RawMessage, error) {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": a.rpcID.Add(1), "method": method, "params": params,
	})
	if err != nil {
		return nil, err
	}

	resp, err := a.http.Post(a.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", out.Error.Code, out.Error.Message)
	}
	return out.Result, nil
}

func (a *Anvil) Mine(n int) {
	a.tb.Helper()
	a.RPC("anvil_mine", hexUint(uint64(n)))
}

func (a *Anvil) BlockNumber() int64 {
	a.tb.Helper()
	var hex string
	a.decode(a.RPC("eth_blockNumber"), &hex)
	return parseHex(a.tb, hex)
}

func (a *Anvil) BlockHash(block int64) string {
	a.tb.Helper()
	var out *struct {
		Hash string `json:"hash"`
	}
	a.decode(a.RPC("eth_getBlockByNumber", hexUint(uint64(block)), false), &out)
	if out == nil {
		return ""
	}
	return out.Hash
}

func (a *Anvil) Snapshot() string {
	a.tb.Helper()
	var id string
	a.decode(a.RPC("evm_snapshot"), &id)
	return id
}

// The snapshot is consumed.
func (a *Anvil) Revert(id string) {
	a.tb.Helper()
	var ok bool
	a.decode(a.RPC("evm_revert", id), &ok)
	if !ok {
		a.tb.Fatalf("evm_revert %s returned false", id)
	}
}

type ReorgTx struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Data        string `json:"data,omitempty"`
	BlockOffset uint64 `json:"-"`
}

// Reorg re-mines depth blocks: the head keeps its height on a branch with new hashes. evm_revert
// makes the head jump back, which the sync loop reads as a lagging endpoint, not a fork.
func (a *Anvil) Reorg(depth int, txs ...ReorgTx) {
	a.tb.Helper()

	// Reaching the fork boundary fails with a bare "Resource not found"; measured, one local block
	// above the fork is not enough, two are.
	if head := a.BlockNumber(); head-int64(depth) <= a.ForkBlock {
		a.tb.Fatalf("reorg of depth %d at head %d would reach the fork block %d: "+
			"anvil can only re-mine blocks of its own, so leave at least one local block below the reorg",
			depth, head, a.ForkBlock)
	}

	pairs := make([]any, 0, len(txs))
	for _, tx := range txs {
		pairs = append(pairs, []any{tx, tx.BlockOffset})
	}
	a.RPC("anvil_reorg", depth, pairs)
}

// Pass "0x" to make it an EOA again.
func (a *Anvil) SetCode(address, code string) {
	a.tb.Helper()
	a.RPC("anvil_setCode", address, code)
}

func (a *Anvil) SetStorageAt(address, slot, value string) {
	a.tb.Helper()
	a.RPC("anvil_setStorageAt", address, slot, value)
}

func (a *Anvil) Fund(address string) {
	a.tb.Helper()
	a.RPC("anvil_setBalance", address, hexBig(big.NewInt(1e18)))
}

func (a *Anvil) Send(from, to string, data []byte) {
	a.tb.Helper()
	if err := a.TrySend(from, to, data); err != nil {
		a.tb.Fatalf("send to %s from %s: %v", to, from, err)
	}
}

func (a *Anvil) TrySend(from, to string, data []byte) error {
	a.tb.Helper()

	a.Fund(from)
	raw, err := a.TryRPC("eth_sendTransaction", map[string]any{
		"from": from, "to": to, "data": "0x" + hexBytes(data),
	})
	if err != nil {
		return err
	}
	var hash string
	a.decode(raw, &hash)

	a.Mine(1)

	var receipt *struct {
		Status string `json:"status"`
	}
	a.decode(a.RPC("eth_getTransactionReceipt", hash), &receipt)
	if receipt == nil {
		return fmt.Errorf("no receipt for %s", hash)
	}
	if receipt.Status != "0x1" {
		return fmt.Errorf("transaction %s reverted", hash)
	}
	return nil
}

// Call is the harness's oracle read; the service reads through its own client.
func (a *Anvil) Call(to string, data []byte) []byte {
	a.tb.Helper()

	var hex string
	a.decode(a.RPC("eth_call", map[string]any{"to": to, "data": "0x" + hexBytes(data)}, "latest"), &hex)
	out, err := decodeHex(hex)
	if err != nil {
		a.tb.Fatalf("call %s: %v", to, err)
	}
	return out
}

func (a *Anvil) decode(raw json.RawMessage, into any) {
	a.tb.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		a.tb.Fatalf("decode %s: %v", string(raw), err)
	}
}

func Upstream(tb testing.TB) string {
	tb.Helper()
	url := os.Getenv("FORK_UPSTREAM_RPC")
	if url == "" {
		tb.Skip("set FORK_UPSTREAM_RPC to run the fork tests")
	}
	return url
}

func ForkBlock() int64 {
	if s := os.Getenv("FORK_BLOCK"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	}
	return defaultForkBlock
}

func freePort(tb testing.TB) int {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("find a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func hexUint(n uint64) string { return "0x" + strconv.FormatUint(n, 16) }

func hexBig(n *big.Int) string { return "0x" + n.Text(16) }

func parseHex(tb testing.TB, s string) int64 {
	tb.Helper()
	n, err := strconv.ParseInt(strings.TrimPrefix(s, "0x"), 16, 64)
	if err != nil {
		tb.Fatalf("parse %q: %v", s, err)
	}
	return n
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Tail(n int) string {
	lines := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
