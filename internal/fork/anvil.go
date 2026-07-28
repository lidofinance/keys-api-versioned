//go:build fork

// Package fork drives the whole service — the chain reader, the writer, the store
// and the HTTP surface — against an anvil fork of a real chain.
//
// It exists because the two test levels below it cannot create the states that
// matter. internal/e2e drives the writer directly and never involves a
// source.Provider, so no chain read is ever exercised; internal/ingest's scripted
// double has a nonce that is an int32 with no causal link to keys, one all-zero key
// per module, and no RPC that can fail. Between them, the sync loop has never run
// against Postgres, the operator-event scan has never run at all, and a branch with
// the same block height and different content cannot be expressed.
//
// A fork can express all three, plus the contract semantics that break the
// assumptions in source.go: removing a signing key moves the last key into the
// vacated slot, so key identity is not stable; a deposit flips `used` through a real
// counter; a rename moves no nonce at all.
//
// # Running
//
//	FORK_UPSTREAM_RPC=http://…-hoodi-geth…  \
//	TEST_DATABASE_URL=postgres://…/keys_api_fork \
//	go test -tags fork ./internal/fork/
//
// Both variables are required; without either the package skips. The fork block is
// pinned (see FORK_BLOCK in fork.go) so anvil serves the second run out of
// ~/.foundry/cache/rpc and the preconditions every test asserts stay true.
//
// # Determinism
//
// anvil runs with --no-mining: blocks are produced by the test, one call at a time,
// and the sync loop's ticker is never started — tests call Cycle explicitly. That is
// the same trick that makes internal/e2e deterministic (do the expensive part outside
// the window, then take one small step), moved to the chain side.
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

// Anvil is a running fork node.
type Anvil struct {
	URL string
	// ForkBlock is the height the fork was taken at. Blocks at or below it came from
	// upstream and cannot be rewritten; Reorg checks that.
	ForkBlock int64

	tb    testing.TB
	cmd   *exec.Cmd
	out   *lockedBuffer
	rpcID atomic.Int64
	http  *http.Client
}

// AnvilOpts configure a fork node. The zero value forks the pinned block off
// FORK_UPSTREAM_RPC, which is what every test wants.
type AnvilOpts struct {
	// Upstream overrides FORK_UPSTREAM_RPC.
	Upstream string
	// Block overrides the pinned fork block. Use a lower one to build a node that
	// lags another.
	Block int64
	// Args are appended to the anvil command line.
	Args []string
}

// StartAnvil boots a fork node and stops it when the test ends.
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
		// Every mutation is sent from an address that already holds the role on the
		// forked chain, so nothing here has to sign anything.
		"--auto-impersonate",
		// Blocks are produced by the test. Without this a scenario's steps land in
		// whatever blocks the clock hands out, and "the rename is in the block after
		// the version" stops being something a test can state.
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
		// Only on failure, and only the tail: anvil prints a banner, ten funded
		// accounts with their private keys, and a line per RPC call, so the whole log
		// buries the assertion that failed.
		if tb.Failed() {
			tb.Logf("last anvil lines:\n%s", a.out.Tail(40))
		}
	})

	a.waitReady(o.Block)
	return a
}

// waitReady blocks until the node answers at the forked height. A fork of a busy
// chain spends its first seconds pulling the block from upstream.
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

// RPC calls a JSON-RPC method and fails the test if it errors.
func (a *Anvil) RPC(method string, params ...any) json.RawMessage {
	a.tb.Helper()
	raw, err := a.TryRPC(method, params...)
	if err != nil {
		a.tb.Fatalf("%s: %v", method, err)
	}
	return raw
}

// TryRPC calls a JSON-RPC method and returns the error, for the cases where the
// error is the thing under test.
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

// Mine produces n blocks, including whatever is in the pool.
func (a *Anvil) Mine(n int) {
	a.tb.Helper()
	a.RPC("anvil_mine", hexUint(uint64(n)))
}

// BlockNumber is the current head.
func (a *Anvil) BlockNumber() int64 {
	a.tb.Helper()
	var hex string
	a.decode(a.RPC("eth_blockNumber"), &hex)
	return parseHex(a.tb, hex)
}

// BlockHash is the hash at a height, or "" if the node does not have it.
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

// Snapshot records the state to revert to.
func (a *Anvil) Snapshot() string {
	a.tb.Helper()
	var id string
	a.decode(a.RPC("evm_snapshot"), &id)
	return id
}

// Revert rolls state and blocks back to a snapshot. The snapshot is consumed.
func (a *Anvil) Revert(id string) {
	a.tb.Helper()
	var ok bool
	a.decode(a.RPC("evm_revert", id), &ok)
	if !ok {
		a.tb.Fatalf("evm_revert %s returned false", id)
	}
}

// ReorgTx is a transaction to include on the new branch, at an offset from the
// first re-mined block.
type ReorgTx struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Data        string `json:"data,omitempty"`
	BlockOffset uint64 `json:"-"`
}

// Reorg rewinds depth blocks and re-mines them, optionally with different
// transactions.
//
// This is the primitive the whole reorg family rests on, and it is faithful in the
// way evm_revert is not: the head keeps its height and moves forward onto a branch
// whose block hashes differ, which is what a node reports during a real reorg. A
// revert instead makes the head jump backwards, which the sync loop treats as a
// lagging endpoint rather than a fork.
func (a *Anvil) Reorg(depth int, txs ...ReorgTx) {
	a.tb.Helper()

	// A reorg may only rewrite blocks anvil produced itself. Reaching the fork
	// boundary fails with a bare "Resource not found", which reads like a broken
	// request rather than "mine another block first" — measured: one local block above
	// the fork is not enough, two are.
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

// SetCode replaces the code at an address. Pass "0x" to make it an EOA again.
func (a *Anvil) SetCode(address, code string) {
	a.tb.Helper()
	a.RPC("anvil_setCode", address, code)
}

// SetStorageAt writes one storage slot.
func (a *Anvil) SetStorageAt(address, slot, value string) {
	a.tb.Helper()
	a.RPC("anvil_setStorageAt", address, slot, value)
}

// Fund gives an address an ether balance, which every impersonated sender needs
// before it can pay for gas.
func (a *Anvil) Fund(address string) {
	a.tb.Helper()
	a.RPC("anvil_setBalance", address, hexBig(big.NewInt(1e18)))
}

// Send submits a transaction from an impersonated sender, mines it, and fails
// unless it succeeded.
func (a *Anvil) Send(from, to string, data []byte) {
	a.tb.Helper()
	if err := a.TrySend(from, to, data); err != nil {
		a.tb.Fatalf("send to %s from %s: %v", to, from, err)
	}
}

// TrySend is Send for the cases where the revert is the expected outcome.
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

// Call is a read at the head, for the harness's own assertions about chain state.
// The service under test reads through its own client; this one is the oracle.
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

// Upstream is the node to fork from, or a skip.
func Upstream(tb testing.TB) string {
	tb.Helper()
	url := os.Getenv("FORK_UPSTREAM_RPC")
	if url == "" {
		tb.Skip("set FORK_UPSTREAM_RPC to run the fork tests")
	}
	return url
}

// ForkBlock is the pinned block, overridable through FORK_BLOCK.
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

// lockedBuffer collects anvil's output; the process writes from its own goroutine
// while the test reads on failure.
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

// Tail is the last n lines, which is what a failure needs: the RPC calls around the
// moment it went wrong.
func (b *lockedBuffer) Tail(n int) string {
	lines := strings.Split(strings.TrimRight(b.String(), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
