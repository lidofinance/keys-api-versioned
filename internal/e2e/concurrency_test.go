package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lidofinance/keys-api-versioned/internal/ingest"
	"github.com/lidofinance/keys-api-versioned/internal/model"
	"github.com/lidofinance/keys-api-versioned/internal/testpg"
	"github.com/lidofinance/keys-api-versioned/internal/testreg"
)

// Each dump must be one version end to end: any published salt is tolerated, but modules must agree.
func TestParallelDumpsUnderAWriterAreEachInternallyConsistent(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	const (
		readers   = 6
		publishes = 4
	)

	spec := testreg.Spec{Modules: 3, OperatorsPerModule: 4, KeysPerOperator: 200}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	srv := testpg.NewServer(t, db.Reader(t, "latest", readers+2))

	var wg sync.WaitGroup
	failures := make(chan error, readers+publishes)

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			resp, err := srv.Client().Get(srv.URL + "/v1/keys")
			if err != nil {
				failures <- fmt.Errorf("dump %d: %w", n, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				failures <- fmt.Errorf("dump %d: status %d", n, resp.StatusCode)
				return
			}
			if err := verifyOneVersion(spec, publishes, resp.Body); err != nil {
				failures <- fmt.Errorf("dump %d: %w", n, err)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for salt := 1; salt <= publishes; salt++ {
			next := spec.WithSalt(uint64(salt))
			next.Publish(ctx, t, db.Writer, testreg.At(db.Block(int64(salt))))
		}
	}()

	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}

	if failures := srv.Logs.Messages(slogError); len(failures) > 0 {
		t.Errorf("the service logged failures while serving dumps under a writer: %v", failures)
	}
}

func verifyOneVersion(spec testreg.Spec, generations int, body io.Reader) error {
	dump, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read the dump: %w", err)
	}

	var best error
	for salt := 0; salt <= generations; salt++ {
		verifier := testreg.NewVerifier(spec.WithSalt(uint64(salt)))
		if _, err := verifier.V1Keys(newReader(dump)); err != nil {
			if best == nil {
				best = err
			}
			continue
		}
		if err := verifier.Done(); err != nil {
			if best == nil {
				best = err
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("no single version explains this dump, so it mixes versions: %w", best)
}

// ErrVersionExists lets two ingest processes share one database without double-publishing.
func TestASecondWriterAtTheSameBlockGetsErrVersionExists(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 2}
	at := testreg.At(db.Block(0))

	first, err := db.Writer.Begin(ctx, at)
	if err != nil {
		t.Fatalf("first writer: %v", err)
	}
	t.Cleanup(func() { first.Abort(ctx) })

	second, err := db.Writer.Begin(ctx, at)
	if !errors.Is(err, ingest.ErrVersionExists) {
		if err == nil {
			second.Abort(ctx)
		}
		t.Fatalf("second writer at the same block: err = %v, want ErrVersionExists", err)
	}

	// The first still finishes: the guard rejects the duplicate rather than poisoning the version.
	module := spec.Module(1)
	keys, err := first.Keys(ctx, module)
	if err != nil {
		t.Fatalf("open keys: %v", err)
	}
	for i := 0; i < spec.KeysPerOperator; i++ {
		if err := keys.Add(ctx, spec.Key(1, 0, i)); err != nil {
			t.Fatalf("add key: %v", err)
		}
	}
	if _, err := keys.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := first.Module(ctx, module, true); err != nil {
		t.Fatalf("module row: %v", err)
	}
	if err := first.Publish(ctx, "latest", 0); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// ErrVersionExists only rejects the same block, so each version must carry what its own build staged.
func TestTwoWritersBuildingTheSameModuleAtDifferentBlocks(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	base := testreg.At(db.Block(0))
	spec.Publish(ctx, t, db.Writer, base)

	early := spec.WithSalt(7)
	late := spec.WithSalt(9)
	earlyAt := testreg.At(db.Block(1))
	lateAt := testreg.At(db.Block(2))

	earlyBuild := early.Fill(ctx, t, db.Writer, earlyAt, 1)
	lateBuild := late.Fill(ctx, t, db.Writer, lateAt, 1)

	if err := earlyBuild.Publish(ctx, "latest", 0); err != nil {
		t.Fatalf("publish the earlier version: %v", err)
	}
	if err := lateBuild.Publish(ctx, "latest", 0); err != nil {
		t.Fatalf("publish the later version: %v", err)
	}

	for _, want := range []struct {
		block int64
		spec  testreg.Spec
	}{{earlyAt.BlockNumber, early}, {lateAt.BlockNumber, late}} {
		reader := db.Reader(t, fmt.Sprint(want.block), 2)
		srv := testpg.NewServer(t, reader)

		resp := srv.Get(t, "/v1/keys")
		verifier := testreg.NewVerifier(want.spec)
		_, err := verifier.V1Keys(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Errorf("version at block %d does not hold the content its build staged (salt %d): %v",
				want.block, want.spec.Salt, err)
			continue
		}
		if err := verifier.Done(); err != nil {
			t.Errorf("version at block %d is incomplete: %v", want.block, err)
		}
	}
}

func newReader(b []byte) io.Reader { return io.NewSectionReader(byteReaderAt(b), 0, int64(len(b))) }

type byteReaderAt []byte

func (b byteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Two writers over one module must each fail or be correct. Without the writer lock the second
// publishes a newer block while the first stalls, then the first rewrites rows that version shares.
func TestTwoConcurrentBuildsOverOneModuleFailRatherThanCorrupt(t *testing.T) {
	db := testpg.New(t)
	ctx := context.Background()

	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 5}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	type outcome struct {
		spec  testreg.Spec
		block int64
		err   error
	}
	results := make(chan outcome, 2)

	var (
		wg        sync.WaitGroup
		head      atomic.Int64
		firstRead = make(chan struct{})
	)
	nextHead := func() model.ELBlockSnapshot { return testreg.At(db.Block(head.Add(1))) }
	write := func(salt uint64, afterHead func()) {
		defer wg.Done()
		generation := spec.WithSalt(salt)
		block, err := writeAsASeparateWriter(ctx, db, generation, nextHead, afterHead)
		results <- outcome{generation, block, err}
	}
	wg.Add(2)
	go write(11, func() {
		close(firstRead)
		time.Sleep(200 * time.Millisecond)
	})
	go func() {
		<-firstRead
		write(12, func() {})
	}()
	wg.Wait()
	close(results)

	published := 0
	for r := range results {
		if r.err != nil {
			t.Logf("build at block %d failed, which is the acceptable outcome: %v", r.block, r.err)
			continue
		}
		published++

		reader := db.Reader(t, fmt.Sprint(r.block), 2)
		srv := testpg.NewServer(t, reader)
		resp := srv.Get(t, "/v1/keys")
		verifier := testreg.NewVerifier(r.spec)
		_, err := verifier.V1Keys(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Errorf("build at block %d reported success but published other content: %v", r.block, err)
			continue
		}
		if err := verifier.Done(); err != nil {
			t.Errorf("version at block %d is incomplete: %v", r.block, err)
		}
	}
	if published == 0 {
		t.Error("both concurrent builds failed; one of them should get through")
	}
}

// KAV-WRITER-01: the lock session dies mid-build, a waiting writer takes over and publishes, and the
// first writer reaches its key commit before its ping notices.
func TestAWriterThatLostItsLockCannotCommit(t *testing.T) {
	testpg.Exclusive(t)
	db := testpg.New(t)
	ctx := context.Background()
	spec := testreg.Spec{Modules: 1, OperatorsPerModule: 1, KeysPerOperator: 3}
	spec.Publish(ctx, t, db.Writer, testreg.At(db.Block(0)))

	lock, err := db.Writer.LockWriter(ctx, ingest.WriterLockOptions{CheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	build, err := db.Writer.Begin(ctx, testreg.At(db.Block(1)))
	if err != nil {
		t.Fatal(err)
	}
	stale := spec.WithSalt(1)
	keys, err := build.Keys(ctx, stale.Module(1))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < stale.KeysPerOperator; i++ {
		if err := keys.Add(ctx, stale.Key(1, 0, i)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.Admin().Exec(ctx, `select pg_terminate_backend(pid) from pg_locks
		where locktype = 'advisory' and objsubid = 2 and granted and classid = x'6b617069'::int::oid
		  and database = (select oid from pg_database where datname = current_database())`); err != nil {
		t.Fatal(err)
	}
	next, err := ingest.NewPGStore(ctx, db.DSN, ingest.PGOptions{Source: "testpg"})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	nextLock, err := next.LockWriter(lockCtx, ingest.WriterLockOptions{})
	if err != nil {
		t.Fatalf("the waiting writer did not get the lock: %v", err)
	}
	defer nextLock.Release()
	if _, err := next.TakeEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	current := spec.WithSalt(2)
	current.Publish(ctx, t, next, testreg.At(db.Block(2)))

	if _, err := keys.Commit(ctx); !errors.As(err, new(ingest.ErrFenced)) {
		t.Fatalf("the old writer's key commit returned %v, want ErrFenced", err)
	}
	build.Abort(ctx)

	srv := testpg.NewServer(t, db.Reader(t, "latest", 2))
	resp := srv.Get(t, "/v1/keys")
	defer resp.Body.Close()
	verifier := testreg.NewVerifier(current)
	if _, err := verifier.V1Keys(resp.Body); err != nil {
		t.Fatalf("the newest version does not serve the new writer's keys: %v", err)
	}
	if err := verifier.Done(); err != nil {
		t.Fatal(err)
	}
}

// writeAsASeparateWriter is a writer process in miniature: its own store, the writer lock, one build.
func writeAsASeparateWriter(ctx context.Context, db *testpg.DB, spec testreg.Spec, head func() model.ELBlockSnapshot, afterHead func()) (int64, error) {
	writer, err := ingest.NewPGStore(ctx, db.DSN, ingest.PGOptions{Source: "test"})
	if err != nil {
		return 0, err
	}
	defer writer.Close()
	lock, err := writer.LockWriter(ctx, ingest.WriterLockOptions{})
	if err != nil {
		return 0, err
	}
	defer lock.Release()
	if _, err := writer.TakeEpoch(ctx); err != nil {
		return 0, err
	}
	at := head()
	afterHead()
	return at.BlockNumber, buildAndPublish(ctx, writer, spec, at)
}

func buildAndPublish(ctx context.Context, writer *ingest.PGStore, spec testreg.Spec, at model.ELBlockSnapshot) (err error) {
	defer func() {
		// testreg helpers fail the test by panicking through here; this turns that into an error.
		if p := recover(); p != nil {
			err = fmt.Errorf("build panicked: %v", p)
		}
	}()

	build, err := writer.Begin(ctx, at)
	if err != nil {
		return err
	}
	module := spec.Module(1)
	keys, err := build.Keys(ctx, module)
	if err != nil {
		build.Abort(ctx)
		return err
	}
	for op := 0; op < spec.OperatorsPerModule; op++ {
		for i := 0; i < spec.KeysPerOperator; i++ {
			if err := keys.Add(ctx, spec.Key(1, op, i)); err != nil {
				build.Abort(ctx)
				return err
			}
		}
	}
	if _, err := keys.Commit(ctx); err != nil {
		build.Abort(ctx)
		return err
	}
	if _, err := build.Operators(ctx, module, spec.Operators(1)); err != nil {
		build.Abort(ctx)
		return err
	}
	if err := build.Module(ctx, module, true); err != nil {
		build.Abort(ctx)
		return err
	}
	return build.Publish(ctx, "latest", 0)
}
