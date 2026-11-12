package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// The writer lock uses the two-int advisory key; leases use the single-bigint one, and pg_locks
// tells them apart (objsubid 2 vs 1), so the writer is never read as a lease.
const (
	writerLockClass = 0x6b617069 // "kapi"
	writerLockKey   = `hashtext(current_setting('search_path'))`
)

const (
	defaultWriterLockCheck = 10 * time.Second
	defaultWriterRetry     = 5 * time.Second
)

type WriterLockOptions struct {
	// OnWait is called once per attempt when another writer holds the lock and this one waits.
	OnWait func()
	// CheckInterval: how often the holding session is pinged; zero means 10 s.
	CheckInterval time.Duration
	// OnStop (RunExclusive) is told why the writer stopped before it takes the lock again.
	OnStop func(error)
	// RetryDelay (RunExclusive): the pause before taking the lock again; zero means 5 s.
	RetryDelay time.Duration
}

// WriterLock is held by a session of its own for the writer's life. Two writers of one schema
// rewrite each other's rows and both report success, so the second waits for the first.
type WriterLock struct {
	conn   *pgx.Conn
	lost   chan struct{}
	err    error
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *PGStore) LockWriter(ctx context.Context, o WriterLockOptions) (*WriterLock, error) {
	conn, err := pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("writer lock: connect: %w", err)
	}
	fail := func(err error) (*WriterLock, error) {
		_ = conn.Close(context.Background())
		return nil, fmt.Errorf("writer lock: %w", err)
	}

	var got bool
	if err := conn.QueryRow(ctx, `select pg_try_advisory_lock($1, `+writerLockKey+`)`, writerLockClass).Scan(&got); err != nil {
		return fail(err)
	}
	if !got {
		if o.OnWait != nil {
			o.OnWait()
		}
		if _, err := conn.Exec(ctx, `select pg_advisory_lock($1, `+writerLockKey+`)`, writerLockClass); err != nil {
			return fail(err)
		}
	}

	interval := o.CheckInterval
	if interval <= 0 {
		interval = defaultWriterLockCheck
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	l := &WriterLock{conn: conn, lost: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
	go l.watch(watchCtx, interval)
	return l, nil
}

func (l *WriterLock) watch(ctx context.Context, interval time.Duration) {
	defer close(l.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, interval)
		err := l.conn.Ping(pingCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			l.err = fmt.Errorf("the session holding the writer lock is gone, another writer may take over: %w", err)
			close(l.lost)
			return
		}
	}
}

// Lost is closed when the holding session dies; the writer must stop, since the lock went with it.
func (l *WriterLock) Lost() <-chan struct{} { return l.lost }

func (l *WriterLock) Err() error { return l.err }

// RunExclusive runs fn while holding the writer lock. A lost lock cancels fn, and the lock is taken
// again after the retry delay; only ctx ends it, so a database restart does not stop the process.
func (s *PGStore) RunExclusive(ctx context.Context, o WriterLockOptions, fn func(context.Context) error) error {
	retry := o.RetryDelay
	if retry <= 0 {
		retry = defaultWriterRetry
	}
	for {
		err := s.runLocked(ctx, o, fn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if o.OnStop != nil {
			o.OnStop(err)
		}
		timer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *PGStore) runLocked(ctx context.Context, o WriterLockOptions, fn func(context.Context) error) error {
	lock, err := s.LockWriter(ctx, o)
	if err != nil {
		return err
	}
	defer lock.Release()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-lock.Lost():
			cancel()
		case <-runCtx.Done():
		}
	}()
	err = fn(runCtx)
	select {
	case <-lock.Lost():
		return lock.Err()
	default:
	}
	if err == nil {
		err = fmt.Errorf("writer returned")
	}
	return err
}

// Release closes the session, which releases the lock.
func (l *WriterLock) Release() {
	l.cancel()
	<-l.done
	_ = l.conn.Close(context.Background())
}
