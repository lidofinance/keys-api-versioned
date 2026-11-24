package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lidofinance/keys-api-versioned/internal/store"
)

// Two-int advisory key with a constant class; leases (internal/store) take their class from the
// schema, so the writer is never read as a lease.
const (
	writerLockClass  = 0x6b617069 // "kapi"
	migrateLockClass = writerLockClass + 1
	writerLockKey    = `hashtext(` + store.SchemaKey + `)`
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

	noWait bool
}

// ErrTakenOver: this writer lost its lock, and another took it before this one could again.
var ErrTakenOver = errors.New("another writer took the writer lock this one lost")

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
		if o.noWait {
			return fail(ErrTakenOver)
		}
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
// again after the retry delay, so a database restart does not stop the process. It returns once
// another writer has taken over (ErrFenced, ErrTakenOver): only a writer that never held the lock
// waits for it, as a new pod does in a rollout.
func (s *PGStore) RunExclusive(ctx context.Context, o WriterLockOptions, fn func(context.Context) error) error {
	retry := o.RetryDelay
	if retry <= 0 {
		retry = defaultWriterRetry
	}
	for {
		held, err := s.runLocked(ctx, o, fn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A newer release's store stays newer: retrying cannot help, and exiting shows in the pod status.
		if fenced, newer := (ErrFenced{}), (ErrNewerStore{}); errors.As(err, &fenced) || errors.Is(err, ErrTakenOver) ||
			errors.As(err, &newer) {
			return err
		}
		o.noWait = o.noWait || held
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

func (s *PGStore) runLocked(ctx context.Context, o WriterLockOptions, fn func(context.Context) error) (bool, error) {
	lock, err := s.LockWriter(ctx, o)
	if err != nil {
		return false, err
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
	if fenced := (ErrFenced{}); errors.As(err, &fenced) {
		return true, err
	}
	select {
	case <-lock.Lost():
		return true, lock.Err()
	default:
	}
	if err == nil {
		err = fmt.Errorf("writer returned")
	}
	return true, err
}

// Release closes the session, which releases the lock.
func (l *WriterLock) Release() {
	l.cancel()
	<-l.done
	_ = l.conn.Close(context.Background())
}
