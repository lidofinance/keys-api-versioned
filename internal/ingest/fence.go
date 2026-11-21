package ingest

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrFenced: another writer took the lock and the epoch after this one lost its lock session.
type ErrFenced struct {
	Own, Current int64
}

func (e ErrFenced) Error() string {
	if e.Own == 0 {
		return "write refused: this process never took the writer epoch"
	}
	return fmt.Sprintf("write refused: writer epoch %d, another writer holds %d", e.Own, e.Current)
}

// TakeEpoch makes this process the only one whose writes land; call it holding the writer lock,
// after Migrate (a rebuild recreates the table).
func (s *PGStore) TakeEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	if err := s.pool.QueryRow(ctx,
		`update writer_fence set epoch = epoch + 1 returning epoch`).Scan(&epoch); err != nil {
		return 0, fmt.Errorf("take the writer epoch: %w", err)
	}
	s.epoch.Store(epoch)
	return epoch, nil
}

type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// beginWrite: the lock session can die while the pool still works, and the old writer learns of
// it only at its next ping. The fence check is the transaction's first statement.
func (s *PGStore) beginWrite(ctx context.Context, b beginner) (pgx.Tx, error) {
	tx, err := b.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var current int64
	if err := tx.QueryRow(ctx, `select epoch from writer_fence for share`).Scan(&current); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("writer fence: %w", err)
	}
	if own := s.epoch.Load(); own == 0 || own != current {
		_ = tx.Rollback(ctx)
		return nil, ErrFenced{Own: own, Current: current}
	}
	return tx, nil
}
