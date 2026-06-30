package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"lib/shared_lib/ctxutil"
	sharedtrace "lib/shared_lib/trace"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	sqlStateSerializationFailure = "40001"
	sqlStateDeadlockDetected     = "40P01"
	sqlStateLockNotAvailable     = "55P03"
)

type TxRunner interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type UnitOfWork struct {
	Pool             ConnectionPool
	IsoLevel         pgx.TxIsoLevel
	AccessMode       pgx.TxAccessMode
	RollbackTimeout  time.Duration
	RetryAttempts    int
	RetryBaseBackoff time.Duration
}

func NewUnitOfWork(pool ConnectionPool) *UnitOfWork {
	return &UnitOfWork{
		Pool: pool,
	}
}

func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) (err error) {
	attempts := u.RetryAttempts
	if attempts <= 0 {
		attempts = 1
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		err = u.doOnce(ctx, fn)
		if !IsRetryableTransactionError(err) || attempt == attempts || ctx.Err() != nil {
			return err
		}
		backoff := time.Duration(attempt) * u.RetryBaseBackoff
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"attempt":      attempt,
			"max_attempts": attempts,
			"backoff":      backoff.String(),
		}).Warn("retrying shared database transaction after retryable error")
		if backoff <= 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
	}
	return err
}

func (u *UnitOfWork) doOnce(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) (err error) {
	ctx, span := sharedtrace.StartSpan(ctx, "shared_lib/db", "db.transaction",
		attribute.String("db.transaction.owner", "shared_lib"),
	)
	defer span.End()

	tx, err := u.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   u.IsoLevel,
		AccessMode: u.AccessMode,
	})
	if err != nil {
		sharedtrace.RecordSpanError(span, err)
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			sharedtrace.RecordSpanError(span, fmt.Errorf("panic: %v", p))
			_ = u.rollback(tx)
			panic(p)
		}
	}()

	if err = fn(ctx, tx); err != nil {
		if rbErr := u.rollback(tx); rbErr != nil && !ignoreRollbackError(rbErr) {
			// Preserve fn errors without a transaction prefix; they already carry
			// domain context. Commit errors are prefixed below so they stand apart
			// from operation failures.
			sharedtrace.RecordSpanErrorFromContext(ctx, span, err)
			sharedtrace.RecordSpanError(span, rbErr)
			return fmt.Errorf("%w; rollback tx: %v", err, rbErr)
		}
		sharedtrace.RecordSpanErrorFromContext(ctx, span, err)
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		if rbErr := u.rollback(tx); rbErr != nil && !ignoreRollbackError(rbErr) {
			sharedtrace.RecordSpanError(span, err)
			span.RecordError(rbErr)
			return fmt.Errorf("commit tx: %w; rollback tx: %v", err, rbErr)
		}
		sharedtrace.RecordSpanError(span, err)
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func (u *UnitOfWork) rollback(tx pgx.Tx) error {
	if u.RollbackTimeout <= 0 {
		return tx.Rollback(context.Background())
	}
	rollbackCtx, cancel := context.WithTimeout(context.Background(), u.RollbackTimeout)
	defer cancel()
	return tx.Rollback(rollbackCtx)
}

func ignoreRollbackError(err error) bool {
	return ctxutil.IsCanceled(err) || errors.Is(err, pgx.ErrTxClosed)
}

func IsRetryableTransactionError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case sqlStateLockNotAvailable,
		sqlStateDeadlockDetected,
		sqlStateSerializationFailure:
		return true
	default:
		return false
	}
}

func isRetryableTransactionError(err error) bool {
	return IsRetryableTransactionError(err)
}
