package database

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"lib/shared_lib/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

var ErrDirectPoolUseInTransaction = errors.New("direct database pool use inside UnitOfWork transaction")

type guardedConnectionPool struct {
	base ConnectionPool
}

func newGuardedConnectionPool(base ConnectionPool) ConnectionPool {
	return &guardedConnectionPool{base: base}
}

func (p *guardedConnectionPool) Close() {
	p.base.Close()
}

func (p *guardedConnectionPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if ctxutil.IsTransactionContext(ctx) {
		return errorRow{err: ErrDirectPoolUseInTransaction}
	}
	logMissingTenantContext(ctx, sql)
	return p.base.QueryRow(ctx, sql, args...)
}

func (p *guardedConnectionPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if ctxutil.IsTransactionContext(ctx) {
		return nil, ErrDirectPoolUseInTransaction
	}
	logMissingTenantContext(ctx, sql)
	return p.base.Query(ctx, sql, args...)
}

func (p *guardedConnectionPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if ctxutil.IsTransactionContext(ctx) {
		return pgconn.CommandTag{}, ErrDirectPoolUseInTransaction
	}
	logMissingTenantContext(ctx, sql)
	return p.base.Exec(ctx, sql, args...)
}

func (p *guardedConnectionPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return p.base.BeginTx(ctx, txOptions)
}

func unwrapPgxPool(pool ConnectionPool) (*pgxpool.Pool, bool) {
	switch p := pool.(type) {
	case *pgxpool.Pool:
		return p, true
	case *guardedConnectionPool:
		return unwrapPgxPool(p.base)
	default:
		return nil, false
	}
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(...any) error {
	return r.err
}

var rlsTablePattern = regexp.MustCompile(`(?i)\b(?:bighill_[a-z_]+_db\.)?(?:tenants|datasets|metadata|source_connectors|upload_sessions|raw_snapshots|feature_snapshots|embedding_snapshots|embedding_records|inference_datasets|inference_models|inference_requests|inference_feedback|preference_examples|preference_dataset_snapshots|models)\b`)

func logMissingTenantContext(ctx context.Context, sql string) {
	if ctxutil.IsSystemContext(ctx) {
		return
	}
	if _, ok := ctxutil.TenantID(ctx); ok {
		return
	}
	if !rlsTablePattern.MatchString(sql) {
		return
	}

	log.WithContext(ctx).WithField("sql", compactSQL(sql)).Debug("database query touches tenant-scoped table without tenant or system context")
}

func compactSQL(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return ""
	}
	compact := strings.Join(fields, " ")
	if len(compact) > 300 {
		return compact[:300] + "..."
	}
	return compact
}
