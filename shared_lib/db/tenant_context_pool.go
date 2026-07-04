package database

import (
	"context"
	"fmt"
	"sync"

	"lib/shared_lib/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type tenantContextPool struct {
	base ConnectionPool
}

func newTenantContextPool(base ConnectionPool) ConnectionPool {
	return &tenantContextPool{base: base}
}

func (p *tenantContextPool) Close() {
	p.base.Close()
}

func (p *tenantContextPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !needsSessionContext(ctx) {
		return p.base.QueryRow(ctx, sql, args...)
	}
	tx, err := p.beginScopedTx(ctx)
	if err != nil {
		return errorRow{err: err}
	}
	row := tx.QueryRow(ctx, sql, args...)
	return scopedRow{ctx: ctx, tx: tx, row: row}
}

func (p *tenantContextPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !needsSessionContext(ctx) {
		return p.base.Query(ctx, sql, args...)
	}
	tx, err := p.beginScopedTx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return &scopedRows{ctx: ctx, tx: tx, rows: rows}, nil
}

func (p *tenantContextPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if !needsSessionContext(ctx) {
		return p.base.Exec(ctx, sql, args...)
	}
	tx, err := p.beginScopedTx(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		_ = tx.Rollback(ctx)
		return tag, err
	}
	if err := tx.Commit(ctx); err != nil {
		return tag, err
	}
	return tag, nil
}

func (p *tenantContextPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx, err := p.base.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	if err := applySessionContext(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func (p *tenantContextPool) beginScopedTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.base.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	if err := applySessionContext(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func applySessionContext(ctx context.Context, tx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	if ctxutil.IsSystemContext(ctx) {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.system_context', 'true', true)`); err != nil {
			return fmt.Errorf("set system database context: %w", err)
		}
		return nil
	}
	if tenantID, ok := ctxutil.TenantID(ctx); ok {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.current_user_id', $1, true)`, tenantID.String()); err != nil {
			return fmt.Errorf("set tenant database context: %w", err)
		}
	}
	return nil
}

func needsSessionContext(ctx context.Context) bool {
	if ctxutil.IsSystemContext(ctx) {
		return true
	}
	_, ok := ctxutil.TenantID(ctx)
	return ok
}

func unwrapPgxPool(pool ConnectionPool) (*pgxpool.Pool, bool) {
	switch p := pool.(type) {
	case *pgxpool.Pool:
		return p, true
	case *tenantContextPool:
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

type scopedRow struct {
	ctx context.Context
	tx  pgx.Tx
	row pgx.Row
}

func (r scopedRow) Scan(dest ...any) error {
	if err := r.row.Scan(dest...); err != nil {
		_ = r.tx.Rollback(r.ctx)
		return err
	}
	if err := r.tx.Commit(r.ctx); err != nil {
		return err
	}
	return nil
}

type scopedRows struct {
	ctx      context.Context
	tx       pgx.Tx
	rows     pgx.Rows
	closeErr error
	once     sync.Once
}

func (r *scopedRows) Close() {
	r.once.Do(func() {
		r.rows.Close()
		if err := r.rows.Err(); err != nil {
			r.closeErr = err
			_ = r.tx.Rollback(r.ctx)
			return
		}
		r.closeErr = r.tx.Commit(r.ctx)
	})
}

func (r *scopedRows) Err() error {
	if err := r.rows.Err(); err != nil {
		return err
	}
	return r.closeErr
}

func (r *scopedRows) CommandTag() pgconn.CommandTag {
	return r.rows.CommandTag()
}

func (r *scopedRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.rows.FieldDescriptions()
}

func (r *scopedRows) Next() bool {
	ok := r.rows.Next()
	if !ok {
		r.Close()
	}
	return ok
}

func (r *scopedRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *scopedRows) Values() ([]any, error) {
	return r.rows.Values()
}

func (r *scopedRows) RawValues() [][]byte {
	return r.rows.RawValues()
}

func (r *scopedRows) Conn() *pgx.Conn {
	return r.rows.Conn()
}
