package database_test

import (
	"context"
	"errors"
	"fmt"
	db "lib/shared_lib/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type testConnectionPool struct {
	QueryRowCalled      bool
	CloseCalled         bool
	QueryCalled         bool
	ExecCalled          bool
	ExecCalls           []string
	PoolQueryCalled     bool
	PoolQueryRowCalled  bool
	TxQueryCalled       bool
	TxQueryRowCalled    bool
	BeginTxCalled       bool
	BeginTxCalledCount  int
	CommitCalled        bool
	CommitCalledCount   int
	RollbackCalled      bool
	RollbackCalledCount int
	NextRow             pgx.Row
	NextQueryRow        pgx.Row
	NextRows            pgx.Rows
	NextTxRows          []pgx.Row
	NextError           error
	NextExecErrors      []error
	NextBeginError      error
	NextCommitError     error
	NextCommitErrors    []error
	NextRollbackError   error
	RollbackContextErr  error
	NextRowsAffected    int64
	ExecCalledCount     int
	LastQuery           string
	LastArgs            []any
}

func (p *testConnectionPool) Close() { p.CloseCalled = true }

type testErrRow struct{ err error }

func (r *testErrRow) Scan(...any) error { return r.err }

func (p *testConnectionPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.QueryRowCalled = true
	p.PoolQueryRowCalled = true
	p.LastQuery = sql
	p.LastArgs = args
	if p.NextRow != nil {
		return p.NextRow
	}
	if p.NextQueryRow != nil {
		return p.NextQueryRow
	}
	return &testErrRow{err: pgx.ErrNoRows}
}

func (p *testConnectionPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.QueryCalled = true
	p.PoolQueryCalled = true
	p.LastQuery = sql
	p.LastArgs = args
	return p.NextRows, p.NextError
}

func (p *testConnectionPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ExecCalled = true
	p.LastQuery = sql
	p.LastArgs = args
	p.ExecCalledCount++
	p.ExecCalls = append(p.ExecCalls, sql)
	nextErr := p.NextError
	if nextErr == nil && len(p.NextExecErrors) > 0 {
		nextErr = p.NextExecErrors[0]
		p.NextExecErrors = p.NextExecErrors[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.NextRowsAffected)), nextErr
}

func (p *testConnectionPool) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	p.BeginTxCalled = true
	p.BeginTxCalledCount++
	if p.NextBeginError != nil {
		return nil, p.NextBeginError
	}
	return &testTx{pool: p}, nil
}

type testTx struct{ pool *testConnectionPool }

func (tx *testTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *testTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *testTx) Conn() *pgx.Conn                                        { return nil }
func (tx *testTx) Begin(context.Context) (pgx.Tx, error) {
	if tx.pool.NextBeginError != nil {
		return nil, tx.pool.NextBeginError
	}
	return tx, nil
}
func (tx *testTx) Commit(context.Context) error {
	tx.pool.CommitCalled = true
	tx.pool.CommitCalledCount++
	if len(tx.pool.NextCommitErrors) > 0 {
		nextErr := tx.pool.NextCommitErrors[0]
		tx.pool.NextCommitErrors = tx.pool.NextCommitErrors[1:]
		return nextErr
	}
	return tx.pool.NextCommitError
}
func (tx *testTx) Rollback(ctx context.Context) error {
	tx.pool.RollbackCalled = true
	tx.pool.RollbackCalledCount++
	tx.pool.RollbackContextErr = ctx.Err()
	return tx.pool.NextRollbackError
}
func (tx *testTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.pool.ExecCalled = true
	tx.pool.ExecCalls = append(tx.pool.ExecCalls, sql)
	tx.pool.LastQuery = sql
	tx.pool.LastArgs = args
	tx.pool.ExecCalledCount++
	nextErr := tx.pool.NextError
	if nextErr == nil && len(tx.pool.NextExecErrors) > 0 {
		nextErr = tx.pool.NextExecErrors[0]
		tx.pool.NextExecErrors = tx.pool.NextExecErrors[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", tx.pool.NextRowsAffected)), nextErr
}
func (tx *testTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, tx.pool.NextError
}
func (tx *testTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.pool.QueryCalled = true
	tx.pool.TxQueryCalled = true
	tx.pool.LastQuery = sql
	tx.pool.LastArgs = args
	return tx.pool.NextRows, tx.pool.NextError
}
func (tx *testTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.pool.QueryRowCalled = true
	tx.pool.TxQueryRowCalled = true
	tx.pool.LastQuery = sql
	tx.pool.LastArgs = args
	if len(tx.pool.NextTxRows) > 0 {
		nextRow := tx.pool.NextTxRows[0]
		tx.pool.NextTxRows = tx.pool.NextTxRows[1:]
		return nextRow
	}
	return tx.pool.NextRow
}
func (tx *testTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

var _ = Describe("database intialization", func() {
	var (
		dbName string
		ctx    context.Context
	)

	BeforeEach(func() {
		dbName = "test_db"
		ctx = context.Background()
	})

	Describe("Initializing a database", func() {
		When("the connection string is invalid", func() {
			It("should return an error", func() {
				db, err := db.InitDatabase(ctx, dbName, "invalid_connection", nil)

				Expect(err).ToNot(BeNil())
				Expect(db).To(BeNil())
				Expect(err.Error()).To(ContainSubstring("failed to create database connection pool"))
			})

		})
	})

	When("Creating a new database", func() {
		It("should return a database with the given name", func() {
			database := db.NewDatabase(nil, dbName)

			Expect(database).ToNot(BeNil())
			Expect(database.Name).To(Equal(dbName))
		})
	})

	When("Closing the database", func() {
		It("should close the connection pool", func() {
			mockPool := &testConnectionPool{}
			database := db.NewDatabase(mockPool, dbName)

			database.Close()

			Expect(mockPool.CloseCalled).To(BeTrue())
		})
	})
})

var _ = Describe("UnitOfWork", func() {
	It("rolls back panic recovery with a live bounded background context", func() {
		pool := &testConnectionPool{}
		uow := db.NewUnitOfWork(pool)
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		Expect(func() {
			_ = uow.Do(canceledCtx, func(ctx context.Context, tx pgx.Tx) error {
				panic("boom")
			})
		}).To(PanicWith("boom"))

		Expect(pool.RollbackCalled).To(BeTrue())
		Expect(pool.RollbackContextErr).To(BeNil())
	})

	It("rolls back operation errors with a live bounded background context", func() {
		pool := &testConnectionPool{}
		uow := db.NewUnitOfWork(pool)
		opErr := errors.New("operation failed")
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err := uow.Do(canceledCtx, func(ctx context.Context, tx pgx.Tx) error {
			return opErr
		})

		Expect(err).To(MatchError(opErr))
		Expect(pool.RollbackCalled).To(BeTrue())
		Expect(pool.RollbackContextErr).To(BeNil())
		Expect(pool.CommitCalled).To(BeFalse())
	})

	It("wraps commit failure with rollback failure when both fail", func() {
		commitErr := errors.New("commit failed")
		rollbackErr := errors.New("rollback failed")
		pool := &testConnectionPool{
			NextCommitError:   commitErr,
			NextRollbackError: rollbackErr,
		}
		uow := db.NewUnitOfWork(pool)
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err := uow.Do(canceledCtx, func(ctx context.Context, tx pgx.Tx) error {
			return nil
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("commit tx: commit failed; rollback tx: rollback failed"))
		Expect(errors.Is(err, commitErr)).To(BeTrue())
		Expect(pool.CommitCalled).To(BeTrue())
		Expect(pool.RollbackCalled).To(BeTrue())
		Expect(pool.RollbackContextErr).To(BeNil())
	})

	It("retries a transient commit lock timeout without surfacing tx-closed rollback noise", func() {
		lockTimeout := &pgconn.PgError{
			Code:    "55P03",
			Message: "canceling statement due to lock timeout",
		}
		pool := &testConnectionPool{
			NextCommitErrors:  []error{lockTimeout, nil},
			NextRollbackError: pgx.ErrTxClosed,
		}
		uow := db.NewUnitOfWork(pool)
		uow.RetryAttempts = 2
		uow.RetryBaseBackoff = 0

		calls := 0
		err := uow.Do(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
			calls++
			return nil
		})

		Expect(err).ToNot(HaveOccurred())
		Expect(calls).To(Equal(2))
		Expect(pool.BeginTxCalledCount).To(Equal(2))
		Expect(pool.CommitCalledCount).To(Equal(2))
		Expect(pool.RollbackCalledCount).To(Equal(1))
	})
})
