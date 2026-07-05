package database_test

import (
	"context"
	"errors"
	"fmt"
	"lib/shared_lib/ctxutil"
	db "lib/shared_lib/db"
	"os"
	"strings"

	"github.com/google/uuid"
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
	ExecArgs            [][]any
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
	BeginTxContext      context.Context
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
	p.ExecArgs = append(p.ExecArgs, args)
	nextErr := p.NextError
	if nextErr == nil && len(p.NextExecErrors) > 0 {
		nextErr = p.NextExecErrors[0]
		p.NextExecErrors = p.NextExecErrors[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.NextRowsAffected)), nextErr
}

func (p *testConnectionPool) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	p.BeginTxCalled = true
	p.BeginTxCalledCount++
	p.BeginTxContext = ctx
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
	tx.pool.ExecArgs = append(tx.pool.ExecArgs, args)
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

type testRows struct {
	closed bool
	err    error
	index  int
	count  int
}

func (r *testRows) Close() {
	r.closed = true
}

func (r *testRows) Err() error {
	return r.err
}

func (r *testRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT 1")
}

func (r *testRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *testRows) Next() bool {
	if r.index >= r.count {
		return false
	}
	r.index++
	return true
}

func (r *testRows) Scan(...any) error {
	return nil
}

func (r *testRows) Values() ([]any, error) {
	return nil, nil
}

func (r *testRows) RawValues() [][]byte {
	return nil
}

func (r *testRows) Conn() *pgx.Conn {
	return nil
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

	It("passes tenant context through to pool acquisition and marks the callback transaction-scoped", func() {
		pool := &testConnectionPool{}
		uow := db.NewUnitOfWork(pool)
		tenantID := uuid.New()
		var callbackCtx context.Context

		err := uow.Do(ctxutil.WithTenantID(context.Background(), tenantID), func(ctx context.Context, tx pgx.Tx) error {
			callbackCtx = ctx
			return nil
		})

		Expect(err).ToNot(HaveOccurred())
		beginTenantID, ok := ctxutil.TenantID(pool.BeginTxContext)
		Expect(ok).To(BeTrue())
		Expect(beginTenantID).To(Equal(tenantID))
		Expect(ctxutil.IsTransactionContext(callbackCtx)).To(BeTrue())
		Expect(pool.ExecCalls).To(BeEmpty())
		Expect(pool.CommitCalled).To(BeTrue())
	})
})

var _ = Describe("tenant context connection pool", func() {
	It("uses the base pool directly when no tenant or system context is present", func() {
		pool := &testConnectionPool{}
		database := db.NewDatabase(pool, "test_db")

		err := database.Pool.QueryRow(context.Background(), "SELECT 1").Scan()

		Expect(err).To(Equal(pgx.ErrNoRows))
		Expect(pool.PoolQueryRowCalled).To(BeTrue())
		Expect(pool.BeginTxCalled).To(BeFalse())
		Expect(pool.ExecCalls).To(BeEmpty())
	})

	It("uses the base pool directly when tenant context is present", func() {
		pool := &testConnectionPool{
			NextRow: &testErrRow{},
		}
		database := db.NewDatabase(pool, "test_db")
		tenantID := uuid.New()

		err := database.Pool.QueryRow(ctxutil.WithTenantID(context.Background(), tenantID), "SELECT 1").Scan()

		Expect(err).ToNot(HaveOccurred())
		Expect(pool.PoolQueryRowCalled).To(BeTrue())
		Expect(pool.BeginTxCalled).To(BeFalse())
		Expect(pool.ExecCalls).To(BeEmpty())
	})

	It("uses the base pool directly when system context is present", func() {
		pool := &testConnectionPool{}
		database := db.NewDatabase(pool, "test_db")

		_, err := database.Pool.Exec(ctxutil.WithSystemContext(context.Background()), "INSERT INTO tenants VALUES ($1)", uuid.New())

		Expect(err).ToNot(HaveOccurred())
		Expect(pool.ExecCalled).To(BeTrue())
		Expect(pool.BeginTxCalled).To(BeFalse())
	})

	It("rejects direct pool Exec calls from inside UnitOfWork callbacks", func() {
		pool := &testConnectionPool{}
		database := db.NewDatabase(pool, "test_db")
		uow := db.NewUnitOfWork(database.Pool)

		err := uow.Do(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
			_, execErr := database.Pool.Exec(ctx, "UPDATE datasets SET title = $1", "bad")
			return execErr
		})

		Expect(errors.Is(err, db.ErrDirectPoolUseInTransaction)).To(BeTrue())
		Expect(pool.ExecCalled).To(BeFalse())
		Expect(pool.RollbackCalled).To(BeTrue())
	})

	It("rejects direct pool QueryRow calls from inside UnitOfWork callbacks", func() {
		pool := &testConnectionPool{}
		database := db.NewDatabase(pool, "test_db")
		uow := db.NewUnitOfWork(database.Pool)

		err := uow.Do(context.Background(), func(ctx context.Context, tx pgx.Tx) error {
			return database.Pool.QueryRow(ctx, "SELECT * FROM datasets WHERE id = $1", uuid.New()).Scan()
		})

		Expect(errors.Is(err, db.ErrDirectPoolUseInTransaction)).To(BeTrue())
		Expect(pool.QueryRowCalled).To(BeFalse())
		Expect(pool.RollbackCalled).To(BeTrue())
	})
})

var _ = Describe("database error classifiers", func() {
	It("detects row-level-security violations", func() {
		err := &pgconn.PgError{
			Code:    "42501",
			Message: `new row violates row-level security policy for table "datasets"`,
		}

		Expect(db.IsRowLevelSecurityViolation(err)).To(BeTrue())
	})

	It("does not classify generic insufficient privilege as row-level-security", func() {
		err := &pgconn.PgError{
			Code:    "42501",
			Message: `permission denied for table datasets`,
		}

		Expect(db.IsRowLevelSecurityViolation(err)).To(BeFalse())
	})
})

var _ = Describe("tenant RLS migration policies", func() {
	It("keeps tenant-scoped service policies fail-closed", func() {
		paths := []string{
			"../../data_registry_service/db/migrations/000001_init_schema.up.sql",
			"../../feature_materializer_service/db/migrations/000001_init_schema.up.sql",
			"../../inference_service/db/migrations/000001_init_schema.up.sql",
			"../../ingestion_service/db/migrations/000001_init_schema.up.sql",
			"../../model_registry_service/db/migrations/000001_init_schema.up.sql",
		}

		for _, path := range paths {
			contentBytes, err := os.ReadFile(path)
			Expect(err).ToNot(HaveOccurred(), path)
			content := string(contentBytes)

			Expect(content).To(ContainSubstring("FORCE ROW LEVEL SECURITY"), path)
			Expect(content).To(ContainSubstring("current_setting('app.system_context', true) = 'true'"), path)
			Expect(content).To(ContainSubstring("NULLIF(current_setting('app.current_user_id', true), '')::uuid"), path)
			Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_user_id'")).To(BeFalse(), path)
			Expect(strings.Contains(content, "user_id = user_id")).To(BeFalse(), path)
			Expect(strings.Contains(content, "status = 'published'")).To(BeFalse(), path)
		}
	})
})
