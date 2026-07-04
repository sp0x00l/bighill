package tenant_test

import (
	"context"
	sharedDB "lib/shared_lib/db"
	sharedDomain "lib/shared_lib/domain"
	"lib/shared_lib/tenant"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type projectionPoolStub struct {
	lastQuery string
	lastArgs  []any
	nextRow   pgx.Row
	nextErr   error
}

func (p *projectionPoolStub) Close() {}

func (p *projectionPoolStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.lastQuery = sql
	p.lastArgs = args
	return pgconn.NewCommandTag("INSERT 1"), p.nextErr
}

func (p *projectionPoolStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.lastQuery = sql
	p.lastArgs = args
	if p.nextRow != nil {
		return p.nextRow
	}
	return projectionRowStub{err: pgx.ErrNoRows}
}

func (p *projectionPoolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, p.nextErr
}

func (p *projectionPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return projectionTxStub{pool: p}, nil
}

type projectionTxStub struct {
	pool *projectionPoolStub
}

func (tx projectionTxStub) Begin(context.Context) (pgx.Tx, error) { return tx, nil }
func (tx projectionTxStub) Commit(context.Context) error          { return nil }
func (tx projectionTxStub) Rollback(context.Context) error        { return nil }
func (tx projectionTxStub) Conn() *pgx.Conn                       { return nil }
func (tx projectionTxStub) LargeObjects() pgx.LargeObjects        { return pgx.LargeObjects{} }
func (tx projectionTxStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (tx projectionTxStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, tx.pool.nextErr
}
func (tx projectionTxStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}
func (tx projectionTxStub) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "set_config('app.system_context'") {
		return pgconn.NewCommandTag("SET"), nil
	}
	return tx.pool.Exec(ctx, sql, args...)
}
func (tx projectionTxStub) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.pool.Query(ctx, sql, args...)
}
func (tx projectionTxStub) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.pool.QueryRow(ctx, sql, args...)
}

type projectionRowStub struct {
	id         uuid.UUID
	email      string
	ciphertext string
	deleted    bool
	updatedAt  time.Time
	err        error
}

func (r projectionRowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*string)) = r.id.String()
	*(dest[1].(*string)) = r.email
	*(dest[2].(*string)) = r.ciphertext
	*(dest[3].(*bool)) = r.deleted
	*(dest[4].(*time.Time)) = r.updatedAt
	return nil
}

var _ = Describe("PostgresProjectionStore", func() {
	var (
		pool  *projectionPoolStub
		store *tenant.PostgresProjectionStore
	)

	BeforeEach(func() {
		pool = &projectionPoolStub{}
		store = tenant.NewPostgresProjectionStore(sharedDB.NewDatabase(pool, "test_db"))
	})

	It("upserts tenant projections", func() {
		tenantID := uuid.New()

		err := store.Upsert(context.Background(), &sharedDomain.Tenant{
			TenantID:                   tenantID,
			Email:                      "user@example.com",
			HuggingFaceTokenCiphertext: "ciphertext-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.tenants"))
		Expect(pool.lastQuery).To(ContainSubstring("ON CONFLICT (id)"))
		Expect(pool.lastArgs[0]).To(HaveKey("id"))
		Expect(pool.lastArgs[0]).To(HaveKey("huggingface_token_ciphertext"))
	})

	It("soft-deletes tenant projections", func() {
		tenantID := uuid.New()

		err := store.Delete(context.Background(), tenantID)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.tenants"))
		Expect(pool.lastQuery).To(ContainSubstring("deleted = true"))
	})

	It("reads a tenant projection", func() {
		tenantID := uuid.New()
		updatedAt := time.Now().UTC()
		pool.nextRow = projectionRowStub{
			id:         tenantID,
			email:      "user@example.com",
			ciphertext: "ciphertext-1",
			updatedAt:  updatedAt,
		}

		record, err := store.Read(context.Background(), tenantID)

		Expect(err).NotTo(HaveOccurred())
		Expect(record.TenantID).To(Equal(tenantID))
		Expect(record.Email).To(Equal("user@example.com"))
		Expect(record.HuggingFaceTokenCiphertext).To(Equal("ciphertext-1"))
		Expect(record.UpdatedAt).To(Equal(updatedAt))
	})

	It("returns ErrTenantNotFound when the projection is absent", func() {
		_, err := store.Read(context.Background(), uuid.New())

		Expect(err).To(MatchError(tenant.ErrTenantNotFound))
	})
})
