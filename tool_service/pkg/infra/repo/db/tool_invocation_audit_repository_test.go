package db

import (
	"context"
	"errors"
	"strings"
	"testing"

	"tool_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInvocationAuditRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool Service Invocation Audit Repository Suite")
}

var _ = Describe("InvocationAuditRepository", func() {
	var (
		ctx        context.Context
		pool       *testConnectionPool
		repository *InvocationAuditRepository
	)

	BeforeEach(func() {
		ctx = ctxutil.WithOrgID(context.Background(), uuid.New())
		pool = &testConnectionPool{}
		repository = NewInvocationAuditRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	It("records boundary audit rows idempotently", func() {
		invocationID := uuid.New()
		orgID := uuid.New()
		userID := uuid.New()

		err := repository.RecordInvocation(ctx, model.ToolInvocationAudit{
			InvocationID:          invocationID,
			OrgID:                 orgID,
			UserID:                userID,
			ToolName:              "http_get",
			ImplementationVersion: "http_get:v1",
			ExecutorKind:          model.ToolExecutorKindHTTPGet,
			Status:                model.ToolInvocationAuditStatusFailed,
			ErrorCode:             "http_tool_request_failed",
			ErrorType:             model.ToolErrorTypeTransient,
			LatencyMs:             17,
			EgressHost:            "example.com",
			TraceID:               "trace-1",
			ArgsHash:              "sha256:abc",
			ArgsPreview:           "keys:url",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.execCalled).To(BeTrue())
		Expect(pool.lastSQL).To(ContainSubstring("INSERT INTO test_db.tool_invocation_audit"))
		Expect(pool.lastSQL).To(ContainSubstring("ON CONFLICT (invocation_id) DO NOTHING"))
		args := namedAuditArgs(pool.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("invocation_id", pgtype.UUID{Bytes: invocationID, Valid: true}),
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}),
			HaveKeyWithValue("tool_name", "http_get"),
			HaveKeyWithValue("tool_impl_version", "http_get:v1"),
			HaveKeyWithValue("executor_kind", model.ToolExecutorKindHTTPGet.String()),
			HaveKeyWithValue("status", model.ToolInvocationAuditStatusFailed.String()),
			HaveKeyWithValue("error_code", "http_tool_request_failed"),
			HaveKeyWithValue("error_type", pgtype.Text{String: model.ToolErrorTypeTransient.String(), Valid: true}),
			HaveKeyWithValue("latency_ms", int64(17)),
			HaveKeyWithValue("egress_host", "example.com"),
			HaveKeyWithValue("trace_id", "trace-1"),
			HaveKeyWithValue("args_hash", "sha256:abc"),
			HaveKeyWithValue("args_preview", "keys:url"),
		))
	})

	It("wraps persistence errors", func() {
		pool.execErr = errors.New("database unavailable")

		err := repository.RecordInvocation(ctx, model.ToolInvocationAudit{
			InvocationID: uuid.New(),
			OrgID:        uuid.New(),
			UserID:       uuid.New(),
			ToolName:     "http_get",
			Status:       model.ToolInvocationAuditStatusDenied,
		})

		Expect(err).To(MatchError(ContainSubstring("record tool invocation audit")))
		Expect(err).To(MatchError(ContainSubstring("database unavailable")))
	})
})

type testConnectionPool struct {
	execCalled bool
	lastSQL    string
	lastArgs   []any
	execErr    error
}

func (p *testConnectionPool) Close() {}

func (p *testConnectionPool) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

func (p *testConnectionPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (p *testConnectionPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execCalled = true
	p.lastSQL = compactAuditSQL(sql)
	p.lastArgs = args
	return pgconn.NewCommandTag("INSERT 0 1"), p.execErr
}

func (p *testConnectionPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, nil
}

func namedAuditArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func compactAuditSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
