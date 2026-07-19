package db_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	coreDB "lib/shared_lib/db"
	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"
	toolexecutiondb "tool_execution_service/pkg/infra/repo/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ToolCatalogProjectionRepository", func() {
	var (
		ctx        context.Context
		pool       *toolCatalogProjectionPoolStub
		repository *toolexecutiondb.ToolCatalogProjectionRepository
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &toolCatalogProjectionPoolStub{rowsAffected: 1}
		repository = toolexecutiondb.NewToolCatalogProjectionRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	It("applies capability projections from the catalog", func() {
		projection := validCapabilityProjection()

		err := repository.ApplyCapabilityProjection(ctx, projection)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.execCalled).To(BeTrue())
		Expect(pool.lastSQL).To(ContainSubstring("INSERT INTO test_db.tool_capability_projections"))
		Expect(pool.lastSQL).To(ContainSubstring("ON CONFLICT (capability_version_id) DO UPDATE"))
		args := namedProjectionArgs(pool.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("capability_version_id", pgtype.UUID{Bytes: projection.CapabilityVersionID, Valid: true}),
			HaveKeyWithValue("capability_id", projection.CapabilityID),
			HaveKeyWithValue("version", projection.Version),
			HaveKeyWithValue("tool_name", projection.ToolName),
			HaveKeyWithValue("executor_kind", projection.ExecutorKind.String()),
			HaveKeyWithValue("mcp_server_endpoint", projection.MCPServerEndpoint),
			HaveKeyWithValue("parameters_json", string(projection.ParametersJSON)),
			HaveKeyWithValue("implementation_version", projection.ImplementationVersion),
			HaveKeyWithValue("credential_required", projection.CredentialRequired),
			HaveKeyWithValue("content_hash", projection.ContentHash),
		))
	})

	It("applies grant projections with tenant scope", func() {
		projection := model.ToolGrantProjection{
			OrgID:               uuid.New(),
			CapabilityVersionID: uuid.New(),
			Status:              "ACTIVE",
		}

		err := repository.ApplyGrantProjection(ctx, projection)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.lastSQL).To(ContainSubstring("INSERT INTO test_db.tool_grant_projections"))
		args := namedProjectionArgs(pool.lastArgs)
		Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: projection.OrgID, Valid: true}))
		Expect(args).To(HaveKeyWithValue("capability_version_id", pgtype.UUID{Bytes: projection.CapabilityVersionID, Valid: true}))
		Expect(args).To(HaveKeyWithValue("status", "ACTIVE"))
	})

	It("applies credential binding projections without raw credential values", func() {
		projection := model.ToolCredentialBindingProjection{
			OrgID:         uuid.New(),
			CapabilityID:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		}

		err := repository.ApplyCredentialBindingProjection(ctx, projection)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.lastSQL).To(ContainSubstring("INSERT INTO test_db.tool_credential_binding_projections"))
		args := namedProjectionArgs(pool.lastArgs)
		Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: projection.OrgID, Valid: true}))
		Expect(args).To(HaveKeyWithValue("capability_id", projection.CapabilityID))
		Expect(args).To(HaveKeyWithValue("credential_ref", projection.CredentialRef))
	})

	It("lists active catalog tools granted to the tenant", func() {
		orgID := uuid.New()
		userID := uuid.New()
		projected := validProjectedTool()
		pool.rows = &projectedToolRowsStub{rows: []pgx.Row{projectedToolRow{tool: projected}}}

		tools, err := repository.ListAvailableTools(ctx, orgID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(HaveLen(1))
		Expect(tools[0]).To(Equal(projected))
		Expect(pool.queryCalled).To(BeTrue())
		Expect(pool.lastSQL).To(ContainSubstring("JOIN test_db.tool_grant_projections"))
		Expect(pool.lastSQL).To(ContainSubstring("c.credential_required = false OR b.credential_ref IS NOT NULL"))
		args := namedProjectionArgs(pool.lastArgs)
		Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
		Expect(args).To(HaveKeyWithValue("active_status", testCatalogStatusActive))
	})

	It("returns an empty list when the actor is missing", func() {
		tools, err := repository.ListAvailableTools(ctx, uuid.Nil, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(BeEmpty())
		Expect(pool.queryCalled).To(BeFalse())
	})

	It("resolves active catalog tools granted to the tenant", func() {
		orgID := uuid.New()
		userID := uuid.New()
		projected := validProjectedTool()
		pool.row = projectedToolRow{tool: projected}

		tool, err := repository.ResolveTool(ctx, orgID, userID, projected.Name)

		Expect(err).NotTo(HaveOccurred())
		Expect(tool).To(Equal(projected))
		Expect(pool.queryRowCalled).To(BeTrue())
		Expect(pool.lastSQL).To(ContainSubstring("lower(c.tool_name) = lower(@tool_name)"))
		Expect(namedProjectionArgs(pool.lastArgs)).To(HaveKeyWithValue("tool_name", projected.Name))
	})

	It("fails closed when resolving without an actor", func() {
		tool, err := repository.ResolveTool(ctx, uuid.Nil, uuid.New(), "crm_lookup")

		Expect(tool).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolDenied)).To(BeTrue())
		Expect(pool.queryRowCalled).To(BeFalse())
	})

	It("returns tool-not-found for unavailable catalog tools", func() {
		pool.row = projectionErrorRow{err: pgx.ErrNoRows}

		tool, err := repository.ResolveTool(ctx, uuid.New(), uuid.New(), "crm_lookup")

		Expect(tool).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolNotFound)).To(BeTrue())
	})

	It("wraps projection persistence errors", func() {
		pool.execErr = errors.New("database unavailable")

		err := repository.ApplyCapabilityProjection(ctx, validCapabilityProjection())

		Expect(err).To(MatchError(ContainSubstring("apply tool capability projection")))
		Expect(err).To(MatchError(ContainSubstring("database unavailable")))
	})
})

type toolCatalogProjectionPoolStub struct {
	execCalled     bool
	queryCalled    bool
	queryRowCalled bool
	lastSQL        string
	lastArgs       []any
	rowsAffected   int64
	execErr        error
	queryErr       error
	row            pgx.Row
	rows           pgx.Rows
}

func (p *toolCatalogProjectionPoolStub) Close() {}

func (p *toolCatalogProjectionPoolStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.queryRowCalled = true
	p.lastSQL = compactProjectionSQL(sql)
	p.lastArgs = args
	if p.row != nil {
		return p.row
	}
	return projectionErrorRow{err: pgx.ErrNoRows}
}

func (p *toolCatalogProjectionPoolStub) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.queryCalled = true
	p.lastSQL = compactProjectionSQL(sql)
	p.lastArgs = args
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	if p.rows != nil {
		return p.rows, nil
	}
	return &projectedToolRowsStub{}, nil
}

func (p *toolCatalogProjectionPoolStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execCalled = true
	p.lastSQL = compactProjectionSQL(sql)
	p.lastArgs = args
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.rowsAffected)), p.execErr
}

func (p *toolCatalogProjectionPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, nil
}

type projectionErrorRow struct {
	err error
}

func (r projectionErrorRow) Scan(...any) error {
	return r.err
}

type projectedToolRowsStub struct {
	rows   []pgx.Row
	index  int
	err    error
	closed bool
}

func (r *projectedToolRowsStub) Close() {
	r.closed = true
}

func (r *projectedToolRowsStub) Err() error {
	return r.err
}

func (r *projectedToolRowsStub) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT")
}

func (r *projectedToolRowsStub) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *projectedToolRowsStub) Next() bool {
	if r.index >= len(r.rows) {
		r.Close()
		return false
	}
	r.index++
	return true
}

func (r *projectedToolRowsStub) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.rows) {
		return pgx.ErrNoRows
	}
	return r.rows[r.index-1].Scan(dest...)
}

func (r *projectedToolRowsStub) Values() ([]any, error) {
	return nil, nil
}

func (r *projectedToolRowsStub) RawValues() [][]byte {
	return nil
}

func (r *projectedToolRowsStub) Conn() *pgx.Conn {
	return nil
}

type projectedToolRow struct {
	tool *model.ToolDefinition
}

func (r projectedToolRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = r.tool.CapabilityVersionID
	*(dest[1].(*string)) = r.tool.CapabilityID
	*(dest[2].(*string)) = r.tool.CapabilityVersion
	*(dest[3].(*string)) = r.tool.Name
	*(dest[4].(*string)) = r.tool.ExecutorKind.String()
	*(dest[5].(*string)) = r.tool.Description
	*(dest[6].(*string)) = r.tool.MCPServerEndpoint
	*(dest[7].(*string)) = string(r.tool.ParametersJSON)
	*(dest[8].(*string)) = r.tool.ImplementationVersion
	*(dest[9].(*[]string)) = append([]string(nil), r.tool.EgressHosts...)
	*(dest[10].(*int64)) = r.tool.TimeoutMs
	*(dest[11].(*int64)) = r.tool.MaxResponseBytes
	*(dest[12].(*string)) = r.tool.CredentialName
	*(dest[13].(*string)) = r.tool.CredentialRef
	return nil
}

func validCapabilityProjection() model.ToolCapabilityProjection {
	return model.ToolCapabilityProjection{
		CapabilityVersionID:   uuid.New(),
		CapabilityID:          "partner.crm.lookup",
		Version:               "2026-07-18",
		ToolName:              "crm_lookup",
		ExecutorKind:          model.ToolExecutorKindMCP,
		MCPServerEndpoint:     "https://mcp.partner.example/rpc",
		Description:           "Looks up a customer.",
		ParametersJSON:        []byte(`{"type":"object"}`),
		ImplementationVersion: "mcp:sha256:test",
		EgressHosts:           []string{"mcp.partner.example"},
		TimeoutMs:             1500,
		MaxResponseBytes:      65536,
		CredentialName:        "partner-crm-token",
		CredentialRequired:    true,
		LifecycleStatus:       testCatalogStatusActive,
		ContentHash:           "sha256:test",
	}
}

func validProjectedTool() *model.ToolDefinition {
	projection := validCapabilityProjection()
	return &model.ToolDefinition{
		CapabilityVersionID:   projection.CapabilityVersionID,
		CapabilityID:          projection.CapabilityID,
		CapabilityVersion:     projection.Version,
		Name:                  projection.ToolName,
		Description:           projection.Description,
		MCPServerEndpoint:     projection.MCPServerEndpoint,
		ParametersJSON:        projection.ParametersJSON,
		ImplementationVersion: projection.ImplementationVersion,
		ExecutorKind:          projection.ExecutorKind,
		EgressHosts:           projection.EgressHosts,
		CredentialName:        projection.CredentialName,
		CredentialRef:         "secrets/partner/crm",
		TimeoutMs:             projection.TimeoutMs,
		MaxResponseBytes:      projection.MaxResponseBytes,
		Enabled:               true,
	}
}

func namedProjectionArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func compactProjectionSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

const testCatalogStatusActive = "ACTIVE"
