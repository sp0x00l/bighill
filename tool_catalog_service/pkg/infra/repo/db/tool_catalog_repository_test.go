package db_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	coreDB "lib/shared_lib/db"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"
	toolcatalogdb "tool_catalog_service/pkg/infra/repo/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolCatalogRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog repository unit test suite")
}

var _ = Describe("ToolCatalogRepository", func() {
	var (
		ctx        context.Context
		pool       *toolCatalogPoolStub
		tx         *toolCatalogTxStub
		repository *toolcatalogdb.ToolCatalogRepository
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &toolCatalogPoolStub{}
		tx = &toolCatalogTxStub{}
		repository = toolcatalogdb.NewToolCatalogRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	It("upserts capability versions and returns the stored projection", func() {
		capability := validToolCapabilityVersion()
		tx.row = capabilityRowFromModel(capability)

		saved, err := repository.UpsertCapabilityVersion(ctx, tx, capability)

		Expect(err).NotTo(HaveOccurred())
		Expect(saved).To(Equal(capability))
		Expect(tx.queryRowCalled).To(BeTrue())
		Expect(tx.lastSQL).To(ContainSubstring("INSERT INTO test_db.tool_capability_versions"))
		Expect(tx.lastSQL).To(ContainSubstring("ON CONFLICT (capability_id, version) DO UPDATE"))
		args := namedToolCatalogArgs(tx.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("capability_version_id", pgtype.UUID{Bytes: capability.CapabilityVersionID, Valid: true}),
			HaveKeyWithValue("capability_id", capability.CapabilityID),
			HaveKeyWithValue("version", capability.Version),
			HaveKeyWithValue("tool_name", capability.ToolName),
			HaveKeyWithValue("kind", capability.Kind.String()),
			HaveKeyWithValue("mcp_server_endpoint", capability.MCPServerEndpoint),
			HaveKeyWithValue("parameters_json", string(capability.ParametersJSON)),
			HaveKeyWithValue("implementation_version", capability.ImplementationVersion),
			HaveKeyWithValue("egress_hosts", capability.EgressHosts),
			HaveKeyWithValue("credential_required", capability.CredentialRequired),
			HaveKeyWithValue("content_hash", capability.ContentHash),
			HaveKeyWithValue("published_by_user_id", pgtype.UUID{Bytes: capability.PublishedByUserID, Valid: true}),
		))
	})

	It("reads capability versions by id", func() {
		capability := validToolCapabilityVersion()
		pool.row = capabilityRowFromModel(capability)

		saved, err := repository.ReadCapabilityVersion(ctx, capability.CapabilityVersionID)

		Expect(err).NotTo(HaveOccurred())
		Expect(saved).To(Equal(capability))
		Expect(pool.queryRowCalled).To(BeTrue())
		Expect(pool.lastSQL).To(ContainSubstring("WHERE capability_version_id = @capability_version_id"))
		Expect(namedToolCatalogArgs(pool.lastArgs)).To(HaveKeyWithValue("capability_version_id", pgtype.UUID{Bytes: capability.CapabilityVersionID, Valid: true}))
	})

	It("returns a capability-not-found error when reading a missing capability version", func() {
		pool.row = toolCatalogErrorRow{err: pgx.ErrNoRows}

		capability, err := repository.ReadCapabilityVersion(ctx, uuid.New())

		Expect(capability).To(BeNil())
		Expect(errors.Is(err, domain.ErrCapabilityNotFound)).To(BeTrue())
	})

	It("reads the latest capability by capability id", func() {
		capability := validToolCapabilityVersion()
		pool.row = capabilityRowFromModel(capability)

		saved, err := repository.ReadCapabilityByCapabilityID(ctx, capability.CapabilityID)

		Expect(err).NotTo(HaveOccurred())
		Expect(saved).To(Equal(capability))
		Expect(pool.lastSQL).To(ContainSubstring("WHERE capability_id = @capability_id"))
		Expect(pool.lastSQL).To(ContainSubstring("ORDER BY published_at DESC LIMIT 1"))
		Expect(namedToolCatalogArgs(pool.lastArgs)).To(HaveKeyWithValue("capability_id", capability.CapabilityID))
	})

	It("upserts tenant grants with tenant-scoped arguments", func() {
		grant := validTenantCapabilityGrant()
		tx.row = grantRowFromModel(grant)

		saved, err := repository.UpsertTenantGrant(ctx, tx, grant)

		Expect(err).NotTo(HaveOccurred())
		Expect(saved).To(Equal(grant))
		Expect(tx.lastSQL).To(ContainSubstring("INSERT INTO test_db.tenant_capability_grants"))
		Expect(tx.lastSQL).To(ContainSubstring("ON CONFLICT (org_id, capability_version_id) DO UPDATE"))
		args := namedToolCatalogArgs(tx.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("grant_id", pgtype.UUID{Bytes: grant.GrantID, Valid: true}),
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: grant.OrgID, Valid: true}),
			HaveKeyWithValue("capability_version_id", pgtype.UUID{Bytes: grant.CapabilityVersionID, Valid: true}),
			HaveKeyWithValue("status", grant.Status.String()),
			HaveKeyWithValue("granted_by_user_id", pgtype.UUID{Bytes: grant.GrantedByUserID, Valid: true}),
		))
	})

	It("upserts credential bindings without storing raw credential values", func() {
		binding := validToolCredentialBinding()
		tx.row = credentialBindingRowFromModel(binding)

		saved, err := repository.UpsertCredentialBinding(ctx, tx, binding)

		Expect(err).NotTo(HaveOccurred())
		Expect(saved).To(Equal(binding))
		Expect(tx.lastSQL).To(ContainSubstring("INSERT INTO test_db.tool_credential_bindings"))
		Expect(tx.lastSQL).To(ContainSubstring("ON CONFLICT (org_id, capability_id) DO UPDATE"))
		args := namedToolCatalogArgs(tx.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("binding_id", pgtype.UUID{Bytes: binding.BindingID, Valid: true}),
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: binding.OrgID, Valid: true}),
			HaveKeyWithValue("capability_id", binding.CapabilityID),
			HaveKeyWithValue("credential_ref", binding.CredentialRef),
			HaveKeyWithValue("bound_by_user_id", pgtype.UUID{Bytes: binding.BoundByUserID, Valid: true}),
		))
	})

	It("wraps capability persistence errors", func() {
		capability := validToolCapabilityVersion()
		tx.row = toolCatalogErrorRow{err: errors.New("scan failed")}

		saved, err := repository.UpsertCapabilityVersion(ctx, tx, capability)

		Expect(saved).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("record tool capability version")))
		Expect(err).To(MatchError(ContainSubstring("scan failed")))
	})
})

type toolCatalogPoolStub struct {
	queryRowCalled bool
	lastSQL        string
	lastArgs       []any
	row            pgx.Row
	err            error
}

func (p *toolCatalogPoolStub) Close() {}

func (p *toolCatalogPoolStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.queryRowCalled = true
	p.lastSQL = compactToolCatalogSQL(sql)
	p.lastArgs = args
	if p.row != nil {
		return p.row
	}
	return toolCatalogErrorRow{err: pgx.ErrNoRows}
}

func (p *toolCatalogPoolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (p *toolCatalogPoolStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), p.err
}

func (p *toolCatalogPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, nil
}

type toolCatalogTxStub struct {
	queryRowCalled bool
	lastSQL        string
	lastArgs       []any
	row            pgx.Row
}

func (tx *toolCatalogTxStub) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *toolCatalogTxStub) Commit(context.Context) error {
	return nil
}

func (tx *toolCatalogTxStub) Rollback(context.Context) error {
	return nil
}

func (tx *toolCatalogTxStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *toolCatalogTxStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *toolCatalogTxStub) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *toolCatalogTxStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *toolCatalogTxStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *toolCatalogTxStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (tx *toolCatalogTxStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.queryRowCalled = true
	tx.lastSQL = compactToolCatalogSQL(sql)
	tx.lastArgs = args
	if tx.row != nil {
		return tx.row
	}
	return toolCatalogErrorRow{err: pgx.ErrNoRows}
}

func (tx *toolCatalogTxStub) Conn() *pgx.Conn {
	return nil
}

type toolCatalogErrorRow struct {
	err error
}

func (r toolCatalogErrorRow) Scan(...any) error {
	return r.err
}

type capabilityRepoRow struct {
	capability *model.ToolCapabilityVersion
}

func capabilityRowFromModel(capability *model.ToolCapabilityVersion) capabilityRepoRow {
	return capabilityRepoRow{capability: capability}
}

func (r capabilityRepoRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = r.capability.CapabilityVersionID
	*(dest[1].(*string)) = r.capability.CapabilityID
	*(dest[2].(*string)) = r.capability.Version
	*(dest[3].(*string)) = r.capability.ToolName
	*(dest[4].(*string)) = r.capability.Kind.String()
	*(dest[5].(*string)) = r.capability.MCPServerEndpoint
	*(dest[6].(*string)) = r.capability.Description
	*(dest[7].(*string)) = string(r.capability.ParametersJSON)
	*(dest[8].(*string)) = r.capability.ImplementationVersion
	*(dest[9].(*[]string)) = append([]string(nil), r.capability.EgressHosts...)
	*(dest[10].(*int64)) = r.capability.TimeoutMs
	*(dest[11].(*int64)) = r.capability.MaxResponseBytes
	*(dest[12].(*string)) = r.capability.CredentialName
	*(dest[13].(*bool)) = r.capability.CredentialRequired
	*(dest[14].(*string)) = r.capability.LifecycleStatus.String()
	*(dest[15].(*string)) = r.capability.ContentHash
	*(dest[16].(*uuid.UUID)) = r.capability.PublishedByUserID
	*(dest[17].(*time.Time)) = r.capability.PublishedAt
	return nil
}

type grantRepoRow struct {
	grant *model.TenantCapabilityGrant
}

func grantRowFromModel(grant *model.TenantCapabilityGrant) grantRepoRow {
	return grantRepoRow{grant: grant}
}

func (r grantRepoRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = r.grant.GrantID
	*(dest[1].(*uuid.UUID)) = r.grant.OrgID
	*(dest[2].(*uuid.UUID)) = r.grant.CapabilityVersionID
	*(dest[3].(*string)) = r.grant.Status.String()
	*(dest[4].(*uuid.UUID)) = r.grant.GrantedByUserID
	*(dest[5].(*time.Time)) = r.grant.GrantedAt
	return nil
}

type credentialBindingRepoRow struct {
	binding *model.ToolCredentialBinding
}

func credentialBindingRowFromModel(binding *model.ToolCredentialBinding) credentialBindingRepoRow {
	return credentialBindingRepoRow{binding: binding}
}

func (r credentialBindingRepoRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = r.binding.BindingID
	*(dest[1].(*uuid.UUID)) = r.binding.OrgID
	*(dest[2].(*string)) = r.binding.CapabilityID
	*(dest[3].(*string)) = r.binding.CredentialRef
	*(dest[4].(*uuid.UUID)) = r.binding.BoundByUserID
	*(dest[5].(*time.Time)) = r.binding.BoundAt
	return nil
}

func validToolCapabilityVersion() *model.ToolCapabilityVersion {
	return &model.ToolCapabilityVersion{
		CapabilityVersionID:   uuid.New(),
		CapabilityID:          "partner.crm.lookup",
		Version:               "2026-07-18",
		ToolName:              "crm_lookup",
		Kind:                  model.CapabilityKindMCP,
		MCPServerEndpoint:     "https://mcp.partner.example/rpc",
		Description:           "Looks up a customer.",
		ParametersJSON:        []byte(`{"type":"object"}`),
		ImplementationVersion: "mcp:sha256:test",
		EgressHosts:           []string{"mcp.partner.example"},
		TimeoutMs:             1500,
		MaxResponseBytes:      65536,
		CredentialName:        "partner-crm-token",
		CredentialRequired:    true,
		LifecycleStatus:       model.CapabilityLifecycleStatusActive,
		ContentHash:           "sha256:test",
		PublishedByUserID:     uuid.New(),
		PublishedAt:           time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC),
	}
}

func validTenantCapabilityGrant() *model.TenantCapabilityGrant {
	return &model.TenantCapabilityGrant{
		GrantID:             uuid.New(),
		OrgID:               uuid.New(),
		CapabilityVersionID: uuid.New(),
		Status:              model.TenantGrantStatusActive,
		GrantedByUserID:     uuid.New(),
		GrantedAt:           time.Date(2026, 7, 18, 10, 5, 0, 0, time.UTC),
	}
}

func validToolCredentialBinding() *model.ToolCredentialBinding {
	return &model.ToolCredentialBinding{
		BindingID:     uuid.New(),
		OrgID:         uuid.New(),
		CapabilityID:  "partner.crm.lookup",
		CredentialRef: "secrets/partner/crm",
		BoundByUserID: uuid.New(),
		BoundAt:       time.Date(2026, 7, 18, 10, 10, 0, 0, time.UTC),
	}
}

func namedToolCatalogArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func compactToolCatalogSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
