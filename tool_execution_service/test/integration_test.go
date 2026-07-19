package integration_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	toolcatalogpb "lib/data_contracts_lib/tool_catalog"
	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	"tool_execution_service/pkg/app"
	"tool_execution_service/pkg/domain/model"
	toolexecutor "tool_execution_service/pkg/infra/executor"
	toolmessaging "tool_execution_service/pkg/infra/network/messaging"
	toolpolicy "tool_execution_service/pkg/infra/policy"
	tooldb "tool_execution_service/pkg/infra/repo/db"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestToolExecutionIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool execution integration test suite")
}

var _ = Describe("Tool execution integration", Ordered, func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		database   *coreDB.Database
		projection *tooldb.ToolCatalogProjectionRepository
		projector  app.ToolCatalogProjectionUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)
		dbConfig := coreDB.DatabaseConfig{}
		dbConfig.WithDbName("TOOL_EXECUTION_SERVICE_DB_NAME", "bighill_tool_execution_db")
		dbConfig.WithDbUser("TOOL_EXECUTION_SERVICE_DB_USER", "bighill_tool_execution_db_user")
		dbConfig.WithDbPassword("TOOL_EXECUTION_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
		dbConfig.WithDbMaxConnections("TOOL_EXECUTION_SERVICE_DB_MAX_CONNECTIONS", "20")
		dbConfig.WithDbHost("PGHOST", "127.0.0.1")
		dbConfig.WithDbPort("PGPORT", "5432")
		dbConfig.WithDbSSLMode("PGSSLMODE", "disable")
		var err error
		database, err = coreDB.InitDatabase(ctx, dbConfig.GetName(), dbConfig.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())
		projection = tooldb.NewToolCatalogProjectionRepository(database)
		projector = app.NewToolCatalogProjectionUsecase(projection)
	})

	BeforeEach(func() {
		Expect(truncateToolExecution(ctx, database)).To(Succeed())
	})

	AfterAll(func() {
		if database != nil {
			database.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("projects catalog events into local execution state and resolves tools without a catalog hot-path call", func() {
		orgID := uuid.New()
		userID := uuid.New()
		capabilityVersionID := uuid.New()
		capabilityListener := toolmessaging.NewToolCapabilityUpdatedEventListener(projector)
		grantListener := toolmessaging.NewToolGrantUpdatedEventListener(projector)
		bindingListener := toolmessaging.NewToolCredentialBindingUpdatedEventListener(projector)

		Expect(capabilityListener.Handle(ctx, capabilityVersionID, &toolcatalogpb.ToolCapabilityUpdatedEvent{
			CapabilityVersionId:   capabilityVersionID.String(),
			CapabilityId:          "partner.crm.lookup",
			Version:               "2026-07-18",
			ToolName:              "crm_lookup",
			Kind:                  "MCP",
			McpServerEndpoint:     "https://mcp.partner.example/rpc",
			Description:           "Looks up a customer.",
			ParametersJson:        []byte(`{"type":"object"}`),
			ImplementationVersion: "mcp:sha256:test",
			EgressHosts:           []string{"mcp.partner.example"},
			TimeoutMs:             1500,
			MaxResponseBytes:      65536,
			CredentialName:        "partner-crm-token",
			CredentialRequired:    true,
			LifecycleStatus:       "ACTIVE",
			ContentHash:           "sha256:test",
		})).To(Succeed())
		tools, err := projection.ListAvailableTools(ctx, orgID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(BeEmpty())

		Expect(grantListener.Handle(ctx, orgID, &toolcatalogpb.ToolGrantUpdatedEvent{
			OrgId:               orgID.String(),
			CapabilityVersionId: capabilityVersionID.String(),
			Status:              "ACTIVE",
		})).To(Succeed())
		tools, err = projection.ListAvailableTools(ctx, orgID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(BeEmpty())

		Expect(bindingListener.Handle(ctx, orgID, &toolcatalogpb.ToolCredentialBindingUpdatedEvent{
			OrgId:         orgID.String(),
			CapabilityId:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		})).To(Succeed())
		tools, err = projection.ListAvailableTools(ctx, orgID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(HaveLen(1))
		Expect(tools[0].Name).To(Equal("crm_lookup"))
		Expect(tools[0].CredentialRef).To(Equal("secrets/partner/crm"))

		tool, err := projection.ResolveTool(ctx, orgID, userID, "CRM_LOOKUP")
		Expect(err).NotTo(HaveOccurred())
		Expect(tool.CapabilityVersionID).To(Equal(capabilityVersionID))
		Expect(tool.ExecutorKind).To(Equal(model.ToolExecutorKindMCP))
		Expect(tool.TimeoutMs).To(Equal(int64(1500)))
		Expect(tool.MaxResponseBytes).To(Equal(int64(65536)))

		otherOrgTool, err := projection.ResolveTool(ctx, uuid.New(), userID, "crm_lookup")
		Expect(otherOrgTool).To(BeNil())
		Expect(err).To(HaveOccurred())

		Expect(grantListener.Handle(ctx, orgID, &toolcatalogpb.ToolGrantUpdatedEvent{
			OrgId:               orgID.String(),
			CapabilityVersionId: capabilityVersionID.String(),
			Status:              "REVOKED",
		})).To(Succeed())
		tools, err = projection.ListAvailableTools(ctx, orgID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(BeEmpty())
	})

	It("invokes a projected MCP tool from local execution state and writes durable audit", func() {
		orgID := uuid.New()
		userID := uuid.New()
		invocationID := uuid.New()
		capabilityVersionID := uuid.New()
		capabilityListener := toolmessaging.NewToolCapabilityUpdatedEventListener(projector)
		grantListener := toolmessaging.NewToolGrantUpdatedEventListener(projector)
		bindingListener := toolmessaging.NewToolCredentialBindingUpdatedEventListener(projector)
		Expect(capabilityListener.Handle(ctx, capabilityVersionID, &toolcatalogpb.ToolCapabilityUpdatedEvent{
			CapabilityVersionId:   capabilityVersionID.String(),
			CapabilityId:          "partner.crm.lookup",
			Version:               "2026-07-18",
			ToolName:              "crm_lookup",
			Kind:                  "MCP",
			McpServerEndpoint:     "https://mcp.partner.example/rpc",
			Description:           "Looks up a customer.",
			ParametersJson:        []byte(`{"type":"object"}`),
			ImplementationVersion: "mcp:mcp.partner.example:sha256-test",
			EgressHosts:           []string{"mcp.partner.example"},
			TimeoutMs:             1500,
			MaxResponseBytes:      65536,
			CredentialName:        "partner-crm-token",
			CredentialRequired:    true,
			LifecycleStatus:       "ACTIVE",
			ContentHash:           "sha256:test",
		})).To(Succeed())
		Expect(grantListener.Handle(ctx, orgID, &toolcatalogpb.ToolGrantUpdatedEvent{
			OrgId:               orgID.String(),
			CapabilityVersionId: capabilityVersionID.String(),
			Status:              "ACTIVE",
		})).To(Succeed())
		Expect(bindingListener.Handle(ctx, orgID, &toolcatalogpb.ToolCredentialBindingUpdatedEvent{
			OrgId:         orgID.String(),
			CapabilityId:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		})).To(Succeed())

		var calledHost string
		mcpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calledHost = req.URL.Hostname()
			Expect(req.Header.Get("Authorization")).To(Equal("Bearer token-value"))
			Expect(req.URL.String()).To(Equal("https://mcp.partner.example/rpc"))
			return jsonResponse(http.StatusOK, `{"jsonrpc":"2.0","id":"`+invocationID.String()+`","result":{"content":[{"type":"text","text":"Alice"}]}}`), nil
		})}
		executors := map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindMCP: toolexecutor.NewMCPExecutorWithClient("", mcpClient, &credentialResolverStub{value: "token-value"}),
		}
		usecase := app.NewToolUsecase(
			projection,
			executors,
			app.WithBoundaryPolicyResolver(toolpolicy.NewBoundaryPolicyResolver(toolpolicy.BoundaryPolicyConfig{
				HTTPTimeout:          5 * time.Second,
				HTTPMaxResponseBytes: 65536,
			})),
			app.WithInvocationAuditRepository(tooldb.NewInvocationAuditRepository(database)),
		)

		result, err := usecase.Invoke(ctx, model.InvokeToolCommand{
			InvocationID:  invocationID,
			ToolName:      "crm_lookup",
			ArgumentsJSON: []byte(`{"customer_id":"cust-123"}`),
			OrgID:         orgID,
			UserID:        userID,
			TraceID:       "trace-123",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeFalse())
		Expect(result.ResultJSON).To(MatchJSON(`{"content":[{"type":"text","text":"Alice"}]}`))
		Expect(calledHost).To(Equal("mcp.partner.example"))
		var status string
		var executorKind string
		var egressHost string
		var traceID string
		var argsHash string
		Expect(database.Pool.QueryRow(ctxutil.WithOrgID(ctx, orgID), `
			SELECT status::text, executor_kind::text, egress_host, trace_id, args_hash
			FROM `+database.Name+`.tool_invocation_audit
			WHERE invocation_id = $1
		`, invocationID).Scan(&status, &executorKind, &egressHost, &traceID, &argsHash)).To(Succeed())
		Expect(status).To(Equal("COMPLETED"))
		Expect(executorKind).To(Equal("MCP"))
		Expect(egressHost).To(Equal("mcp.partner.example"))
		Expect(traceID).To(Equal("trace-123"))
		Expect(argsHash).To(HavePrefix("sha256:"))
	})
})

func truncateToolExecution(ctx context.Context, database *coreDB.Database) error {
	ctx = ctxutil.WithSystemContext(ctx)
	for _, table := range []string{"tool_credential_binding_projections", "tool_grant_projections", "tool_capability_projections", "tool_invocation_audit"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

type credentialResolverStub struct {
	value string
}

func (s *credentialResolverStub) ResolveCredential(context.Context, string) (string, error) {
	return s.value, nil
}
