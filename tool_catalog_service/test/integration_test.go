package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	toolcatalogpb "lib/data_contracts_lib/tool_catalog"
	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"
	"lib/shared_lib/serializer"
	shareduow "lib/shared_lib/uow"
	"tool_catalog_service/pkg/app"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"
	toolcatalogmessaging "tool_catalog_service/pkg/infra/network/messaging"
	toolcatalogdb "tool_catalog_service/pkg/infra/repo/db"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestToolCatalogIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog integration test suite")
}

var _ = Describe("Tool catalog integration", Ordered, func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		database   *coreDB.Database
		usecase    app.ToolCatalogUsecase
		newUsecase func(app.CapabilityManifestVerifier) app.ToolCatalogUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)
		dbConfig := coreDB.DatabaseConfig{}
		dbConfig.WithDbName("TOOL_CATALOG_SERVICE_DB_NAME", "bighill_tool_catalog_db")
		dbConfig.WithDbUser("TOOL_CATALOG_SERVICE_DB_USER", "bighill_tool_catalog_db_user")
		dbConfig.WithDbPassword("TOOL_CATALOG_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
		dbConfig.WithDbMaxConnections("TOOL_CATALOG_SERVICE_DB_MAX_CONNECTIONS", "20")
		dbConfig.WithDbHost("PGHOST", "127.0.0.1")
		dbConfig.WithDbPort("PGPORT", "5432")
		dbConfig.WithDbSSLMode("PGSSLMODE", "disable")
		var err error
		database, err = coreDB.InitDatabase(ctx, dbConfig.GetName(), dbConfig.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		repository := toolcatalogdb.NewToolCatalogRepository(database)
		unitOfWork := shareduow.New(database.Pool, shareduow.WithTransactionalOutbox(orderedOutbox))
		eventBuilder := toolcatalogmessaging.NewToolCatalogEventBuilder("tool_catalog")
		newUsecase = func(verifier app.CapabilityManifestVerifier) app.ToolCatalogUsecase {
			return app.NewToolCatalogUsecase(
				repository,
				unitOfWork,
				eventBuilder,
				serializer.NewJSONSerializer(),
				app.WithCapabilityManifestVerifier(verifier),
			)
		}
		usecase = newUsecase(&manifestVerifierStub{})
	})

	BeforeEach(func() {
		Expect(truncateToolCatalog(ctx, database)).To(Succeed())
		usecase = newUsecase(&manifestVerifierStub{})
	})

	AfterAll(func() {
		if database != nil {
			database.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("publishes a capability, grants a tenant, binds a credential ref, and emits projection events", func() {
		userID := uuid.New()
		orgID := uuid.New()
		tenantCtx := ctxutil.WithActorOrg(ctx, userID, orgID)

		capability, err := usecase.PublishCapability(ctx, model.PublishCapabilityCommand{
			UserID:             userID,
			CapabilityID:       "partner.crm.lookup",
			Version:            "2026-07-18",
			ToolName:           "crm_lookup",
			Kind:               model.CapabilityKindMCP,
			MCPServerEndpoint:  "https://mcp.partner.example/rpc",
			Description:        "Looks up a customer.",
			ParametersJSON:     []byte(`{"type":"object","additionalProperties":false}`),
			EgressHosts:        []string{"mcp.partner.example"},
			TimeoutMs:          1500,
			MaxResponseBytes:   65536,
			CredentialName:     "partner-crm-token",
			CredentialRequired: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(capability.ImplementationVersion).To(ContainSubstring("mcp:mcp.partner.example:"))

		var storedToolName string
		var storedEndpoint string
		var storedParameters string
		var storedImplementationVersion string
		var storedContentHash string
		Expect(database.Pool.QueryRow(ctx, `
			SELECT tool_name, mcp_server_endpoint, parameters_json::text, implementation_version, content_hash
			FROM `+database.Name+`.tool_capability_versions
			WHERE capability_version_id = $1
		`, capability.CapabilityVersionID).Scan(&storedToolName, &storedEndpoint, &storedParameters, &storedImplementationVersion, &storedContentHash)).To(Succeed())
		Expect(storedToolName).To(Equal("crm_lookup"))
		Expect(storedEndpoint).To(Equal("https://mcp.partner.example/rpc"))
		Expect(storedParameters).To(MatchJSON(`{"additionalProperties":false,"type":"object"}`))
		Expect(storedImplementationVersion).To(Equal(capability.ImplementationVersion))
		Expect(storedContentHash).To(Equal(capability.ContentHash))

		grant, err := usecase.GrantCapability(ctx, model.GrantCapabilityCommand{
			UserID:              userID,
			OrgID:               orgID,
			CapabilityVersionID: capability.CapabilityVersionID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(grant.Status).To(Equal(model.TenantGrantStatusActive))
		var storedGrantStatus string
		Expect(database.Pool.QueryRow(tenantCtx, `
			SELECT status::text
			FROM `+database.Name+`.tenant_capability_grants
			WHERE org_id = $1 AND capability_version_id = $2
		`, orgID, capability.CapabilityVersionID).Scan(&storedGrantStatus)).To(Succeed())
		Expect(storedGrantStatus).To(Equal("ACTIVE"))

		binding, err := usecase.BindCredential(ctx, model.BindCredentialCommand{
			UserID:        userID,
			OrgID:         orgID,
			CapabilityID:  capability.CapabilityID,
			CredentialRef: "secrets/partner/crm",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(binding.CredentialRef).To(Equal("secrets/partner/crm"))
		var storedCredentialRef string
		Expect(database.Pool.QueryRow(tenantCtx, `
			SELECT credential_ref
			FROM `+database.Name+`.tool_credential_bindings
			WHERE org_id = $1 AND capability_id = $2
		`, orgID, capability.CapabilityID).Scan(&storedCredentialRef)).To(Succeed())
		Expect(storedCredentialRef).To(Equal("secrets/partner/crm"))

		var rawCapability []byte
		Expect(database.Pool.QueryRow(ctx, `
			SELECT payload
			FROM `+database.Name+`.outbox_messages
			WHERE resource_key = $1 AND event_type = $2
		`, capability.CapabilityVersionID, sharedmessaging.MsgTypeToolCapabilityUpdated.String()).Scan(&rawCapability)).To(Succeed())
		var capabilityEnvelope sharedmessaging.Message
		Expect(capabilityEnvelope.Deserialize(ctx, rawCapability)).To(Succeed())
		capabilityPayload := &toolcatalogpb.ToolCapabilityUpdatedEvent{}
		Expect(capabilityEnvelope.DeserializePayload(capabilityPayload)).To(Succeed())
		Expect(capabilityPayload.GetToolName()).To(Equal("crm_lookup"))
		Expect(capabilityPayload.GetMcpServerEndpoint()).To(Equal("https://mcp.partner.example/rpc"))
		Expect(capabilityPayload.GetParametersJson()).To(MatchJSON(`{"additionalProperties":false,"type":"object"}`))
		Expect(capabilityPayload.GetImplementationVersion()).To(Equal(capability.ImplementationVersion))

		var rawGrant []byte
		Expect(database.Pool.QueryRow(ctx, `
			SELECT payload
			FROM `+database.Name+`.outbox_messages
			WHERE resource_key = $1 AND event_type = $2
		`, orgID, sharedmessaging.MsgTypeToolGrantUpdated.String()).Scan(&rawGrant)).To(Succeed())
		var grantEnvelope sharedmessaging.Message
		Expect(grantEnvelope.Deserialize(ctx, rawGrant)).To(Succeed())
		grantPayload := &toolcatalogpb.ToolGrantUpdatedEvent{}
		Expect(grantEnvelope.DeserializePayload(grantPayload)).To(Succeed())
		Expect(grantPayload.GetCapabilityVersionId()).To(Equal(capability.CapabilityVersionID.String()))
		Expect(grantPayload.GetStatus()).To(Equal("ACTIVE"))

		var rawBinding []byte
		Expect(database.Pool.QueryRow(ctx, `
			SELECT payload
			FROM `+database.Name+`.outbox_messages
			WHERE resource_key = $1 AND event_type = $2
		`, orgID, sharedmessaging.MsgTypeToolCredentialBindingUpdated.String()).Scan(&rawBinding)).To(Succeed())
		var bindingEnvelope sharedmessaging.Message
		Expect(bindingEnvelope.Deserialize(ctx, rawBinding)).To(Succeed())
		bindingPayload := &toolcatalogpb.ToolCredentialBindingUpdatedEvent{}
		Expect(bindingEnvelope.DeserializePayload(bindingPayload)).To(Succeed())
		Expect(bindingPayload.GetCapabilityId()).To(Equal(capability.CapabilityID))
		Expect(bindingPayload.GetCredentialRef()).To(Equal("secrets/partner/crm"))
		Expect(string(rawBinding)).NotTo(ContainSubstring("bearer-token-value"))
	})

	It("does not persist or emit an MCP capability when live manifest verification fails", func() {
		verifier := &manifestVerifierStub{err: domain.ErrToolCatalogValidation.Extend("schema mismatch")}
		usecase = newUsecase(verifier)

		capability, err := usecase.PublishCapability(ctx, model.PublishCapabilityCommand{
			UserID:             uuid.New(),
			CapabilityID:       "partner.crm.lookup",
			Version:            "2026-07-18",
			ToolName:           "crm_lookup",
			Kind:               model.CapabilityKindMCP,
			MCPServerEndpoint:  "https://mcp.partner.example/rpc",
			Description:        "Looks up a customer.",
			ParametersJSON:     []byte(`{"type":"object","additionalProperties":false}`),
			EgressHosts:        []string{"mcp.partner.example"},
			TimeoutMs:          1500,
			MaxResponseBytes:   65536,
			CredentialName:     "partner-crm-token",
			CredentialRequired: true,
		})

		Expect(capability).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolCatalogValidation)).To(BeTrue())
		Expect(verifier.commands).To(HaveLen(1))
		var capabilityCount int
		Expect(database.Pool.QueryRow(ctx, `
			SELECT count(*)
			FROM `+database.Name+`.tool_capability_versions
		`).Scan(&capabilityCount)).To(Succeed())
		Expect(capabilityCount).To(Equal(0))
		var eventCount int
		Expect(database.Pool.QueryRow(ctx, `
			SELECT count(*)
			FROM `+database.Name+`.outbox_messages
		`).Scan(&eventCount)).To(Succeed())
		Expect(eventCount).To(Equal(0))
	})

	It("fails closed for grants and credential bindings when the capability does not exist", func() {
		userID := uuid.New()
		orgID := uuid.New()

		grant, err := usecase.GrantCapability(ctx, model.GrantCapabilityCommand{
			UserID:              userID,
			OrgID:               orgID,
			CapabilityVersionID: uuid.New(),
		})
		Expect(grant).To(BeNil())
		Expect(errors.Is(err, domain.ErrCapabilityNotFound)).To(BeTrue())

		binding, err := usecase.BindCredential(ctx, model.BindCredentialCommand{
			UserID:        userID,
			OrgID:         orgID,
			CapabilityID:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		})
		Expect(binding).To(BeNil())
		Expect(errors.Is(err, domain.ErrCapabilityNotFound)).To(BeTrue())

		var eventCount int
		Expect(database.Pool.QueryRow(ctx, `
			SELECT count(*)
			FROM `+database.Name+`.outbox_messages
		`).Scan(&eventCount)).To(Succeed())
		Expect(eventCount).To(Equal(0))
	})
})

func truncateToolCatalog(ctx context.Context, database *coreDB.Database) error {
	ctx = ctxutil.WithSystemContext(ctx)
	for _, table := range []string{"outbox_messages", "tool_credential_bindings", "tenant_capability_grants", "tool_capability_versions"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

type manifestVerifierStub struct {
	err      error
	commands []model.PublishCapabilityCommand
}

func (s *manifestVerifierStub) VerifyCapabilityManifest(_ context.Context, command model.PublishCapabilityCommand) error {
	s.commands = append(s.commands, command)
	return s.err
}
