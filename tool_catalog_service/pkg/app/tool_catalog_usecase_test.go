package app_test

import (
	"context"
	"errors"
	"testing"

	"lib/shared_lib/ctxutil"
	"lib/shared_lib/serializer"
	shareduow "lib/shared_lib/uow"
	"tool_catalog_service/pkg/app"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolCatalogUsecase(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog app unit test suite")
}

var _ = Describe("ToolCatalogUsecase", func() {
	var (
		ctx        context.Context
		repo       *catalogRepositoryStub
		uow        *catalogUnitOfWorkStub
		events     *catalogEventBuilderStub
		usecase    app.ToolCatalogUsecase
		userID     uuid.UUID
		orgID      uuid.UUID
		capability *model.ToolCapabilityVersion
	)

	BeforeEach(func() {
		ctx = context.Background()
		userID = uuid.New()
		orgID = uuid.New()
		capability = validCapability()
		repo = &catalogRepositoryStub{capability: capability}
		uow = &catalogUnitOfWorkStub{}
		events = &catalogEventBuilderStub{}
		usecase = app.NewToolCatalogUsecase(repo, uow, events, serializer.NewJSONSerializer(), app.WithCapabilityManifestVerifier(&manifestVerifierStub{}))
	})

	It("publishes a content-addressed capability and enqueues its projection event", func() {
		record, err := usecase.PublishCapability(ctx, validPublishCommand(userID))

		Expect(err).NotTo(HaveOccurred())
		Expect(record).NotTo(BeNil())
		Expect(repo.upsertedCapability).NotTo(BeNil())
		Expect(repo.upsertedCapability.ContentHash).To(HavePrefix("sha256:"))
		Expect(repo.upsertedCapability.ImplementationVersion).To(HavePrefix("mcp:mcp.partner.example:"))
		Expect(repo.upsertedCapability.ParametersJSON).To(MatchJSON(`{"type":"object","additionalProperties":false}`))
		Expect(events.capabilityEvent).To(Equal(record.CapabilityVersionID))
		Expect(uow.enqueued).To(HaveLen(1))
		Expect(ctxutil.IsSystemContext(uow.lastCtx)).To(BeTrue())
	})

	It("rejects MCP publishing when the live manifest verifier fails", func() {
		verifier := &manifestVerifierStub{err: domain.ErrToolCatalogValidation.Extend("schema mismatch")}
		usecase = app.NewToolCatalogUsecase(repo, uow, events, serializer.NewJSONSerializer(), app.WithCapabilityManifestVerifier(verifier))

		record, err := usecase.PublishCapability(ctx, validPublishCommand(userID))

		Expect(record).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolCatalogValidation)).To(BeTrue())
		Expect(verifier.called).To(BeTrue())
		Expect(repo.upsertedCapability).To(BeNil())
		Expect(uow.enqueued).To(BeEmpty())
	})

	It("rejects grants for unknown capability versions", func() {
		repo.readCapabilityErr = domain.ErrCapabilityNotFound.Extend("missing")

		grant, err := usecase.GrantCapability(ctx, model.GrantCapabilityCommand{
			UserID:              userID,
			OrgID:               orgID,
			CapabilityVersionID: uuid.New(),
		})

		Expect(grant).To(BeNil())
		Expect(errors.Is(err, domain.ErrCapabilityNotFound)).To(BeTrue())
		Expect(repo.upsertedGrant).To(BeNil())
	})

	It("records a tenant grant and enqueues the grant projection event", func() {
		grant, err := usecase.GrantCapability(ctx, model.GrantCapabilityCommand{
			UserID:              userID,
			OrgID:               orgID,
			CapabilityVersionID: capability.CapabilityVersionID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(grant.OrgID).To(Equal(orgID))
		Expect(grant.Status).To(Equal(model.TenantGrantStatusActive))
		Expect(events.grantEvent).To(Equal(grant.GrantID))
		Expect(uow.enqueued).To(HaveLen(1))
		writeOrgID, ok := ctxutil.OrgID(uow.lastCtx)
		Expect(ok).To(BeTrue())
		Expect(writeOrgID).To(Equal(orgID))
	})

	It("rejects grants without tenant, actor, or capability identity", func() {
		grant, err := usecase.GrantCapability(ctx, model.GrantCapabilityCommand{
			UserID:              userID,
			OrgID:               orgID,
			CapabilityVersionID: uuid.Nil,
		})

		Expect(grant).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolCatalogValidation)).To(BeTrue())
		Expect(repo.upsertedGrant).To(BeNil())
	})

	It("rejects credential bindings for unknown capabilities", func() {
		repo.readCapabilityByIDErr = domain.ErrCapabilityNotFound.Extend("missing")

		binding, err := usecase.BindCredential(ctx, model.BindCredentialCommand{
			UserID:        userID,
			OrgID:         orgID,
			CapabilityID:  "partner.crm.lookup",
			CredentialRef: "secret-ref",
		})

		Expect(binding).To(BeNil())
		Expect(errors.Is(err, domain.ErrCapabilityNotFound)).To(BeTrue())
		Expect(repo.upsertedBinding).To(BeNil())
	})

	It("rejects credential bindings without a credential ref", func() {
		binding, err := usecase.BindCredential(ctx, model.BindCredentialCommand{
			UserID:        userID,
			OrgID:         orgID,
			CapabilityID:  capability.CapabilityID,
			CredentialRef: " ",
		})

		Expect(binding).To(BeNil())
		Expect(errors.Is(err, domain.ErrToolCatalogValidation)).To(BeTrue())
		Expect(repo.upsertedBinding).To(BeNil())
	})

	It("records a tenant credential binding and enqueues its projection event", func() {
		binding, err := usecase.BindCredential(ctx, model.BindCredentialCommand{
			UserID:        userID,
			OrgID:         orgID,
			CapabilityID:  capability.CapabilityID,
			CredentialRef: "secrets/partner/crm",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(binding.OrgID).To(Equal(orgID))
		Expect(binding.CapabilityID).To(Equal(capability.CapabilityID))
		Expect(events.credentialEvent).To(Equal(binding.BindingID))
		Expect(uow.enqueued).To(HaveLen(1))
		writeOrgID, ok := ctxutil.OrgID(uow.lastCtx)
		Expect(ok).To(BeTrue())
		Expect(writeOrgID).To(Equal(orgID))
	})
})

type catalogRepositoryStub struct {
	capability            *model.ToolCapabilityVersion
	upsertedCapability    *model.ToolCapabilityVersion
	upsertedGrant         *model.TenantCapabilityGrant
	upsertedBinding       *model.ToolCredentialBinding
	readCapabilityErr     error
	readCapabilityByIDErr error
}

func (s *catalogRepositoryStub) UpsertCapabilityVersion(_ context.Context, _ pgx.Tx, capability *model.ToolCapabilityVersion) (*model.ToolCapabilityVersion, error) {
	s.upsertedCapability = capability
	return capability, nil
}

func (s *catalogRepositoryStub) ReadCapabilityVersion(context.Context, uuid.UUID) (*model.ToolCapabilityVersion, error) {
	return s.capability, s.readCapabilityErr
}

func (s *catalogRepositoryStub) ReadCapabilityByCapabilityID(context.Context, string) (*model.ToolCapabilityVersion, error) {
	return s.capability, s.readCapabilityByIDErr
}

func (s *catalogRepositoryStub) UpsertTenantGrant(_ context.Context, _ pgx.Tx, grant *model.TenantCapabilityGrant) (*model.TenantCapabilityGrant, error) {
	s.upsertedGrant = grant
	return grant, nil
}

func (s *catalogRepositoryStub) UpsertCredentialBinding(_ context.Context, _ pgx.Tx, binding *model.ToolCredentialBinding) (*model.ToolCredentialBinding, error) {
	s.upsertedBinding = binding
	return binding, nil
}

type catalogUnitOfWorkStub struct {
	enqueued []shareduow.OutboundMessage
	lastCtx  context.Context
}

func (s *catalogUnitOfWorkStub) Do(ctx context.Context, fn shareduow.TxFunc) error {
	s.lastCtx = ctx
	return fn(ctx, nil, func(message shareduow.OutboundMessage) error {
		s.enqueued = append(s.enqueued, message)
		return nil
	})
}

type catalogEventBuilderStub struct {
	capabilityEvent uuid.UUID
	grantEvent      uuid.UUID
	credentialEvent uuid.UUID
}

func (s *catalogEventBuilderStub) CapabilityUpdatedMessage(capability *model.ToolCapabilityVersion) shareduow.OutboundMessage {
	s.capabilityEvent = capability.CapabilityVersionID
	return shareduow.OutboundMessage{}
}

func (s *catalogEventBuilderStub) GrantUpdatedMessage(grant *model.TenantCapabilityGrant) shareduow.OutboundMessage {
	s.grantEvent = grant.GrantID
	return shareduow.OutboundMessage{}
}

func (s *catalogEventBuilderStub) CredentialBindingUpdatedMessage(binding *model.ToolCredentialBinding) shareduow.OutboundMessage {
	s.credentialEvent = binding.BindingID
	return shareduow.OutboundMessage{}
}

type manifestVerifierStub struct {
	called  bool
	command model.PublishCapabilityCommand
	err     error
}

func (s *manifestVerifierStub) VerifyCapabilityManifest(_ context.Context, command model.PublishCapabilityCommand) error {
	s.called = true
	s.command = command
	return s.err
}

func validPublishCommand(userID uuid.UUID) model.PublishCapabilityCommand {
	return model.PublishCapabilityCommand{
		UserID:             userID,
		CapabilityID:       "partner.crm.lookup",
		Version:            "2026-07-18",
		ToolName:           "crm_lookup",
		Kind:               model.CapabilityKindMCP,
		MCPServerEndpoint:  "https://mcp.partner.example/rpc",
		Description:        "Looks up a customer in the partner CRM.",
		ParametersJSON:     []byte(`{"type":"object","additionalProperties":false}`),
		EgressHosts:        []string{"mcp.partner.example"},
		TimeoutMs:          1500,
		MaxResponseBytes:   65536,
		CredentialName:     "partner-crm-token",
		CredentialRequired: true,
	}
}

func validCapability() *model.ToolCapabilityVersion {
	command := validPublishCommand(uuid.New())
	return &model.ToolCapabilityVersion{
		CapabilityVersionID:   uuid.New(),
		CapabilityID:          command.CapabilityID,
		Version:               command.Version,
		ToolName:              command.ToolName,
		Kind:                  command.Kind,
		MCPServerEndpoint:     command.MCPServerEndpoint,
		Description:           command.Description,
		ParametersJSON:        command.ParametersJSON,
		ImplementationVersion: "mcp:sha256:test",
		EgressHosts:           command.EgressHosts,
		TimeoutMs:             command.TimeoutMs,
		MaxResponseBytes:      command.MaxResponseBytes,
		CredentialName:        command.CredentialName,
		CredentialRequired:    command.CredentialRequired,
		LifecycleStatus:       model.CapabilityLifecycleStatusActive,
		ContentHash:           "sha256:test",
		PublishedByUserID:     command.UserID,
	}
}
