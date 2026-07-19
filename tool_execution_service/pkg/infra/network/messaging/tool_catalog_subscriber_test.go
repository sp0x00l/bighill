package messaging_test

import (
	"context"
	"errors"
	"testing"

	toolcatalogpb "lib/data_contracts_lib/tool_catalog"
	sharedmessaging "lib/shared_lib/messaging"
	"tool_execution_service/pkg/domain/model"
	toolmessaging "tool_execution_service/pkg/infra/network/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolCatalogSubscriber(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog subscriber unit test suite")
}

var _ = Describe("Tool catalog subscriber", func() {
	It("projects capability update events into the local execution catalog", func() {
		usecase := &projectionUsecaseStub{}
		listener := toolmessaging.NewToolCapabilityUpdatedEventListener(usecase)
		capabilityVersionID := uuid.New()

		err := listener.Handle(context.Background(), capabilityVersionID, validCapabilityUpdatedEvent(capabilityVersionID))

		Expect(err).NotTo(HaveOccurred())
		Expect(usecase.capabilityProjection.CapabilityVersionID).To(Equal(capabilityVersionID))
		Expect(usecase.capabilityProjection.CapabilityID).To(Equal("partner.crm.lookup"))
		Expect(usecase.capabilityProjection.ExecutorKind).To(Equal(model.ToolExecutorKindMCP))
		Expect(usecase.capabilityProjection.MCPServerEndpoint).To(Equal("https://mcp.partner.example/rpc"))
		Expect(usecase.capabilityProjection.ParametersJSON).To(MatchJSON(`{"type":"object"}`))
		Expect(usecase.capabilityProjection.CredentialRequired).To(BeTrue())
	})

	It("marks malformed capability events as non-retryable", func() {
		usecase := &projectionUsecaseStub{}
		listener := toolmessaging.NewToolCapabilityUpdatedEventListener(usecase)
		capabilityVersionID := uuid.New()

		err := listener.Handle(context.Background(), capabilityVersionID, &toolcatalogpb.ToolCapabilityUpdatedEvent{
			CapabilityVersionId: capabilityVersionID.String(),
			CapabilityId:        "partner.crm.lookup",
			Kind:                "MCP",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
		Expect(usecase.capabilityCalled).To(BeFalse())
	})

	It("projects grant update events including revocations", func() {
		usecase := &projectionUsecaseStub{}
		listener := toolmessaging.NewToolGrantUpdatedEventListener(usecase)
		orgID := uuid.New()
		capabilityVersionID := uuid.New()

		err := listener.Handle(context.Background(), orgID, &toolcatalogpb.ToolGrantUpdatedEvent{
			OrgId:               orgID.String(),
			CapabilityVersionId: capabilityVersionID.String(),
			Status:              "REVOKED",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(usecase.grantProjection.OrgID).To(Equal(orgID))
		Expect(usecase.grantProjection.CapabilityVersionID).To(Equal(capabilityVersionID))
		Expect(usecase.grantProjection.Status).To(Equal("REVOKED"))
	})

	It("marks malformed grant events as non-retryable", func() {
		usecase := &projectionUsecaseStub{}
		listener := toolmessaging.NewToolGrantUpdatedEventListener(usecase)
		orgID := uuid.New()

		err := listener.Handle(context.Background(), orgID, &toolcatalogpb.ToolGrantUpdatedEvent{
			OrgId:               orgID.String(),
			CapabilityVersionId: uuid.NewString(),
			Status:              "MAYBE",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
		Expect(usecase.grantCalled).To(BeFalse())
	})

	It("projects credential binding update events without raw credential values", func() {
		usecase := &projectionUsecaseStub{}
		listener := toolmessaging.NewToolCredentialBindingUpdatedEventListener(usecase)
		orgID := uuid.New()

		err := listener.Handle(context.Background(), orgID, &toolcatalogpb.ToolCredentialBindingUpdatedEvent{
			OrgId:         orgID.String(),
			CapabilityId:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(usecase.bindingProjection.OrgID).To(Equal(orgID))
		Expect(usecase.bindingProjection.CapabilityID).To(Equal("partner.crm.lookup"))
		Expect(usecase.bindingProjection.CredentialRef).To(Equal("secrets/partner/crm"))
	})

	It("marks malformed credential binding events as non-retryable", func() {
		usecase := &projectionUsecaseStub{}
		listener := toolmessaging.NewToolCredentialBindingUpdatedEventListener(usecase)
		orgID := uuid.New()

		err := listener.Handle(context.Background(), orgID, &toolcatalogpb.ToolCredentialBindingUpdatedEvent{
			OrgId:        orgID.String(),
			CapabilityId: "partner.crm.lookup",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
		Expect(usecase.bindingCalled).To(BeFalse())
	})

	It("propagates capability repository errors as retryable", func() {
		usecase := &projectionUsecaseStub{err: errors.New("database unavailable")}
		listener := toolmessaging.NewToolCapabilityUpdatedEventListener(usecase)
		capabilityVersionID := uuid.New()

		err := listener.Handle(context.Background(), capabilityVersionID, validCapabilityUpdatedEvent(capabilityVersionID))

		Expect(err).To(MatchError("database unavailable"))
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeFalse())
	})

	It("propagates grant repository errors as retryable", func() {
		usecase := &projectionUsecaseStub{err: errors.New("database unavailable")}
		listener := toolmessaging.NewToolGrantUpdatedEventListener(usecase)
		orgID := uuid.New()

		err := listener.Handle(context.Background(), orgID, &toolcatalogpb.ToolGrantUpdatedEvent{
			OrgId:               orgID.String(),
			CapabilityVersionId: uuid.NewString(),
			Status:              "ACTIVE",
		})

		Expect(err).To(MatchError("database unavailable"))
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeFalse())
	})

	It("propagates credential binding repository errors as retryable", func() {
		usecase := &projectionUsecaseStub{err: errors.New("database unavailable")}
		listener := toolmessaging.NewToolCredentialBindingUpdatedEventListener(usecase)
		orgID := uuid.New()

		err := listener.Handle(context.Background(), orgID, &toolcatalogpb.ToolCredentialBindingUpdatedEvent{
			OrgId:         orgID.String(),
			CapabilityId:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		})

		Expect(err).To(MatchError("database unavailable"))
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeFalse())
	})
})

type projectionUsecaseStub struct {
	capabilityCalled     bool
	grantCalled          bool
	bindingCalled        bool
	capabilityProjection model.ToolCapabilityProjection
	grantProjection      model.ToolGrantProjection
	bindingProjection    model.ToolCredentialBindingProjection
	err                  error
}

func (s *projectionUsecaseStub) ApplyCapabilityProjection(_ context.Context, projection model.ToolCapabilityProjection) error {
	s.capabilityCalled = true
	s.capabilityProjection = projection
	return s.err
}

func (s *projectionUsecaseStub) ApplyGrantProjection(_ context.Context, projection model.ToolGrantProjection) error {
	s.grantCalled = true
	s.grantProjection = projection
	return s.err
}

func (s *projectionUsecaseStub) ApplyCredentialBindingProjection(_ context.Context, projection model.ToolCredentialBindingProjection) error {
	s.bindingCalled = true
	s.bindingProjection = projection
	return s.err
}

func validCapabilityUpdatedEvent(capabilityVersionID uuid.UUID) *toolcatalogpb.ToolCapabilityUpdatedEvent {
	return &toolcatalogpb.ToolCapabilityUpdatedEvent{
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
	}
}
