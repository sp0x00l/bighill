package messaging_test

import (
	"testing"

	toolcatalogpb "lib/data_contracts_lib/tool_catalog"
	msgConn "lib/shared_lib/messaging"
	"tool_catalog_service/pkg/domain/model"
	toolcatalogmessaging "tool_catalog_service/pkg/infra/network/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestToolCatalogEventBuilder(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog event builder unit test suite")
}

var _ = Describe("ToolCatalogEventBuilder", func() {
	var builder *toolcatalogmessaging.ToolCatalogEventBuilder

	BeforeEach(func() {
		builder = toolcatalogmessaging.NewToolCatalogEventBuilder("tool_catalog")
	})

	It("builds capability projection events from real capability metadata", func() {
		capability := validCapability()

		message := builder.CapabilityUpdatedMessage(capability)

		Expect(message.Topic).To(Equal("tool_catalog"))
		Expect(message.Message.MsgType).To(Equal(msgConn.MsgTypeToolCapabilityUpdated))
		Expect(message.Message.ResourceKey).To(Equal(capability.CapabilityVersionID))
		var payload toolcatalogpb.ToolCapabilityUpdatedEvent
		Expect(proto.Unmarshal(message.Message.Payload, &payload)).To(Succeed())
		Expect(payload.GetCapabilityId()).To(Equal(capability.CapabilityID))
		Expect(payload.GetToolName()).To(Equal(capability.ToolName))
		Expect(payload.GetMcpServerEndpoint()).To(Equal(capability.MCPServerEndpoint))
		Expect(payload.GetImplementationVersion()).To(Equal(capability.ImplementationVersion))
		Expect(payload.GetParametersJson()).To(MatchJSON(capability.ParametersJSON))
	})

	It("builds tenant grant projection events", func() {
		grant := &model.TenantCapabilityGrant{
			GrantID:             uuid.New(),
			OrgID:               uuid.New(),
			CapabilityVersionID: uuid.New(),
			Status:              model.TenantGrantStatusActive,
		}

		message := builder.GrantUpdatedMessage(grant)

		Expect(message.Message.MsgType).To(Equal(msgConn.MsgTypeToolGrantUpdated))
		Expect(message.Message.ResourceKey).To(Equal(grant.OrgID))
		var payload toolcatalogpb.ToolGrantUpdatedEvent
		Expect(proto.Unmarshal(message.Message.Payload, &payload)).To(Succeed())
		Expect(payload.GetOrgId()).To(Equal(grant.OrgID.String()))
		Expect(payload.GetCapabilityVersionId()).To(Equal(grant.CapabilityVersionID.String()))
		Expect(payload.GetStatus()).To(Equal("ACTIVE"))
	})

	It("builds credential binding projection events without raw credentials", func() {
		binding := &model.ToolCredentialBinding{
			BindingID:     uuid.New(),
			OrgID:         uuid.New(),
			CapabilityID:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
		}

		message := builder.CredentialBindingUpdatedMessage(binding)

		Expect(message.Message.MsgType).To(Equal(msgConn.MsgTypeToolCredentialBindingUpdated))
		Expect(message.Message.ResourceKey).To(Equal(binding.OrgID))
		var payload toolcatalogpb.ToolCredentialBindingUpdatedEvent
		Expect(proto.Unmarshal(message.Message.Payload, &payload)).To(Succeed())
		Expect(payload.GetCredentialRef()).To(Equal(binding.CredentialRef))
		Expect(string(message.Message.Payload)).NotTo(ContainSubstring("bearer-token-value"))
	})
})

func validCapability() *model.ToolCapabilityVersion {
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
	}
}
