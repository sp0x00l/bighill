package adapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"lib/shared_lib/serializer"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"
	toolcatalogadapter "tool_catalog_service/pkg/infra/network/adapter"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolCatalogDTOAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog DTO adapter unit test suite")
}

var _ = Describe("ToolCatalogDTOAdapter", func() {
	var adapter toolcatalogadapter.ToolCatalogDTOAdapter

	BeforeEach(func() {
		adapter = toolcatalogadapter.NewToolCatalogDTOAdapter(serializer.NewJSONSerializer())
	})

	It("parses and canonicalizes capability publish requests at the boundary", func() {
		command, err := adapter.FromPublishCapabilityDTO(context.Background(), []byte(`{
			"capability_id":"partner.crm.lookup",
			"version":"2026-07-18",
			"tool_name":"crm_lookup",
			"kind":"MCP",
			"mcp_server_endpoint":"https://mcp.partner.example/rpc",
			"description":"Looks up a customer.",
			"parameters_json":{
				"additionalProperties":false,
				"type":"object"
			},
			"egress_hosts":["mcp.partner.example"],
			"timeout_ms":1500,
			"max_response_bytes":65536,
			"credential_name":"partner-crm-token",
			"credential_required":true
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.Kind).To(Equal(model.CapabilityKindMCP))
		Expect(command.MCPServerEndpoint).To(Equal("https://mcp.partner.example/rpc"))
		Expect(command.ParametersJSON).To(MatchJSON(`{"additionalProperties":false,"type":"object"}`))
		Expect(string(command.ParametersJSON)).To(Equal(`{"additionalProperties":false,"type":"object"}`))
	})

	It("rejects invalid capability kinds at the boundary", func() {
		_, err := adapter.FromPublishCapabilityDTO(context.Background(), []byte(`{
			"capability_id":"partner.crm.lookup",
			"version":"2026-07-18",
			"tool_name":"crm_lookup",
			"kind":"CONTAINER",
			"description":"Looks up a customer.",
			"parameters_json":{"type":"object"},
			"egress_hosts":["mcp.partner.example"],
			"timeout_ms":1500,
			"max_response_bytes":65536
		}`))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrToolCatalogValidation)).To(BeTrue())
	})

	It("parses tenant grant requests at the boundary", func() {
		capabilityVersionID := uuid.New()

		command, err := adapter.FromGrantCapabilityDTO(context.Background(), []byte(`{
			"capability_version_id":"`+capabilityVersionID.String()+`"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.CapabilityVersionID).To(Equal(capabilityVersionID))
	})

	It("rejects malformed tenant grant UUIDs at the boundary", func() {
		_, err := adapter.FromGrantCapabilityDTO(context.Background(), []byte(`{
			"capability_version_id":"not-a-uuid"
		}`))

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrToolCatalogValidation)).To(BeTrue())
	})

	It("parses credential binding requests at the boundary", func() {
		command, err := adapter.FromBindCredentialDTO(context.Background(), []byte(`{
			"capability_id":"partner.crm.lookup",
			"credential_ref":"secrets/partner/crm"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.CapabilityID).To(Equal("partner.crm.lookup"))
		Expect(command.CredentialRef).To(Equal("secrets/partner/crm"))
	})

	It("serializes capability responses", func() {
		capability := &model.ToolCapabilityVersion{
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

		payload, err := adapter.ToCapabilityDTO(context.Background(), capability)

		Expect(err).NotTo(HaveOccurred())
		var decoded map[string]any
		Expect(json.Unmarshal(payload, &decoded)).To(Succeed())
		Expect(decoded).To(HaveKeyWithValue("capability_id", capability.CapabilityID))
		Expect(decoded).To(HaveKeyWithValue("kind", "MCP"))
		Expect(decoded).To(HaveKeyWithValue("mcp_server_endpoint", "https://mcp.partner.example/rpc"))
		Expect(decoded).To(HaveKeyWithValue("implementation_version", capability.ImplementationVersion))
	})
})
