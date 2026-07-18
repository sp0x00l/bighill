package policy

import (
	"testing"
	"time"

	"tool_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBoundaryPolicyResolver(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool Service Boundary Policy Suite")
}

var _ = Describe("BoundaryPolicyResolver", func() {
	It("resolves boundary policy from a tool definition and platform config", func() {
		resolver := NewBoundaryPolicyResolver(BoundaryPolicyConfig{
			HTTPTimeout:            3 * time.Second,
			HTTPMaxResponseBytes:   4096,
			PinnedMCPCredentialRef: "MCP_TOKEN",
		})
		tool := &model.ToolDefinition{
			Name:           "http_get",
			EgressHosts:    []string{"example.com"},
			ParametersJSON: []byte(`{"type":"object"}`),
		}

		policy, err := resolver.ResolvePolicy(tool)

		Expect(err).NotTo(HaveOccurred())
		Expect(policy.Egress.AllowedSchemes).To(Equal([]string{"http", "https"}))
		Expect(policy.Egress.AllowedHosts).To(Equal([]string{"example.com"}))
		Expect(policy.Timeout.CallTimeout).To(Equal(3 * time.Second))
		Expect(policy.ResponseCap.MaxBytes).To(Equal(int64(4096)))
		Expect(policy.Credential.Mode).To(Equal(credentialModeNone))
		Expect(policy.Schema.InputSchemaJSON).To(MatchJSON(`{"type":"object"}`))
	})

	It("attaches the configured credential policy for MCP tools", func() {
		resolver := NewBoundaryPolicyResolver(BoundaryPolicyConfig{
			HTTPTimeout:            3 * time.Second,
			HTTPMaxResponseBytes:   4096,
			PinnedMCPCredentialRef: "MCP_TOKEN",
		})
		tool := &model.ToolDefinition{
			Name:           "partner_tool",
			ExecutorKind:   model.ToolExecutorKindMCP,
			EgressHosts:    []string{"mcp.example"},
			ParametersJSON: []byte(`{"type":"object"}`),
		}

		policy, err := resolver.ResolvePolicy(tool)

		Expect(err).NotTo(HaveOccurred())
		Expect(policy.Credential.Mode).To(Equal(credentialModeBearer))
		Expect(policy.Credential.SecretRef).To(Equal("MCP_TOKEN"))
		Expect(policy.Credential.HeaderName).To(Equal(credentialHeaderAuthorization))
		Expect(policy.Credential.Prefix).To(Equal(credentialPrefixBearer))
	})

	It("copies mutable tool slices into the resolved policy", func() {
		resolver := NewBoundaryPolicyResolver(BoundaryPolicyConfig{})
		tool := &model.ToolDefinition{
			EgressHosts:    []string{"first.example"},
			ParametersJSON: []byte(`{"type":"object"}`),
		}

		policy, err := resolver.ResolvePolicy(tool)
		Expect(err).NotTo(HaveOccurred())

		tool.EgressHosts[0] = "changed.example"
		tool.ParametersJSON[0] = '['
		Expect(policy.Egress.AllowedHosts).To(Equal([]string{"first.example"}))
		Expect(policy.Schema.InputSchemaJSON).To(MatchJSON(`{"type":"object"}`))
	})
})
