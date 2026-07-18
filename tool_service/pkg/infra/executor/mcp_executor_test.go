package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"tool_service/pkg/domain"
	"tool_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MCPExecutor", func() {
	It("calls an MCP tool through HTTP JSON-RPC with boundary credentials", func() {
		client := &mcpHTTPClientStub{}
		executor := NewMCPExecutorWithClient("https://mcp.example/rpc", client, &credentialResolverStub{value: "token"})
		policy := mcpPolicy("mcp.example", `{"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string"}}}`)

		result, err := executor.Execute(context.Background(), mcpTool("search_partner"), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "search_partner",
			ArgumentsJSON: []byte(`{"query":"contract terms"}`),
			OrgID:         uuid.New(),
			UserID:        uuid.New(),
		}, policy)

		Expect(err).NotTo(HaveOccurred())
		Expect(client.method).To(Equal(mcpMethodToolsCall))
		Expect(client.arguments).To(HaveKeyWithValue("query", "contract terms"))
		Expect(client.authorization).To(Equal("Bearer token"))
		Expect(result.IsError).To(BeFalse())
		Expect(result.EgressHost).To(Equal("mcp.example"))
		Expect(result.ResultJSON).To(MatchJSON(`{"content":[{"text":"mcp result","type":"text"}]}`))
	})

	It("rejects invalid arguments before executing the MCP request", func() {
		client := &mcpHTTPClientStub{}
		executor := NewMCPExecutorWithClient("https://mcp.example/rpc", client, &credentialResolverStub{value: "token"})

		_, err := executor.Execute(context.Background(), mcpTool("search_partner"), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "search_partner",
			ArgumentsJSON: []byte(`{"unexpected":"value"}`),
			OrgID:         uuid.New(),
			UserID:        uuid.New(),
		}, mcpPolicy("mcp.example", `{"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string"}}}`))

		Expect(err).To(MatchError(ContainSubstring("tool arguments do not match schema")))
		Expect(client.called).To(BeFalse())
	})

	It("fails closed when the MCP credential cannot be resolved", func() {
		client := &mcpHTTPClientStub{}
		executor := NewMCPExecutorWithClient("https://mcp.example/rpc", client, &credentialResolverStub{err: domain.ErrToolDenied.Extend("missing secret")})

		_, err := executor.Execute(context.Background(), mcpTool("search_partner"), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "search_partner",
			ArgumentsJSON: []byte(`{"query":"contract terms"}`),
			OrgID:         uuid.New(),
			UserID:        uuid.New(),
		}, mcpPolicy("mcp.example", `{"type":"object","required":["query"],"properties":{"query":{"type":"string"}}}`))

		Expect(err).To(MatchError(ContainSubstring("missing secret")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))
		Expect(client.called).To(BeFalse())
	})

	It("discovers declared MCP tools with server-provided schemas", func() {
		client := &mcpHTTPClientStub{listMode: true}
		policy := mcpPolicy("mcp.example", `{"type":"object"}`)

		tools, err := discoverMCPToolsWithClient(context.Background(), MCPDiscoveryConfig{
			Endpoint:      "https://mcp.example/rpc",
			DeclaredTools: []string{"search_partner"},
			AllowedOrgIDs: []uuid.UUID{uuid.MustParse("11111111-1111-1111-1111-111111111111")},
		}, policy, &credentialResolverStub{value: "token"}, client)

		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(HaveLen(1))
		Expect(tools[0].Name).To(Equal("search_partner"))
		Expect(tools[0].ExecutorKind).To(Equal(model.ToolExecutorKindMCP))
		Expect(tools[0].ParametersJSON).To(MatchJSON(`{"additionalProperties":false,"properties":{"query":{"type":"string"}},"required":["query"],"type":"object"}`))
		Expect(tools[0].ImplementationVersion).To(ContainSubstring("mcp:mcp.example:"))
		Expect(tools[0].EgressHosts).To(Equal([]string{"mcp.example"}))
	})

	It("does not register a placeholder when a declared MCP tool is absent", func() {
		client := &mcpHTTPClientStub{listMode: true}
		policy := mcpPolicy("mcp.example", `{"type":"object"}`)

		tools, err := discoverMCPToolsWithClient(context.Background(), MCPDiscoveryConfig{
			Endpoint:      "https://mcp.example/rpc",
			DeclaredTools: []string{"missing_tool"},
			AllowedOrgIDs: []uuid.UUID{uuid.New()},
		}, policy, &credentialResolverStub{value: "token"}, client)

		Expect(err).To(MatchError(ContainSubstring("declared mcp tool is unavailable")))
		Expect(tools).To(BeNil())
	})
})

func mcpTool(name string) *model.ToolDefinition {
	return &model.ToolDefinition{
		Name:                  name,
		ImplementationVersion: "mcp:mcp.example:hash",
		ExecutorKind:          model.ToolExecutorKindMCP,
		EgressHosts:           []string{"mcp.example"},
		Enabled:               true,
	}
}

func mcpPolicy(host string, schema string) model.PolicySet {
	policy := policyWithHosts(host)
	policy.Credential = model.CredentialPolicy{
		SecretRef:  "MCP_TOKEN",
		HeaderName: "Authorization",
		Prefix:     "Bearer ",
	}
	policy.Schema.InputSchemaJSON = []byte(schema)
	return policy
}

type credentialResolverStub struct {
	value string
	err   error
}

func (s *credentialResolverStub) ResolveCredential(context.Context, string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return s.value, nil
}

type mcpHTTPClientStub struct {
	called        bool
	listMode      bool
	method        string
	arguments     map[string]any
	authorization string
}

func (c *mcpHTTPClientStub) Do(req *http.Request) (*http.Response, error) {
	c.called = true
	c.authorization = req.Header.Get("Authorization")
	var request struct {
		Method string `json:"method"`
		Params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		} `json:"params"`
	}
	Expect(json.NewDecoder(req.Body).Decode(&request)).To(Succeed())
	c.method = request.Method
	c.arguments = request.Params.Arguments
	if c.listMode || request.Method == mcpMethodToolsList {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"jsonrpc":"2.0",
				"id":"tools-list",
				"result":{
					"tools":[{
						"name":"search_partner",
						"description":"Search partner system",
						"inputSchema":{
							"type":"object",
							"additionalProperties":false,
							"required":["query"],
							"properties":{"query":{"type":"string"}}
						}
					}]
				}
			}`)),
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":"call","result":{"content":[{"type":"text","text":"mcp result"}]}}`)),
	}, nil
}
