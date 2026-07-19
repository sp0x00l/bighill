package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestMCPManifestVerifier(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog MCP manifest verifier unit test suite")
}

var _ = Describe("ManifestVerifier", func() {
	It("accepts a declared MCP capability when the live schema matches", func() {
		verifier := NewManifestVerifier(&manifestHTTPClientStub{body: listToolsResponse(`{"type":"object","additionalProperties":false}`)}, time.Second)

		err := verifier.VerifyCapabilityManifest(context.Background(), publishCommandWithSchema(`{"additionalProperties":false,"type":"object"}`))

		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects a declared MCP capability when the live schema differs", func() {
		verifier := NewManifestVerifier(&manifestHTTPClientStub{body: listToolsResponse(`{"type":"object","required":["query"]}`)}, time.Second)

		err := verifier.VerifyCapabilityManifest(context.Background(), publishCommandWithSchema(`{"type":"object","additionalProperties":false}`))

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("mcp tool schema does not match live tool")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolCatalogValidation.Error() + ".*")))
	})

	It("rejects a declared MCP capability when the live server does not expose the tool", func() {
		verifier := NewManifestVerifier(&manifestHTTPClientStub{body: listToolsResponseForTool("different_tool", `{"type":"object"}`)}, time.Second)

		err := verifier.VerifyCapabilityManifest(context.Background(), publishCommandWithSchema(`{"type":"object"}`))

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("mcp tool is not available")))
	})

	It("validates HTTPGet schemas without calling a live verifier endpoint", func() {
		client := &manifestHTTPClientStub{body: listToolsResponse(`{"type":"object"}`)}
		verifier := NewManifestVerifier(client, time.Second)

		err := verifier.VerifyCapabilityManifest(context.Background(), publishHTTPGetCommandWithSchema(`{"type":"object","properties":{"url":{"type":"string","format":"uri"}}}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(client.called).To(BeFalse())
	})

	It("rejects empty HTTPGet schemas", func() {
		verifier := NewManifestVerifier(&manifestHTTPClientStub{}, time.Second)

		err := verifier.VerifyCapabilityManifest(context.Background(), publishHTTPGetCommandWithSchema(`{}`))

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("tool input schema must not be empty")))
	})

	It("rejects non-object HTTPGet schemas", func() {
		verifier := NewManifestVerifier(&manifestHTTPClientStub{}, time.Second)

		err := verifier.VerifyCapabilityManifest(context.Background(), publishHTTPGetCommandWithSchema(`null`))

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("tool input schema must be a JSON object")))
	})
})

type manifestHTTPClientStub struct {
	body   string
	called bool
}

func (s *manifestHTTPClientStub) Do(*http.Request) (*http.Response, error) {
	log.Trace("manifestHTTPClientStub Do")

	s.called = true
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(s.body)),
		Header:     http.Header{},
	}, nil
}

func publishCommandWithSchema(schema string) model.PublishCapabilityCommand {
	log.Trace("publishCommandWithSchema")

	return model.PublishCapabilityCommand{
		UserID:             uuid.New(),
		CapabilityID:       "partner.crm.lookup",
		Version:            "2026-07-18",
		ToolName:           "crm_lookup",
		Kind:               model.CapabilityKindMCP,
		MCPServerEndpoint:  "https://mcp.partner.example/rpc",
		Description:        "Looks up a customer.",
		ParametersJSON:     []byte(schema),
		EgressHosts:        []string{"mcp.partner.example"},
		TimeoutMs:          1500,
		MaxResponseBytes:   65536,
		CredentialName:     "partner-crm-token",
		CredentialRequired: true,
	}
}

func publishHTTPGetCommandWithSchema(schema string) model.PublishCapabilityCommand {
	log.Trace("publishHTTPGetCommandWithSchema")

	command := publishCommandWithSchema(schema)
	command.Kind = model.CapabilityKindHTTPGet
	command.CapabilityID = "platform.http.get"
	command.ToolName = "http_get"
	command.MCPServerEndpoint = ""
	command.EgressHosts = []string{"example.com"}
	command.CredentialName = ""
	command.CredentialRequired = false
	return command
}

func listToolsResponse(inputSchema string) string {
	log.Trace("listToolsResponse")

	return listToolsResponseForTool("crm_lookup", inputSchema)
}

func listToolsResponseForTool(toolName string, inputSchema string) string {
	log.Trace("listToolsResponseForTool")

	return `{
		"jsonrpc":"2.0",
		"id":"tools-list",
		"result":{
			"tools":[{
				"name":"` + toolName + `",
				"inputSchema":` + inputSchema + `
			}]
		}
	}`
}
