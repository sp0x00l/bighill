package test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	toolspb "lib/data_contracts_lib/tools"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tool catalog workflow", Label("tool-catalog"), func() {
	It("publishes, grants, binds, and projects a capability into tool execution", func() {
		user := createVerifiedProfileAndLogin()
		suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
		capabilityID := "partner.http.fetch." + suffix
		toolName := "partner_http_fetch_" + suffix

		status, body := doJSON(http.MethodPost, "/v1/private/tool-catalog/capabilities", map[string]any{
			"capability_id":       capabilityID,
			"version":             "2026-07-18-" + suffix,
			"tool_name":           toolName,
			"kind":                "HTTP_GET",
			"description":         "Fetches an allowlisted partner URL.",
			"parameters_json":     map[string]any{"type": "object", "additionalProperties": false, "required": []string{"url"}, "properties": map[string]any{"url": map[string]any{"type": "string", "format": "uri"}}},
			"egress_hosts":        []string{"example.com"},
			"timeout_ms":          1500,
			"max_response_bytes":  65536,
			"credential_name":     "partner-http-token",
			"credential_required": true,
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		capability := decodeSingleObject(body)
		capabilityVersionID := stringField(capability, "capability_version_id")
		Expect(capability["capability_id"]).To(Equal(capabilityID))
		Expect(capability["tool_name"]).To(Equal(toolName))
		Expect(capability["kind"]).To(Equal("HTTP_GET"))
		Expect(capability["lifecycle_status"]).To(Equal("ACTIVE"))
		Expect(stringField(capability, "content_hash")).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
		Expect(stringField(capability, "implementation_version")).To(HavePrefix("http_get:sha256:"))

		status, body = doJSON(http.MethodGet, "/v1/private/tool-catalog/capabilities/"+capabilityVersionID, nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		readCapability := decodeSingleObject(body)
		Expect(readCapability["capability_version_id"]).To(Equal(capabilityVersionID))
		Expect(readCapability["content_hash"]).To(Equal(capability["content_hash"]))

		status, body = doJSON(http.MethodPost, "/v1/private/tool-catalog/grants", map[string]any{
			"capability_version_id": capabilityVersionID,
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		grant := decodeSingleObject(body)
		Expect(grant["org_id"]).To(Equal(user.OrgID.String()))
		Expect(grant["status"]).To(Equal("ACTIVE"))

		status, body = doJSON(http.MethodPost, "/v1/private/tool-catalog/credential-bindings", map[string]any{
			"capability_id":  capabilityID,
			"credential_ref": "PARTNER_HTTP_TOKEN",
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		binding := decodeSingleObject(body)
		Expect(binding["org_id"]).To(Equal(user.OrgID.String()))
		Expect(binding["capability_id"]).To(Equal(capabilityID))
		Expect(binding["credential_ref"]).To(Equal("PARTNER_HTTP_TOKEN"))

		Eventually(func() []string {
			return availableToolNames(user.OrgID, user.ID)
		}, 30*time.Second, time.Second).Should(ContainElement(toolName))

		otherUser := createVerifiedProfileAndLogin()
		Consistently(func() []string {
			return availableToolNames(otherUser.OrgID, otherUser.ID)
		}, 3*time.Second, time.Second).ShouldNot(ContainElement(toolName))

		status, body = doJSON(http.MethodPost, "/v1/private/tool-catalog/grants", map[string]any{
			"capability_version_id": uuid.NewString(),
		}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusNotFound), "body: %s", string(body))
		Expect(strings.ToLower(string(body))).To(ContainSubstring("tool capability not found"))
	})
})

func availableToolNames(orgID uuid.UUID, userID uuid.UUID) []string {
	client, cleanup := newToolExecutionServiceClient()
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), toolExecutionServiceE2ERequestTimeout)
	defer cancel()
	response, err := client.ListAvailableTools(ctx, &toolspb.ListAvailableToolsRequest{
		OrgId:  orgID.String(),
		UserId: userID.String(),
	})
	if err != nil {
		return []string{fmt.Sprintf("error:%v", err)}
	}
	names := make([]string, 0, len(response.GetTools()))
	for _, tool := range response.GetTools() {
		names = append(names, tool.GetName())
	}
	return names
}
