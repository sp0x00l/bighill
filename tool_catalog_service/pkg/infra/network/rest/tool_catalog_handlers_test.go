package rest_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"lib/shared_lib/authz"
	"lib/shared_lib/serializer"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"
	toolcatalogadapter "tool_catalog_service/pkg/infra/network/adapter"
	toolcatalogrest "tool_catalog_service/pkg/infra/network/rest"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolCatalogHandlers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool catalog REST handler unit test suite")
}

var _ = Describe("ToolCatalogHandlers", func() {
	var (
		orgID    uuid.UUID
		userID   uuid.UUID
		usecase  *toolCatalogUsecaseStub
		handlers *toolcatalogrest.ToolCatalogHandlers
	)

	BeforeEach(func() {
		orgID = uuid.New()
		userID = uuid.New()
		usecase = &toolCatalogUsecaseStub{}
		handlers = toolcatalogrest.NewToolCatalogHandlers(usecase, toolcatalogadapter.NewToolCatalogDTOAdapter(serializer.NewJSONSerializer()))
	})

	It("publishes capabilities with the actor from headers", func() {
		usecase.capability = validCapabilityResponse(userID)
		req := requestWithActorPermissions(http.MethodPost, "/v1/tool-catalog/capabilities", `{
			"capability_id":"partner.crm.lookup",
			"version":"2026-07-18",
			"tool_name":"crm_lookup",
			"kind":"MCP",
			"mcp_server_endpoint":"https://mcp.partner.example/rpc",
			"description":"Looks up a customer.",
			"parameters_json":{"type":"object"},
			"egress_hosts":["mcp.partner.example"],
			"timeout_ms":1500,
			"max_response_bytes":65536,
			"credential_name":"partner-crm-token",
			"credential_required":true
		}`, orgID, userID, authz.PermissionToolCatalogPublish)

		response, err := handlers.PublishCapability(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(usecase.publishCommand.UserID).To(Equal(userID))
		Expect(usecase.publishCommand.CapabilityID).To(Equal("partner.crm.lookup"))
		Expect(usecase.publishCommand.Kind).To(Equal(model.CapabilityKindMCP))
		Expect(usecase.publishCommand.MCPServerEndpoint).To(Equal("https://mcp.partner.example/rpc"))
		Expect(response.Payload()).To(MatchJSON(`{
			"capability_version_id":"` + usecase.capability.CapabilityVersionID.String() + `",
			"capability_id":"partner.crm.lookup",
			"version":"2026-07-18",
			"tool_name":"crm_lookup",
			"kind":"MCP",
			"mcp_server_endpoint":"https://mcp.partner.example/rpc",
			"description":"Looks up a customer.",
			"parameters_json":{"type":"object"},
			"implementation_version":"mcp:sha256:test",
			"egress_hosts":["mcp.partner.example"],
			"timeout_ms":1500,
			"max_response_bytes":65536,
			"credential_name":"partner-crm-token",
			"credential_required":true,
			"lifecycle_status":"ACTIVE",
			"content_hash":"sha256:test",
			"published_by_user_id":"` + userID.String() + `",
			"published_at":"` + usecase.capability.PublishedAt.UTC().Format(time.RFC3339Nano) + `"
		}`))
	})

	It("rejects publish when the trusted permissions do not include tool catalog publish", func() {
		req := requestWithActorPermissions(http.MethodPost, "/v1/tool-catalog/capabilities", `{
			"capability_id":"partner.crm.lookup",
			"version":"2026-07-18",
			"tool_name":"crm_lookup",
			"kind":"MCP",
			"mcp_server_endpoint":"https://mcp.partner.example/rpc",
			"description":"Looks up a customer.",
			"parameters_json":{"type":"object"},
			"egress_hosts":["mcp.partner.example"],
			"timeout_ms":1500,
			"max_response_bytes":65536
		}`, orgID, userID, authz.PermissionModelWrite)

		response, err := handlers.PublishCapability(context.Background(), req)

		Expect(response).To(BeNil())
		Expect(err).To(MatchError("Forbidden"))
		Expect(usecase.publishCalled).To(BeFalse())
	})

	It("rejects malformed publish DTOs before the usecase", func() {
		req := requestWithActorPermissions(http.MethodPost, "/v1/tool-catalog/capabilities", `{
			"capability_id":"partner.crm.lookup"
		}`, orgID, userID, authz.PermissionToolCatalogPublish)

		response, err := handlers.PublishCapability(context.Background(), req)

		Expect(response).To(BeNil())
		Expect(err).To(MatchError("Invalid tool capability request"))
		Expect(usecase.publishCalled).To(BeFalse())
	})

	It("reads capabilities by version id", func() {
		usecase.capability = validCapabilityResponse(userID)
		req := requestWithActor(http.MethodGet, "/v1/tool-catalog/capabilities/"+usecase.capability.CapabilityVersionID.String(), "", orgID, userID)
		req = mux.SetURLVars(req, map[string]string{"capabilityVersionId": usecase.capability.CapabilityVersionID.String()})

		response, err := handlers.ReadCapability(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusOK))
		Expect(usecase.readCapabilityVersionID).To(Equal(usecase.capability.CapabilityVersionID))
	})

	It("maps missing capabilities to not found errors", func() {
		missing := uuid.New()
		usecase.err = domain.ErrCapabilityNotFound.Extend("missing")
		req := requestWithActor(http.MethodGet, "/v1/tool-catalog/capabilities/"+missing.String(), "", orgID, userID)
		req = mux.SetURLVars(req, map[string]string{"capabilityVersionId": missing.String()})

		response, err := handlers.ReadCapability(context.Background(), req)

		Expect(response).To(BeNil())
		Expect(errors.Is(err, domain.ErrCapabilityNotFound)).To(BeTrue())
		Expect(err).To(MatchError("Tool capability not found"))
	})

	It("grants capabilities to the actor org", func() {
		capabilityVersionID := uuid.New()
		usecase.grant = &model.TenantCapabilityGrant{
			GrantID:             uuid.New(),
			OrgID:               orgID,
			CapabilityVersionID: capabilityVersionID,
			Status:              model.TenantGrantStatusActive,
			GrantedByUserID:     userID,
			GrantedAt:           time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC),
		}
		req := requestWithActor(http.MethodPost, "/v1/tool-catalog/grants", `{
			"capability_version_id":"`+capabilityVersionID.String()+`"
		}`, orgID, userID)

		response, err := handlers.GrantCapability(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(usecase.grantCommand.UserID).To(Equal(userID))
		Expect(usecase.grantCommand.OrgID).To(Equal(orgID))
		Expect(usecase.grantCommand.CapabilityVersionID).To(Equal(capabilityVersionID))
	})

	It("binds credential refs to the actor org without raw credential values", func() {
		usecase.binding = &model.ToolCredentialBinding{
			BindingID:     uuid.New(),
			OrgID:         orgID,
			CapabilityID:  "partner.crm.lookup",
			CredentialRef: "secrets/partner/crm",
			BoundByUserID: userID,
			BoundAt:       time.Date(2026, 7, 18, 11, 5, 0, 0, time.UTC),
		}
		req := requestWithActor(http.MethodPost, "/v1/tool-catalog/credential-bindings", `{
			"capability_id":"partner.crm.lookup",
			"credential_ref":"secrets/partner/crm"
		}`, orgID, userID)

		response, err := handlers.BindCredential(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(usecase.bindCommand.UserID).To(Equal(userID))
		Expect(usecase.bindCommand.OrgID).To(Equal(orgID))
		Expect(usecase.bindCommand.CredentialRef).To(Equal("secrets/partner/crm"))
		Expect(string(response.Payload())).NotTo(ContainSubstring("bearer-token-value"))
	})
})

type toolCatalogUsecaseStub struct {
	publishCalled           bool
	publishCommand          model.PublishCapabilityCommand
	grantCommand            model.GrantCapabilityCommand
	bindCommand             model.BindCredentialCommand
	readCapabilityVersionID uuid.UUID
	capability              *model.ToolCapabilityVersion
	grant                   *model.TenantCapabilityGrant
	binding                 *model.ToolCredentialBinding
	err                     error
}

func (s *toolCatalogUsecaseStub) PublishCapability(_ context.Context, command model.PublishCapabilityCommand) (*model.ToolCapabilityVersion, error) {
	s.publishCalled = true
	s.publishCommand = command
	return s.capability, s.err
}

func (s *toolCatalogUsecaseStub) GrantCapability(_ context.Context, command model.GrantCapabilityCommand) (*model.TenantCapabilityGrant, error) {
	s.grantCommand = command
	return s.grant, s.err
}

func (s *toolCatalogUsecaseStub) BindCredential(_ context.Context, command model.BindCredentialCommand) (*model.ToolCredentialBinding, error) {
	s.bindCommand = command
	return s.binding, s.err
}

func (s *toolCatalogUsecaseStub) ReadCapabilityVersion(_ context.Context, capabilityVersionID uuid.UUID) (*model.ToolCapabilityVersion, error) {
	s.readCapabilityVersionID = capabilityVersionID
	return s.capability, s.err
}

func requestWithActor(method string, path string, body string, orgID uuid.UUID, userID uuid.UUID) *http.Request {
	return requestWithActorPermissions(method, path, body, orgID, userID)
}

func requestWithActorPermissions(method string, path string, body string, orgID uuid.UUID, userID uuid.UUID, permissions ...string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(authz.HeaderOrgID, orgID.String())
	req.Header.Set(authz.HeaderUserID, userID.String())
	if len(permissions) > 0 {
		req.Header.Set(authz.HeaderPermissions, authz.EncodeStringSlice(permissions))
	}
	return req
}

func validCapabilityResponse(userID uuid.UUID) *model.ToolCapabilityVersion {
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
		PublishedByUserID:     userID,
		PublishedAt:           time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC),
	}
}
