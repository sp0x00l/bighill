package rest

import (
	"context"
	"errors"
	"net/http"

	"lib/shared_lib/authz"
	"lib/shared_lib/ctxutil"
	"lib/shared_lib/transport"
	"tool_catalog_service/pkg/app"
	"tool_catalog_service/pkg/domain"
	toolcatalogadapter "tool_catalog_service/pkg/infra/network/adapter"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	pathCapabilities       = "/v1/tool-catalog/capabilities"
	pathCapability         = "/v1/tool-catalog/capabilities/{capabilityVersionId}"
	pathTenantGrants       = "/v1/tool-catalog/grants"
	pathCredentialBindings = "/v1/tool-catalog/credential-bindings"
)

type ToolCatalogHandlers struct {
	usecase app.ToolCatalogUsecase
	adapter toolcatalogadapter.ToolCatalogDTOAdapter
}

func NewToolCatalogHandlers(usecase app.ToolCatalogUsecase, adapter toolcatalogadapter.ToolCatalogDTOAdapter) *ToolCatalogHandlers {
	log.Trace("NewToolCatalogHandlers")

	return &ToolCatalogHandlers{usecase: usecase, adapter: adapter}
}

func (h *ToolCatalogHandlers) GetRoutes() []Route {
	log.Trace("ToolCatalogHandlers GetRoutes")

	return []Route{
		{
			Path:     pathCapabilities,
			Handler:  h.PublishCapability,
			Method:   http.MethodPost,
			SpanName: "publish-tool-capability",
		},
		{
			Path:     pathCapability,
			Handler:  h.ReadCapability,
			Method:   http.MethodGet,
			SpanName: "read-tool-capability",
		},
		{
			Path:     pathTenantGrants,
			Handler:  h.GrantCapability,
			Method:   http.MethodPost,
			SpanName: "grant-tool-capability",
		},
		{
			Path:     pathCredentialBindings,
			Handler:  h.BindCredential,
			Method:   http.MethodPost,
			SpanName: "bind-tool-credential",
		},
	}
}

func (h *ToolCatalogHandlers) PublishCapability(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("ToolCatalogHandlers PublishCapability")

	userID, _, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := requirePermission(ctx, req, authz.PermissionToolCatalogPublish); err != nil {
		return nil, err
	}
	command, err := h.adapter.FromPublishCapabilityDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid tool capability request")
	}
	command.UserID = userID
	capability, err := h.usecase.PublishCapability(ctx, command)
	if err != nil {
		return nil, mapToolCatalogError(err)
	}
	payload, err := h.adapter.ToCapabilityDTO(ctx, capability)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode tool capability")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *ToolCatalogHandlers) ReadCapability(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("ToolCatalogHandlers ReadCapability")

	userID, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, err
	}
	capabilityVersionID, err := uuid.Parse(mux.Vars(req)["capabilityVersionId"])
	if err != nil || capabilityVersionID == uuid.Nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid capability version id")
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	capability, err := h.usecase.ReadCapabilityVersion(ctx, capabilityVersionID)
	if err != nil {
		return nil, mapToolCatalogError(err)
	}
	payload, err := h.adapter.ToCapabilityDTO(ctx, capability)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode tool capability")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *ToolCatalogHandlers) GrantCapability(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("ToolCatalogHandlers GrantCapability")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromGrantCapabilityDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid tool grant request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	grant, err := h.usecase.GrantCapability(ctx, command)
	if err != nil {
		return nil, mapToolCatalogError(err)
	}
	payload, err := h.adapter.ToGrantDTO(ctx, grant)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode tool grant")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *ToolCatalogHandlers) BindCredential(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("ToolCatalogHandlers BindCredential")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromBindCredentialDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid tool credential binding request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	binding, err := h.usecase.BindCredential(ctx, command)
	if err != nil {
		return nil, mapToolCatalogError(err)
	}
	payload, err := h.adapter.ToCredentialBindingDTO(ctx, binding)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode tool credential binding")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func readActorOrgBody(ctx context.Context, req *http.Request) (userID uuid.UUID, orgID uuid.UUID, body []byte, err error) {
	log.Trace("readActorOrgBody")

	user, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	org, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	body, err = transport.ReadReqBody(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	return user, org, body, nil
}

func readActorOrg(ctx context.Context, req *http.Request) (userID uuid.UUID, orgID uuid.UUID, err error) {
	log.Trace("readActorOrg")

	user, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	org, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	return user, org, nil
}

func requirePermission(ctx context.Context, req *http.Request, permission string) error {
	log.Trace("requirePermission")

	permissions, err := authz.ReadPermissionsHeader(ctx, req)
	if err != nil || !authz.HasPermission(permissions, permission) {
		return ErrForbidden().WithMessage("Forbidden")
	}
	return nil
}

func mapToolCatalogError(err error) error {
	log.Trace("mapToolCatalogError")

	switch {
	case errors.Is(err, domain.ErrToolCatalogValidation):
		return ErrBadRequest().Wrap(err).WithMessage("Invalid tool catalog request")
	case errors.Is(err, domain.ErrCapabilityNotFound):
		return ErrNotFound().Wrap(err).WithMessage("Tool capability not found")
	default:
		return ErrInternalServer().Wrap(err).WithMessage("Tool catalog request failed")
	}
}
