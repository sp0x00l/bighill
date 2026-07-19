package adapter

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"lib/shared_lib/serializer"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type ToolCatalogDTOAdapter interface {
	FromPublishCapabilityDTO(ctx context.Context, body []byte) (model.PublishCapabilityCommand, error)
	FromGrantCapabilityDTO(ctx context.Context, body []byte) (model.GrantCapabilityCommand, error)
	FromBindCredentialDTO(ctx context.Context, body []byte) (model.BindCredentialCommand, error)
	ToCapabilityDTO(ctx context.Context, capability *model.ToolCapabilityVersion) ([]byte, error)
	ToGrantDTO(ctx context.Context, grant *model.TenantCapabilityGrant) ([]byte, error)
	ToCredentialBindingDTO(ctx context.Context, binding *model.ToolCredentialBinding) ([]byte, error)
}

type toolCatalogDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializer.Encoder
}

type PublishCapabilityDTO struct {
	CapabilityID       string          `json:"capability_id" validate:"required"`
	Version            string          `json:"version" validate:"required"`
	ToolName           string          `json:"tool_name" validate:"required"`
	Kind               string          `json:"kind" validate:"required"`
	MCPServerEndpoint  string          `json:"mcp_server_endpoint,omitempty" validate:"omitempty,url"`
	Description        string          `json:"description" validate:"required"`
	ParametersJSON     json.RawMessage `json:"parameters_json" validate:"required"`
	EgressHosts        []string        `json:"egress_hosts" validate:"required,min=1,dive,required"`
	TimeoutMs          int64           `json:"timeout_ms" validate:"required,min=1"`
	MaxResponseBytes   int64           `json:"max_response_bytes" validate:"required,min=1"`
	CredentialName     string          `json:"credential_name,omitempty"`
	CredentialRequired bool            `json:"credential_required"`
}

type GrantCapabilityDTO struct {
	CapabilityVersionID string `json:"capability_version_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
}

type BindCredentialDTO struct {
	CapabilityID  string `json:"capability_id" validate:"required"`
	CredentialRef string `json:"credential_ref" validate:"required"`
}

type CapabilityDTO struct {
	CapabilityVersionID   string          `json:"capability_version_id"`
	CapabilityID          string          `json:"capability_id"`
	Version               string          `json:"version"`
	ToolName              string          `json:"tool_name"`
	Kind                  string          `json:"kind"`
	MCPServerEndpoint     string          `json:"mcp_server_endpoint,omitempty"`
	Description           string          `json:"description"`
	ParametersJSON        json.RawMessage `json:"parameters_json"`
	ImplementationVersion string          `json:"implementation_version"`
	EgressHosts           []string        `json:"egress_hosts"`
	TimeoutMs             int64           `json:"timeout_ms"`
	MaxResponseBytes      int64           `json:"max_response_bytes"`
	CredentialName        string          `json:"credential_name,omitempty"`
	CredentialRequired    bool            `json:"credential_required"`
	LifecycleStatus       string          `json:"lifecycle_status"`
	ContentHash           string          `json:"content_hash"`
	PublishedByUserID     string          `json:"published_by_user_id"`
	PublishedAt           string          `json:"published_at"`
}

type GrantDTO struct {
	GrantID             string `json:"grant_id"`
	OrgID               string `json:"org_id"`
	CapabilityVersionID string `json:"capability_version_id"`
	Status              string `json:"status"`
	GrantedByUserID     string `json:"granted_by_user_id"`
	GrantedAt           string `json:"granted_at"`
}

type CredentialBindingDTO struct {
	BindingID     string `json:"binding_id"`
	OrgID         string `json:"org_id"`
	CapabilityID  string `json:"capability_id"`
	CredentialRef string `json:"credential_ref"`
	BoundByUserID string `json:"bound_by_user_id"`
	BoundAt       string `json:"bound_at"`
}

func NewToolCatalogDTOAdapter(encoder *serializer.Encoder) ToolCatalogDTOAdapter {
	log.Trace("NewToolCatalogDTOAdapter")

	if encoder == nil {
		log.Fatal("tool catalog serializer is required")
	}
	return &toolCatalogDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *toolCatalogDTOAdapter) FromPublishCapabilityDTO(_ context.Context, body []byte) (model.PublishCapabilityCommand, error) {
	log.Trace("toolCatalogDTOAdapter FromPublishCapabilityDTO")

	var dto PublishCapabilityDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return model.PublishCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend("request body is invalid JSON")
	}
	trimPublishCapabilityDTO(&dto)
	if err := a.validator.Struct(dto); err != nil {
		return model.PublishCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend(err.Error())
	}
	var parametersValue any
	if err := json.Unmarshal(dto.ParametersJSON, &parametersValue); err != nil {
		return model.PublishCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend("parameters_json must be valid JSON")
	}
	parametersJSON, err := a.encoder.Serialize(parametersValue)
	if err != nil {
		return model.PublishCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend("parameters_json must be serializable")
	}
	kind, err := model.ToCapabilityKind(dto.Kind)
	if err != nil {
		return model.PublishCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend(err.Error())
	}
	if err := validateCapabilityEndpoint(kind, dto.MCPServerEndpoint, dto.EgressHosts); err != nil {
		return model.PublishCapabilityCommand{}, err
	}
	return model.PublishCapabilityCommand{
		CapabilityID:       dto.CapabilityID,
		Version:            dto.Version,
		ToolName:           dto.ToolName,
		Kind:               kind,
		MCPServerEndpoint:  dto.MCPServerEndpoint,
		Description:        dto.Description,
		ParametersJSON:     append([]byte(nil), parametersJSON...),
		EgressHosts:        append([]string(nil), dto.EgressHosts...),
		TimeoutMs:          dto.TimeoutMs,
		MaxResponseBytes:   dto.MaxResponseBytes,
		CredentialName:     dto.CredentialName,
		CredentialRequired: dto.CredentialRequired,
	}, nil
}

func (a *toolCatalogDTOAdapter) FromGrantCapabilityDTO(_ context.Context, body []byte) (model.GrantCapabilityCommand, error) {
	log.Trace("toolCatalogDTOAdapter FromGrantCapabilityDTO")

	var dto GrantCapabilityDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return model.GrantCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend("request body is invalid JSON")
	}
	dto.CapabilityVersionID = strings.TrimSpace(dto.CapabilityVersionID)
	if err := a.validator.Struct(dto); err != nil {
		return model.GrantCapabilityCommand{}, domain.ErrToolCatalogValidation.Extend(err.Error())
	}
	capabilityVersionID, err := parseRequiredUUID("capability_version_id", dto.CapabilityVersionID)
	if err != nil {
		return model.GrantCapabilityCommand{}, err
	}
	return model.GrantCapabilityCommand{CapabilityVersionID: capabilityVersionID}, nil
}

func (a *toolCatalogDTOAdapter) FromBindCredentialDTO(_ context.Context, body []byte) (model.BindCredentialCommand, error) {
	log.Trace("toolCatalogDTOAdapter FromBindCredentialDTO")

	var dto BindCredentialDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return model.BindCredentialCommand{}, domain.ErrToolCatalogValidation.Extend("request body is invalid JSON")
	}
	dto.CapabilityID = strings.TrimSpace(dto.CapabilityID)
	dto.CredentialRef = strings.TrimSpace(dto.CredentialRef)
	if err := a.validator.Struct(dto); err != nil {
		return model.BindCredentialCommand{}, domain.ErrToolCatalogValidation.Extend(err.Error())
	}
	return model.BindCredentialCommand{
		CapabilityID:  dto.CapabilityID,
		CredentialRef: dto.CredentialRef,
	}, nil
}

func (a *toolCatalogDTOAdapter) ToCapabilityDTO(_ context.Context, capability *model.ToolCapabilityVersion) ([]byte, error) {
	log.Trace("toolCatalogDTOAdapter ToCapabilityDTO")

	if capability == nil {
		return a.encoder.Serialize(CapabilityDTO{})
	}
	return a.encoder.Serialize(CapabilityDTO{
		CapabilityVersionID:   capability.CapabilityVersionID.String(),
		CapabilityID:          capability.CapabilityID,
		Version:               capability.Version,
		ToolName:              capability.ToolName,
		Kind:                  capability.Kind.String(),
		MCPServerEndpoint:     capability.MCPServerEndpoint,
		Description:           capability.Description,
		ParametersJSON:        json.RawMessage(capability.ParametersJSON),
		ImplementationVersion: capability.ImplementationVersion,
		EgressHosts:           append([]string(nil), capability.EgressHosts...),
		TimeoutMs:             capability.TimeoutMs,
		MaxResponseBytes:      capability.MaxResponseBytes,
		CredentialName:        capability.CredentialName,
		CredentialRequired:    capability.CredentialRequired,
		LifecycleStatus:       capability.LifecycleStatus.String(),
		ContentHash:           capability.ContentHash,
		PublishedByUserID:     capability.PublishedByUserID.String(),
		PublishedAt:           capability.PublishedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (a *toolCatalogDTOAdapter) ToGrantDTO(_ context.Context, grant *model.TenantCapabilityGrant) ([]byte, error) {
	log.Trace("toolCatalogDTOAdapter ToGrantDTO")

	if grant == nil {
		return a.encoder.Serialize(GrantDTO{})
	}
	return a.encoder.Serialize(GrantDTO{
		GrantID:             grant.GrantID.String(),
		OrgID:               grant.OrgID.String(),
		CapabilityVersionID: grant.CapabilityVersionID.String(),
		Status:              grant.Status.String(),
		GrantedByUserID:     grant.GrantedByUserID.String(),
		GrantedAt:           grant.GrantedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (a *toolCatalogDTOAdapter) ToCredentialBindingDTO(_ context.Context, binding *model.ToolCredentialBinding) ([]byte, error) {
	log.Trace("toolCatalogDTOAdapter ToCredentialBindingDTO")

	if binding == nil {
		return a.encoder.Serialize(CredentialBindingDTO{})
	}
	return a.encoder.Serialize(CredentialBindingDTO{
		BindingID:     binding.BindingID.String(),
		OrgID:         binding.OrgID.String(),
		CapabilityID:  binding.CapabilityID,
		CredentialRef: binding.CredentialRef,
		BoundByUserID: binding.BoundByUserID.String(),
		BoundAt:       binding.BoundAt.UTC().Format(time.RFC3339Nano),
	})
}

func trimPublishCapabilityDTO(dto *PublishCapabilityDTO) {
	log.Trace("trimPublishCapabilityDTO")

	dto.CapabilityID = strings.TrimSpace(dto.CapabilityID)
	dto.Version = strings.TrimSpace(dto.Version)
	dto.ToolName = strings.TrimSpace(dto.ToolName)
	dto.Kind = strings.TrimSpace(dto.Kind)
	dto.MCPServerEndpoint = strings.TrimSpace(dto.MCPServerEndpoint)
	dto.Description = strings.TrimSpace(dto.Description)
	dto.CredentialName = strings.TrimSpace(dto.CredentialName)
	for i := range dto.EgressHosts {
		dto.EgressHosts[i] = strings.TrimSpace(dto.EgressHosts[i])
	}
}

func validateCapabilityEndpoint(kind model.CapabilityKind, endpoint string, egressHosts []string) error {
	log.Trace("validateCapabilityEndpoint")

	endpoint = strings.TrimSpace(endpoint)
	if kind != model.CapabilityKindMCP {
		if endpoint != "" {
			return domain.ErrToolCatalogValidation.Extend("mcp_server_endpoint is only valid for MCP capabilities")
		}
		return nil
	}
	if endpoint == "" {
		return domain.ErrToolCatalogValidation.Extend("mcp_server_endpoint is required for MCP capabilities")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return domain.ErrToolCatalogValidation.Extend("mcp_server_endpoint is invalid")
	}
	host := strings.ToLower(parsed.Hostname())
	for _, candidate := range egressHosts {
		if strings.EqualFold(strings.TrimSpace(candidate), host) {
			return nil
		}
	}
	return domain.ErrToolCatalogValidation.Extend("mcp_server_endpoint host must be in egress_hosts")
}

func parseRequiredUUID(name string, raw string) (uuid.UUID, error) {
	log.Trace("parseRequiredUUID")

	value, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || value == uuid.Nil {
		return uuid.Nil, domain.ErrToolCatalogValidation.Extend(name + " is invalid")
	}
	return value, nil
}
