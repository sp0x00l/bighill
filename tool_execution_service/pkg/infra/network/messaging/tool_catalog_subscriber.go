package messaging

import (
	"context"
	"fmt"
	"strings"

	toolcatalogpb "lib/data_contracts_lib/tool_catalog"
	msgConn "lib/shared_lib/messaging"
	"tool_execution_service/pkg/app"
	"tool_execution_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type ToolCatalogSubscriber interface {
	Start(ctx context.Context) error
}

type toolCatalogSubscriber struct {
	subscriber msgConn.Subscriber
	usecase    app.ToolCatalogProjectionUsecase
	topic      string
}

func NewToolCatalogSubscriber(subscriber msgConn.Subscriber, usecase app.ToolCatalogProjectionUsecase, topic string) ToolCatalogSubscriber {
	log.Trace("NewToolCatalogSubscriber")

	return &toolCatalogSubscriber{subscriber: subscriber, usecase: usecase, topic: strings.TrimSpace(topic)}
}

func (s *toolCatalogSubscriber) Start(ctx context.Context) error {
	log.Trace("toolCatalogSubscriber Start")

	msgConn.AddListener(s.subscriber, NewToolCapabilityUpdatedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewToolGrantUpdatedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewToolCredentialBindingUpdatedEventListener(s.usecase))
	if s.topic == "" {
		return nil
	}
	return s.subscriber.Subscribe(ctx, []string{s.topic})
}

type toolCapabilityUpdatedEventListener struct {
	usecase app.ToolCatalogProjectionUsecase
}

func NewToolCapabilityUpdatedEventListener(usecase app.ToolCatalogProjectionUsecase) *toolCapabilityUpdatedEventListener {
	log.Trace("NewToolCapabilityUpdatedEventListener")

	return &toolCapabilityUpdatedEventListener{usecase: usecase}
}

func (l *toolCapabilityUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("toolCapabilityUpdatedEventListener MsgType")

	return msgConn.MsgTypeToolCapabilityUpdated
}

func (l *toolCapabilityUpdatedEventListener) NewMessage() *toolcatalogpb.ToolCapabilityUpdatedEvent {
	log.Trace("toolCapabilityUpdatedEventListener NewMessage")

	return &toolcatalogpb.ToolCapabilityUpdatedEvent{}
}

func (l *toolCapabilityUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *toolcatalogpb.ToolCapabilityUpdatedEvent) error {
	log.Trace("toolCapabilityUpdatedEventListener Handle")

	projection, err := capabilityProjectionFromEvent(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.usecase.ApplyCapabilityProjection(ctx, projection)
}

type toolGrantUpdatedEventListener struct {
	usecase app.ToolCatalogProjectionUsecase
}

func NewToolGrantUpdatedEventListener(usecase app.ToolCatalogProjectionUsecase) *toolGrantUpdatedEventListener {
	log.Trace("NewToolGrantUpdatedEventListener")

	return &toolGrantUpdatedEventListener{usecase: usecase}
}

func (l *toolGrantUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("toolGrantUpdatedEventListener MsgType")

	return msgConn.MsgTypeToolGrantUpdated
}

func (l *toolGrantUpdatedEventListener) NewMessage() *toolcatalogpb.ToolGrantUpdatedEvent {
	log.Trace("toolGrantUpdatedEventListener NewMessage")

	return &toolcatalogpb.ToolGrantUpdatedEvent{}
}

func (l *toolGrantUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *toolcatalogpb.ToolGrantUpdatedEvent) error {
	log.Trace("toolGrantUpdatedEventListener Handle")

	projection, err := grantProjectionFromEvent(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.usecase.ApplyGrantProjection(ctx, projection)
}

type toolCredentialBindingUpdatedEventListener struct {
	usecase app.ToolCatalogProjectionUsecase
}

func NewToolCredentialBindingUpdatedEventListener(usecase app.ToolCatalogProjectionUsecase) *toolCredentialBindingUpdatedEventListener {
	log.Trace("NewToolCredentialBindingUpdatedEventListener")

	return &toolCredentialBindingUpdatedEventListener{usecase: usecase}
}

func (l *toolCredentialBindingUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("toolCredentialBindingUpdatedEventListener MsgType")

	return msgConn.MsgTypeToolCredentialBindingUpdated
}

func (l *toolCredentialBindingUpdatedEventListener) NewMessage() *toolcatalogpb.ToolCredentialBindingUpdatedEvent {
	log.Trace("toolCredentialBindingUpdatedEventListener NewMessage")

	return &toolcatalogpb.ToolCredentialBindingUpdatedEvent{}
}

func (l *toolCredentialBindingUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *toolcatalogpb.ToolCredentialBindingUpdatedEvent) error {
	log.Trace("toolCredentialBindingUpdatedEventListener Handle")

	projection, err := credentialBindingProjectionFromEvent(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.usecase.ApplyCredentialBindingProjection(ctx, projection)
}

func capabilityProjectionFromEvent(resourceKey uuid.UUID, payload *toolcatalogpb.ToolCapabilityUpdatedEvent) (model.ToolCapabilityProjection, error) {
	log.Trace("capabilityProjectionFromEvent")

	if resourceKey == uuid.Nil {
		return model.ToolCapabilityProjection{}, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.ToolCapabilityProjection{}, fmt.Errorf("tool capability updated payload is required")
	}
	capabilityVersionID, err := msgConn.ParseUUID("capability_version_id", payload.GetCapabilityVersionId())
	if err != nil {
		return model.ToolCapabilityProjection{}, err
	}
	if capabilityVersionID != resourceKey {
		return model.ToolCapabilityProjection{}, fmt.Errorf("capability version id %s does not match resource key %s", capabilityVersionID, resourceKey)
	}
	kind, err := model.ToToolExecutorKind(payload.GetKind())
	if err != nil {
		return model.ToolCapabilityProjection{}, err
	}
	projection := model.ToolCapabilityProjection{
		CapabilityVersionID:   capabilityVersionID,
		CapabilityID:          strings.TrimSpace(payload.GetCapabilityId()),
		Version:               strings.TrimSpace(payload.GetVersion()),
		ToolName:              strings.TrimSpace(payload.GetToolName()),
		ExecutorKind:          kind,
		MCPServerEndpoint:     strings.TrimSpace(payload.GetMcpServerEndpoint()),
		Description:           strings.TrimSpace(payload.GetDescription()),
		ParametersJSON:        append([]byte(nil), payload.GetParametersJson()...),
		ImplementationVersion: strings.TrimSpace(payload.GetImplementationVersion()),
		EgressHosts:           cleanStrings(payload.GetEgressHosts()),
		TimeoutMs:             payload.GetTimeoutMs(),
		MaxResponseBytes:      payload.GetMaxResponseBytes(),
		CredentialName:        strings.TrimSpace(payload.GetCredentialName()),
		CredentialRequired:    payload.GetCredentialRequired(),
		LifecycleStatus:       strings.TrimSpace(payload.GetLifecycleStatus()),
		ContentHash:           strings.TrimSpace(payload.GetContentHash()),
	}
	if err := validateCapabilityProjection(projection); err != nil {
		return model.ToolCapabilityProjection{}, err
	}
	return projection, nil
}

func grantProjectionFromEvent(resourceKey uuid.UUID, payload *toolcatalogpb.ToolGrantUpdatedEvent) (model.ToolGrantProjection, error) {
	log.Trace("grantProjectionFromEvent")

	if resourceKey == uuid.Nil {
		return model.ToolGrantProjection{}, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.ToolGrantProjection{}, fmt.Errorf("tool grant updated payload is required")
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.ToolGrantProjection{}, err
	}
	if orgID != resourceKey {
		return model.ToolGrantProjection{}, fmt.Errorf("org id %s does not match resource key %s", orgID, resourceKey)
	}
	capabilityVersionID, err := msgConn.ParseUUID("capability_version_id", payload.GetCapabilityVersionId())
	if err != nil {
		return model.ToolGrantProjection{}, err
	}
	projection := model.ToolGrantProjection{
		OrgID:               orgID,
		CapabilityVersionID: capabilityVersionID,
		Status:              strings.TrimSpace(payload.GetStatus()),
	}
	if projection.Status != catalogProjectionStatusActive && projection.Status != catalogProjectionStatusRevoked {
		return model.ToolGrantProjection{}, fmt.Errorf("grant status is invalid")
	}
	return projection, nil
}

func credentialBindingProjectionFromEvent(resourceKey uuid.UUID, payload *toolcatalogpb.ToolCredentialBindingUpdatedEvent) (model.ToolCredentialBindingProjection, error) {
	log.Trace("credentialBindingProjectionFromEvent")

	if resourceKey == uuid.Nil {
		return model.ToolCredentialBindingProjection{}, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.ToolCredentialBindingProjection{}, fmt.Errorf("tool credential binding updated payload is required")
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.ToolCredentialBindingProjection{}, err
	}
	if orgID != resourceKey {
		return model.ToolCredentialBindingProjection{}, fmt.Errorf("org id %s does not match resource key %s", orgID, resourceKey)
	}
	projection := model.ToolCredentialBindingProjection{
		OrgID:         orgID,
		CapabilityID:  strings.TrimSpace(payload.GetCapabilityId()),
		CredentialRef: strings.TrimSpace(payload.GetCredentialRef()),
	}
	if projection.CapabilityID == "" || projection.CredentialRef == "" {
		return model.ToolCredentialBindingProjection{}, fmt.Errorf("credential binding payload is incomplete")
	}
	return projection, nil
}

const (
	catalogProjectionStatusActive  = "ACTIVE"
	catalogProjectionStatusRevoked = "REVOKED"
)

func validateCapabilityProjection(projection model.ToolCapabilityProjection) error {
	log.Trace("validateCapabilityProjection")

	if projection.CapabilityID == "" ||
		projection.Version == "" ||
		projection.ToolName == "" ||
		projection.Description == "" ||
		len(projection.ParametersJSON) == 0 ||
		projection.ImplementationVersion == "" ||
		len(projection.EgressHosts) == 0 ||
		projection.TimeoutMs <= 0 ||
		projection.MaxResponseBytes <= 0 ||
		projection.LifecycleStatus != catalogProjectionStatusActive ||
		projection.ContentHash == "" {
		return fmt.Errorf("tool capability payload is incomplete")
	}
	if projection.ExecutorKind == model.ToolExecutorKindMCP && projection.MCPServerEndpoint == "" {
		return fmt.Errorf("mcp capability endpoint is required")
	}
	if projection.ExecutorKind != model.ToolExecutorKindMCP && projection.MCPServerEndpoint != "" {
		return fmt.Errorf("mcp capability endpoint is only valid for mcp tools")
	}
	return nil
}

func cleanStrings(values []string) []string {
	log.Trace("cleanStrings")

	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
