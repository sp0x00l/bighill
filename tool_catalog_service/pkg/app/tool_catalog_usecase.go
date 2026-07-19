package app

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"lib/shared_lib/ctxutil"
	"lib/shared_lib/serializer"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type toolCatalogUsecase struct {
	repository ToolCatalogRepository
	unitOfWork ToolCatalogUnitOfWork
	events     ToolCatalogEventBuilder
	encoder    *serializer.Encoder
	verifier   CapabilityManifestVerifier
}

type ToolCatalogUsecaseOption func(*toolCatalogUsecase)

func WithCapabilityManifestVerifier(verifier CapabilityManifestVerifier) ToolCatalogUsecaseOption {
	log.Trace("WithCapabilityManifestVerifier")

	return func(u *toolCatalogUsecase) {
		u.verifier = verifier
	}
}

func NewToolCatalogUsecase(repository ToolCatalogRepository, unitOfWork ToolCatalogUnitOfWork, events ToolCatalogEventBuilder, encoder *serializer.Encoder, opts ...ToolCatalogUsecaseOption) ToolCatalogUsecase {
	log.Trace("NewToolCatalogUsecase")

	if encoder == nil {
		log.Fatal("tool catalog serializer is required")
	}
	u := &toolCatalogUsecase{
		repository: repository,
		unitOfWork: unitOfWork,
		events:     events,
		encoder:    encoder,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
}

func (u *toolCatalogUsecase) PublishCapability(ctx context.Context, command model.PublishCapabilityCommand) (*model.ToolCapabilityVersion, error) {
	log.Trace("toolCatalogUsecase PublishCapability")

	if u.verifier == nil {
		return nil, domain.ErrToolCatalogValidation.Extend("capability manifest verifier is not configured")
	}
	if err := u.verifier.VerifyCapabilityManifest(ctx, command); err != nil {
		return nil, err
	}
	contentHash, err := u.capabilityContentHash(command)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	capability := &model.ToolCapabilityVersion{
		CapabilityVersionID:   uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{command.CapabilityID, command.Version, contentHash}, ":"))),
		CapabilityID:          strings.TrimSpace(command.CapabilityID),
		Version:               strings.TrimSpace(command.Version),
		ToolName:              strings.TrimSpace(command.ToolName),
		Kind:                  command.Kind,
		MCPServerEndpoint:     strings.TrimSpace(command.MCPServerEndpoint),
		Description:           strings.TrimSpace(command.Description),
		ParametersJSON:        append([]byte(nil), command.ParametersJSON...),
		ImplementationVersion: capabilityImplementationVersion(command, contentHash),
		EgressHosts:           sortedStrings(command.EgressHosts),
		TimeoutMs:             command.TimeoutMs,
		MaxResponseBytes:      command.MaxResponseBytes,
		CredentialName:        strings.TrimSpace(command.CredentialName),
		CredentialRequired:    command.CredentialRequired,
		LifecycleStatus:       model.CapabilityLifecycleStatusActive,
		ContentHash:           contentHash,
		PublishedByUserID:     command.UserID,
		PublishedAt:           now,
	}
	var saved *model.ToolCapabilityVersion
	writeCtx := ctxutil.WithSystemContext(ctx)
	if err := u.unitOfWork.Do(writeCtx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var err error
		saved, err = u.repository.UpsertCapabilityVersion(ctx, tx, capability)
		if err != nil {
			return err
		}
		return enqueue(u.events.CapabilityUpdatedMessage(saved))
	}); err != nil {
		return nil, err
	}
	return saved, nil
}

func (u *toolCatalogUsecase) GrantCapability(ctx context.Context, command model.GrantCapabilityCommand) (*model.TenantCapabilityGrant, error) {
	log.Trace("toolCatalogUsecase GrantCapability")

	if command.OrgID == uuid.Nil || command.UserID == uuid.Nil || command.CapabilityVersionID == uuid.Nil {
		return nil, domain.ErrToolCatalogValidation.Extend("org_id, user_id, and capability_version_id are required")
	}
	if _, err := u.repository.ReadCapabilityVersion(ctx, command.CapabilityVersionID); err != nil {
		return nil, err
	}
	grant := &model.TenantCapabilityGrant{
		GrantID:             uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{command.OrgID.String(), command.CapabilityVersionID.String()}, ":"))),
		OrgID:               command.OrgID,
		CapabilityVersionID: command.CapabilityVersionID,
		Status:              model.TenantGrantStatusActive,
		GrantedByUserID:     command.UserID,
		GrantedAt:           time.Now().UTC(),
	}
	var saved *model.TenantCapabilityGrant
	writeCtx := ctxutil.WithActorOrg(ctx, command.UserID, command.OrgID)
	if err := u.unitOfWork.Do(writeCtx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var err error
		saved, err = u.repository.UpsertTenantGrant(ctx, tx, grant)
		if err != nil {
			return err
		}
		return enqueue(u.events.GrantUpdatedMessage(saved))
	}); err != nil {
		return nil, err
	}
	return saved, nil
}

func (u *toolCatalogUsecase) BindCredential(ctx context.Context, command model.BindCredentialCommand) (*model.ToolCredentialBinding, error) {
	log.Trace("toolCatalogUsecase BindCredential")

	if command.OrgID == uuid.Nil || command.UserID == uuid.Nil || strings.TrimSpace(command.CapabilityID) == "" || strings.TrimSpace(command.CredentialRef) == "" {
		return nil, domain.ErrToolCatalogValidation.Extend("org_id, user_id, capability_id, and credential_ref are required")
	}
	if _, err := u.repository.ReadCapabilityByCapabilityID(ctx, command.CapabilityID); err != nil {
		return nil, err
	}
	binding := &model.ToolCredentialBinding{
		BindingID:     uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{command.OrgID.String(), strings.TrimSpace(command.CapabilityID)}, ":"))),
		OrgID:         command.OrgID,
		CapabilityID:  strings.TrimSpace(command.CapabilityID),
		CredentialRef: strings.TrimSpace(command.CredentialRef),
		BoundByUserID: command.UserID,
		BoundAt:       time.Now().UTC(),
	}
	var saved *model.ToolCredentialBinding
	writeCtx := ctxutil.WithActorOrg(ctx, command.UserID, command.OrgID)
	if err := u.unitOfWork.Do(writeCtx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var err error
		saved, err = u.repository.UpsertCredentialBinding(ctx, tx, binding)
		if err != nil {
			return err
		}
		return enqueue(u.events.CredentialBindingUpdatedMessage(saved))
	}); err != nil {
		return nil, err
	}
	return saved, nil
}

func (u *toolCatalogUsecase) ReadCapabilityVersion(ctx context.Context, capabilityVersionID uuid.UUID) (*model.ToolCapabilityVersion, error) {
	log.Trace("toolCatalogUsecase ReadCapabilityVersion")

	return u.repository.ReadCapabilityVersion(ctx, capabilityVersionID)
}

func (u *toolCatalogUsecase) capabilityContentHash(command model.PublishCapabilityCommand) (string, error) {
	log.Trace("toolCatalogUsecase capabilityContentHash")

	manifest := map[string]any{
		"capability_id":       strings.TrimSpace(command.CapabilityID),
		"version":             strings.TrimSpace(command.Version),
		"tool_name":           strings.TrimSpace(command.ToolName),
		"kind":                command.Kind.String(),
		"mcp_server_endpoint": strings.TrimSpace(command.MCPServerEndpoint),
		"description":         strings.TrimSpace(command.Description),
		"parameters_json":     string(command.ParametersJSON),
		"egress_hosts":        sortedStrings(command.EgressHosts),
		"timeout_ms":          command.TimeoutMs,
		"max_response_bytes":  command.MaxResponseBytes,
		"credential_name":     strings.TrimSpace(command.CredentialName),
		"credential_required": command.CredentialRequired,
	}
	payload, err := u.encoder.Serialize(manifest)
	if err != nil {
		return "", domain.ErrToolCatalogValidation.Extend("capability manifest is not serializable")
	}
	return "sha256:" + userevents.SHA256String(string(payload)), nil
}

func capabilityImplementationVersion(command model.PublishCapabilityCommand, contentHash string) string {
	log.Trace("capabilityImplementationVersion")

	if command.Kind == model.CapabilityKindMCP {
		schemaHash := userevents.SHA256String(strings.TrimSpace(command.ToolName) + ":" + string(command.ParametersJSON))
		return fmt.Sprintf("mcp:%s:%s", endpointHost(command.MCPServerEndpoint), schemaHash)
	}
	return fmt.Sprintf("%s:%s", strings.ToLower(command.Kind.String()), contentHash)
}

func endpointHost(endpoint string) string {
	log.Trace("endpointHost")

	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func sortedStrings(values []string) []string {
	log.Trace("sortedStrings")

	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	sort.Strings(result)
	return result
}
