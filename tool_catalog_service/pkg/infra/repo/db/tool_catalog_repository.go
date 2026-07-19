package db

import (
	"context"
	"errors"
	"fmt"

	coreDB "lib/shared_lib/db"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type ToolCatalogRepository struct {
	coreDB.Database
}

func NewToolCatalogRepository(db *coreDB.Database) *ToolCatalogRepository {
	log.Trace("NewToolCatalogRepository")

	return &ToolCatalogRepository{Database: *db}
}

func (r *ToolCatalogRepository) UpsertCapabilityVersion(ctx context.Context, tx pgx.Tx, capability *model.ToolCapabilityVersion) (*model.ToolCapabilityVersion, error) {
	log.Trace("ToolCatalogRepository UpsertCapabilityVersion")

	query := `INSERT INTO ` + r.Name + `.tool_capability_versions (
		capability_version_id, capability_id, version, tool_name, kind,
		mcp_server_endpoint, description, parameters_json, implementation_version, egress_hosts,
		timeout_ms, max_response_bytes, credential_name, credential_required,
		lifecycle_status, content_hash, published_by_user_id, published_at
	) VALUES (
		@capability_version_id, @capability_id, @version, @tool_name,
		@kind::` + r.Name + `.tool_capability_kind_enum, @mcp_server_endpoint, @description, @parameters_json::jsonb,
		@implementation_version, @egress_hosts, @timeout_ms, @max_response_bytes,
		@credential_name, @credential_required,
		@lifecycle_status::` + r.Name + `.tool_capability_lifecycle_status_enum,
		@content_hash, @published_by_user_id, @published_at
	)
	ON CONFLICT (capability_id, version) DO UPDATE SET
		tool_name = EXCLUDED.tool_name,
		kind = EXCLUDED.kind,
		mcp_server_endpoint = EXCLUDED.mcp_server_endpoint,
		description = EXCLUDED.description,
		parameters_json = EXCLUDED.parameters_json,
		implementation_version = EXCLUDED.implementation_version,
		egress_hosts = EXCLUDED.egress_hosts,
		timeout_ms = EXCLUDED.timeout_ms,
		max_response_bytes = EXCLUDED.max_response_bytes,
		credential_name = EXCLUDED.credential_name,
		credential_required = EXCLUDED.credential_required,
		lifecycle_status = EXCLUDED.lifecycle_status,
		content_hash = EXCLUDED.content_hash
	RETURNING capability_version_id, capability_id, version, tool_name, kind::text,
		mcp_server_endpoint, description, parameters_json::text, implementation_version, egress_hosts,
		timeout_ms, max_response_bytes, credential_name, credential_required,
		lifecycle_status::text, content_hash, published_by_user_id, published_at`
	row := tx.QueryRow(ctx, query, capabilityArgs(capability))
	saved, err := scanCapability(row)
	if err != nil {
		return nil, fmt.Errorf("record tool capability version: %w", err)
	}
	return saved, nil
}

func (r *ToolCatalogRepository) ReadCapabilityVersion(ctx context.Context, capabilityVersionID uuid.UUID) (*model.ToolCapabilityVersion, error) {
	log.Trace("ToolCatalogRepository ReadCapabilityVersion")

	query := `SELECT capability_version_id, capability_id, version, tool_name, kind::text,
		mcp_server_endpoint, description, parameters_json::text, implementation_version, egress_hosts,
		timeout_ms, max_response_bytes, credential_name, credential_required,
		lifecycle_status::text, content_hash, published_by_user_id, published_at
	FROM ` + r.Name + `.tool_capability_versions
	WHERE capability_version_id = @capability_version_id`
	row := r.Pool.QueryRow(ctx, query, pgx.NamedArgs{"capability_version_id": requiredUUID(capabilityVersionID)})
	capability, err := scanCapability(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrCapabilityNotFound.Extend(capabilityVersionID.String())
	}
	if err != nil {
		r.LogPoolStatsOnError(ctx, "read tool capability version failed", err)
		return nil, fmt.Errorf("read tool capability version: %w", err)
	}
	return capability, nil
}

func (r *ToolCatalogRepository) ReadCapabilityByCapabilityID(ctx context.Context, capabilityID string) (*model.ToolCapabilityVersion, error) {
	log.Trace("ToolCatalogRepository ReadCapabilityByCapabilityID")

	query := `SELECT capability_version_id, capability_id, version, tool_name, kind::text,
		mcp_server_endpoint, description, parameters_json::text, implementation_version, egress_hosts,
		timeout_ms, max_response_bytes, credential_name, credential_required,
		lifecycle_status::text, content_hash, published_by_user_id, published_at
	FROM ` + r.Name + `.tool_capability_versions
	WHERE capability_id = @capability_id
	ORDER BY published_at DESC
	LIMIT 1`
	row := r.Pool.QueryRow(ctx, query, pgx.NamedArgs{"capability_id": capabilityID})
	capability, err := scanCapability(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrCapabilityNotFound.Extend(capabilityID)
	}
	if err != nil {
		r.LogPoolStatsOnError(ctx, "read tool capability by capability id failed", err)
		return nil, fmt.Errorf("read tool capability by capability id: %w", err)
	}
	return capability, nil
}

func (r *ToolCatalogRepository) UpsertTenantGrant(ctx context.Context, tx pgx.Tx, grant *model.TenantCapabilityGrant) (*model.TenantCapabilityGrant, error) {
	log.Trace("ToolCatalogRepository UpsertTenantGrant")

	query := `INSERT INTO ` + r.Name + `.tenant_capability_grants (
		grant_id, org_id, capability_version_id, status, granted_by_user_id, granted_at
	) VALUES (
		@grant_id, @org_id, @capability_version_id,
		@status::` + r.Name + `.tenant_capability_grant_status_enum,
		@granted_by_user_id, @granted_at
	)
	ON CONFLICT (org_id, capability_version_id) DO UPDATE SET
		status = EXCLUDED.status,
		granted_by_user_id = EXCLUDED.granted_by_user_id,
		granted_at = EXCLUDED.granted_at
	RETURNING grant_id, org_id, capability_version_id, status::text, granted_by_user_id, granted_at`
	row := tx.QueryRow(ctx, query, grantArgs(grant))
	saved, err := scanGrant(row)
	if err != nil {
		return nil, fmt.Errorf("record tenant capability grant: %w", err)
	}
	return saved, nil
}

func (r *ToolCatalogRepository) UpsertCredentialBinding(ctx context.Context, tx pgx.Tx, binding *model.ToolCredentialBinding) (*model.ToolCredentialBinding, error) {
	log.Trace("ToolCatalogRepository UpsertCredentialBinding")

	query := `INSERT INTO ` + r.Name + `.tool_credential_bindings (
		binding_id, org_id, capability_id, credential_ref, bound_by_user_id, bound_at
	) VALUES (
		@binding_id, @org_id, @capability_id, @credential_ref, @bound_by_user_id, @bound_at
	)
	ON CONFLICT (org_id, capability_id) DO UPDATE SET
		credential_ref = EXCLUDED.credential_ref,
		bound_by_user_id = EXCLUDED.bound_by_user_id,
		bound_at = EXCLUDED.bound_at
	RETURNING binding_id, org_id, capability_id, credential_ref, bound_by_user_id, bound_at`
	row := tx.QueryRow(ctx, query, credentialBindingArgs(binding))
	saved, err := scanCredentialBinding(row)
	if err != nil {
		return nil, fmt.Errorf("record tool credential binding: %w", err)
	}
	return saved, nil
}

func capabilityArgs(capability *model.ToolCapabilityVersion) pgx.NamedArgs {
	log.Trace("capabilityArgs")

	return pgx.NamedArgs{
		"capability_version_id":  requiredUUID(capability.CapabilityVersionID),
		"capability_id":          capability.CapabilityID,
		"version":                capability.Version,
		"tool_name":              capability.ToolName,
		"kind":                   capability.Kind.String(),
		"mcp_server_endpoint":    capability.MCPServerEndpoint,
		"description":            capability.Description,
		"parameters_json":        string(capability.ParametersJSON),
		"implementation_version": capability.ImplementationVersion,
		"egress_hosts":           capability.EgressHosts,
		"timeout_ms":             capability.TimeoutMs,
		"max_response_bytes":     capability.MaxResponseBytes,
		"credential_name":        capability.CredentialName,
		"credential_required":    capability.CredentialRequired,
		"lifecycle_status":       capability.LifecycleStatus.String(),
		"content_hash":           capability.ContentHash,
		"published_by_user_id":   requiredUUID(capability.PublishedByUserID),
		"published_at":           capability.PublishedAt,
	}
}

func grantArgs(grant *model.TenantCapabilityGrant) pgx.NamedArgs {
	log.Trace("grantArgs")

	return pgx.NamedArgs{
		"grant_id":              requiredUUID(grant.GrantID),
		"org_id":                requiredUUID(grant.OrgID),
		"capability_version_id": requiredUUID(grant.CapabilityVersionID),
		"status":                grant.Status.String(),
		"granted_by_user_id":    requiredUUID(grant.GrantedByUserID),
		"granted_at":            grant.GrantedAt,
	}
}

func credentialBindingArgs(binding *model.ToolCredentialBinding) pgx.NamedArgs {
	log.Trace("credentialBindingArgs")

	return pgx.NamedArgs{
		"binding_id":       requiredUUID(binding.BindingID),
		"org_id":           requiredUUID(binding.OrgID),
		"capability_id":    binding.CapabilityID,
		"credential_ref":   binding.CredentialRef,
		"bound_by_user_id": requiredUUID(binding.BoundByUserID),
		"bound_at":         binding.BoundAt,
	}
}

func scanCapability(row pgx.Row) (*model.ToolCapabilityVersion, error) {
	log.Trace("scanCapability")

	record := &model.ToolCapabilityVersion{}
	var kindRaw, statusRaw, parametersJSON string
	if err := row.Scan(
		&record.CapabilityVersionID,
		&record.CapabilityID,
		&record.Version,
		&record.ToolName,
		&kindRaw,
		&record.MCPServerEndpoint,
		&record.Description,
		&parametersJSON,
		&record.ImplementationVersion,
		&record.EgressHosts,
		&record.TimeoutMs,
		&record.MaxResponseBytes,
		&record.CredentialName,
		&record.CredentialRequired,
		&statusRaw,
		&record.ContentHash,
		&record.PublishedByUserID,
		&record.PublishedAt,
	); err != nil {
		return nil, err
	}
	kind, err := model.ToCapabilityKind(kindRaw)
	if err != nil {
		return nil, err
	}
	status, err := model.ToCapabilityLifecycleStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	record.Kind = kind
	record.LifecycleStatus = status
	record.ParametersJSON = []byte(parametersJSON)
	return record, nil
}

func scanGrant(row pgx.Row) (*model.TenantCapabilityGrant, error) {
	log.Trace("scanGrant")

	record := &model.TenantCapabilityGrant{}
	var statusRaw string
	if err := row.Scan(
		&record.GrantID,
		&record.OrgID,
		&record.CapabilityVersionID,
		&statusRaw,
		&record.GrantedByUserID,
		&record.GrantedAt,
	); err != nil {
		return nil, err
	}
	status, err := model.ToTenantGrantStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	record.Status = status
	return record, nil
}

func scanCredentialBinding(row pgx.Row) (*model.ToolCredentialBinding, error) {
	log.Trace("scanCredentialBinding")

	record := &model.ToolCredentialBinding{}
	if err := row.Scan(
		&record.BindingID,
		&record.OrgID,
		&record.CapabilityID,
		&record.CredentialRef,
		&record.BoundByUserID,
		&record.BoundAt,
	); err != nil {
		return nil, err
	}
	return record, nil
}

func requiredUUID(value uuid.UUID) pgtype.UUID {
	log.Trace("requiredUUID")

	return pgtype.UUID{Bytes: value, Valid: value != uuid.Nil}
}
