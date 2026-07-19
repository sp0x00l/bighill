package db

import (
	"context"
	"errors"
	"fmt"

	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"
	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

const (
	catalogStatusActive = "ACTIVE"
)

type ToolCatalogProjectionRepository struct {
	coreDB.Database
}

func NewToolCatalogProjectionRepository(db *coreDB.Database) *ToolCatalogProjectionRepository {
	log.Trace("NewToolCatalogProjectionRepository")

	return &ToolCatalogProjectionRepository{Database: *db}
}

func (r *ToolCatalogProjectionRepository) ApplyCapabilityProjection(ctx context.Context, projection model.ToolCapabilityProjection) error {
	log.Trace("ToolCatalogProjectionRepository ApplyCapabilityProjection")

	query := `INSERT INTO ` + r.Name + `.tool_capability_projections (
		capability_version_id, capability_id, version, tool_name, executor_kind,
		description, mcp_server_endpoint, parameters_json, implementation_version, egress_hosts,
		timeout_ms, max_response_bytes, credential_name, credential_required,
		lifecycle_status, content_hash
	) VALUES (
		@capability_version_id, @capability_id, @version, @tool_name,
		@executor_kind::tool_executor_kind_enum, @description, @mcp_server_endpoint, @parameters_json::jsonb,
		@implementation_version, @egress_hosts, @timeout_ms, @max_response_bytes,
		@credential_name, @credential_required, @lifecycle_status, @content_hash
	)
	ON CONFLICT (capability_version_id) DO UPDATE SET
		capability_id = EXCLUDED.capability_id,
		version = EXCLUDED.version,
		tool_name = EXCLUDED.tool_name,
		executor_kind = EXCLUDED.executor_kind,
		description = EXCLUDED.description,
		mcp_server_endpoint = EXCLUDED.mcp_server_endpoint,
		parameters_json = EXCLUDED.parameters_json,
		implementation_version = EXCLUDED.implementation_version,
		egress_hosts = EXCLUDED.egress_hosts,
		timeout_ms = EXCLUDED.timeout_ms,
		max_response_bytes = EXCLUDED.max_response_bytes,
		credential_name = EXCLUDED.credential_name,
		credential_required = EXCLUDED.credential_required,
		lifecycle_status = EXCLUDED.lifecycle_status,
		content_hash = EXCLUDED.content_hash,
		updated_at = now()`
	if _, err := r.Pool.Exec(ctx, query, capabilityProjectionArgs(projection)); err != nil {
		r.LogPoolStatsOnError(ctx, "apply tool capability projection failed", err)
		return fmt.Errorf("apply tool capability projection: %w", err)
	}
	return nil
}

func (r *ToolCatalogProjectionRepository) ApplyGrantProjection(ctx context.Context, projection model.ToolGrantProjection) error {
	log.Trace("ToolCatalogProjectionRepository ApplyGrantProjection")

	query := `INSERT INTO ` + r.Name + `.tool_grant_projections (
		org_id, capability_version_id, status
	) VALUES (
		@org_id, @capability_version_id, @status
	)
	ON CONFLICT (org_id, capability_version_id) DO UPDATE SET
		status = EXCLUDED.status,
		updated_at = now()`
	if _, err := r.Pool.Exec(ctxutil.WithOrgID(ctx, projection.OrgID), query, grantProjectionArgs(projection)); err != nil {
		r.LogPoolStatsOnError(ctx, "apply tool grant projection failed", err)
		return fmt.Errorf("apply tool grant projection: %w", err)
	}
	return nil
}

func (r *ToolCatalogProjectionRepository) ApplyCredentialBindingProjection(ctx context.Context, projection model.ToolCredentialBindingProjection) error {
	log.Trace("ToolCatalogProjectionRepository ApplyCredentialBindingProjection")

	query := `INSERT INTO ` + r.Name + `.tool_credential_binding_projections (
		org_id, capability_id, credential_ref
	) VALUES (
		@org_id, @capability_id, @credential_ref
	)
	ON CONFLICT (org_id, capability_id) DO UPDATE SET
		credential_ref = EXCLUDED.credential_ref,
		updated_at = now()`
	if _, err := r.Pool.Exec(ctxutil.WithOrgID(ctx, projection.OrgID), query, credentialBindingProjectionArgs(projection)); err != nil {
		r.LogPoolStatsOnError(ctx, "apply tool credential binding projection failed", err)
		return fmt.Errorf("apply tool credential binding projection: %w", err)
	}
	return nil
}

func (r *ToolCatalogProjectionRepository) ListAvailableTools(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) ([]*model.ToolDefinition, error) {
	log.Trace("ToolCatalogProjectionRepository ListAvailableTools")

	if orgID == uuid.Nil || userID == uuid.Nil {
		return []*model.ToolDefinition{}, nil
	}
	query := `SELECT c.capability_version_id, c.capability_id, c.version, c.tool_name,
		c.executor_kind::text, c.description, c.mcp_server_endpoint, c.parameters_json::text,
		c.implementation_version, c.egress_hosts, c.timeout_ms, c.max_response_bytes,
		c.credential_name, COALESCE(b.credential_ref, '')
	FROM ` + r.Name + `.tool_capability_projections c
	JOIN ` + r.Name + `.tool_grant_projections g
		ON g.capability_version_id = c.capability_version_id
		AND g.org_id = @org_id
		AND g.status = @active_status
	LEFT JOIN ` + r.Name + `.tool_credential_binding_projections b
		ON b.org_id = g.org_id
		AND b.capability_id = c.capability_id
	WHERE c.lifecycle_status = @active_status
		AND (c.credential_required = false OR b.credential_ref IS NOT NULL)
	ORDER BY c.tool_name`
	rows, err := r.Pool.Query(ctxutil.WithOrgID(ctx, orgID), query, pgx.NamedArgs{
		"org_id":        requiredUUID(orgID),
		"active_status": catalogStatusActive,
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "list projected tools failed", err)
		return nil, fmt.Errorf("list projected tools: %w", err)
	}
	defer rows.Close()
	tools := []*model.ToolDefinition{}
	for rows.Next() {
		tool, err := scanProjectedTool(rows)
		if err != nil {
			return nil, fmt.Errorf("scan projected tool: %w", err)
		}
		tools = append(tools, tool)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projected tools: %w", err)
	}
	return tools, nil
}

func (r *ToolCatalogProjectionRepository) ResolveTool(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, toolName string) (*model.ToolDefinition, error) {
	log.Trace("ToolCatalogProjectionRepository ResolveTool")

	if orgID == uuid.Nil || userID == uuid.Nil {
		return nil, domain.ErrToolDenied.Extend("actor is required")
	}
	query := `SELECT c.capability_version_id, c.capability_id, c.version, c.tool_name,
		c.executor_kind::text, c.description, c.mcp_server_endpoint, c.parameters_json::text,
		c.implementation_version, c.egress_hosts, c.timeout_ms, c.max_response_bytes,
		c.credential_name, COALESCE(b.credential_ref, '')
	FROM ` + r.Name + `.tool_capability_projections c
	JOIN ` + r.Name + `.tool_grant_projections g
		ON g.capability_version_id = c.capability_version_id
		AND g.org_id = @org_id
		AND g.status = @active_status
	LEFT JOIN ` + r.Name + `.tool_credential_binding_projections b
		ON b.org_id = g.org_id
		AND b.capability_id = c.capability_id
	WHERE c.lifecycle_status = @active_status
		AND lower(c.tool_name) = lower(@tool_name)
		AND (c.credential_required = false OR b.credential_ref IS NOT NULL)`
	row := r.Pool.QueryRow(ctxutil.WithOrgID(ctx, orgID), query, pgx.NamedArgs{
		"org_id":        requiredUUID(orgID),
		"tool_name":     toolName,
		"active_status": catalogStatusActive,
	})
	tool, err := scanProjectedTool(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrToolNotFound.Extend(toolName)
	}
	if err != nil {
		r.LogPoolStatsOnError(ctx, "resolve projected tool failed", err)
		return nil, fmt.Errorf("resolve projected tool: %w", err)
	}
	return tool, nil
}

func capabilityProjectionArgs(projection model.ToolCapabilityProjection) pgx.NamedArgs {
	log.Trace("capabilityProjectionArgs")

	return pgx.NamedArgs{
		"capability_version_id":  requiredUUID(projection.CapabilityVersionID),
		"capability_id":          projection.CapabilityID,
		"version":                projection.Version,
		"tool_name":              projection.ToolName,
		"executor_kind":          projection.ExecutorKind.String(),
		"description":            projection.Description,
		"mcp_server_endpoint":    projection.MCPServerEndpoint,
		"parameters_json":        string(projection.ParametersJSON),
		"implementation_version": projection.ImplementationVersion,
		"egress_hosts":           projection.EgressHosts,
		"timeout_ms":             projection.TimeoutMs,
		"max_response_bytes":     projection.MaxResponseBytes,
		"credential_name":        projection.CredentialName,
		"credential_required":    projection.CredentialRequired,
		"lifecycle_status":       projection.LifecycleStatus,
		"content_hash":           projection.ContentHash,
	}
}

func grantProjectionArgs(projection model.ToolGrantProjection) pgx.NamedArgs {
	log.Trace("grantProjectionArgs")

	return pgx.NamedArgs{
		"org_id":                requiredUUID(projection.OrgID),
		"capability_version_id": requiredUUID(projection.CapabilityVersionID),
		"status":                projection.Status,
	}
}

func credentialBindingProjectionArgs(projection model.ToolCredentialBindingProjection) pgx.NamedArgs {
	log.Trace("credentialBindingProjectionArgs")

	return pgx.NamedArgs{
		"org_id":         requiredUUID(projection.OrgID),
		"capability_id":  projection.CapabilityID,
		"credential_ref": projection.CredentialRef,
	}
}

func scanProjectedTool(row pgx.Row) (*model.ToolDefinition, error) {
	log.Trace("scanProjectedTool")

	tool := &model.ToolDefinition{Enabled: true}
	var kindRaw string
	var parametersJSON string
	if err := row.Scan(
		&tool.CapabilityVersionID,
		&tool.CapabilityID,
		&tool.CapabilityVersion,
		&tool.Name,
		&kindRaw,
		&tool.Description,
		&tool.MCPServerEndpoint,
		&parametersJSON,
		&tool.ImplementationVersion,
		&tool.EgressHosts,
		&tool.TimeoutMs,
		&tool.MaxResponseBytes,
		&tool.CredentialName,
		&tool.CredentialRef,
	); err != nil {
		return nil, err
	}
	kind, err := model.ToToolExecutorKind(kindRaw)
	if err != nil {
		return nil, err
	}
	tool.ExecutorKind = kind
	tool.ParametersJSON = []byte(parametersJSON)
	return tool, nil
}
