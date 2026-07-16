package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type AgentSpecRepository struct {
	coreDB.Database
}

func NewAgentSpecRepository(db *coreDB.Database) *AgentSpecRepository {
	log.Trace("NewAgentSpecRepository")

	return &AgentSpecRepository{Database: *db}
}

func (r *AgentSpecRepository) UpsertAgentSpec(ctx context.Context, spec *model.AgentSpec) (*model.AgentSpec, error) {
	log.Trace("AgentSpecRepository UpsertAgentSpec")

	query := `INSERT INTO ` + r.Name + `.agent_specs (
		org_id, agent_lineage, system_prompt, source_yaml, canonical_json, schema_version, content_hash,
		validation_report, model_id, tool_bindings, retrieval_config,
		budgets, stop_conditions, guardrails, status
	) VALUES (
		@org_id, @agent_lineage, @system_prompt, @source_yaml, @canonical_json::jsonb, @schema_version, @content_hash,
		@validation_report, @model_id, @tool_bindings::jsonb, @retrieval_config::jsonb,
		@budgets::jsonb, @stop_conditions::jsonb, @guardrails::jsonb, @status::agent_spec_status_enum
	)
	ON CONFLICT (org_id, content_hash) DO UPDATE SET
		validation_report = EXCLUDED.validation_report,
		status = EXCLUDED.status
	RETURNING ` + agentSpecColumns()

	return scanAgentSpec(r.Pool.QueryRow(ctx, query, agentSpecArgs(spec)))
}

func (r *AgentSpecRepository) ReadAgentSpecByHash(ctx context.Context, orgID uuid.UUID, contentHash string) (*model.AgentSpec, error) {
	log.Trace("AgentSpecRepository ReadAgentSpecByHash")

	query := `SELECT ` + agentSpecColumns() + `
		FROM ` + r.Name + `.agent_specs
		WHERE org_id = @org_id AND content_hash = @content_hash`
	record, err := scanAgentSpec(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":       nullableUUID(orgID),
		"content_hash": contentHash,
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		return nil, fmt.Errorf("read agent spec: %w", err)
	}
	return record, nil
}

func agentSpecColumns() string {
	log.Trace("agentSpecColumns")

	return `agent_spec_id::text,
		org_id::text,
		agent_lineage,
		system_prompt,
		source_yaml,
		canonical_json::text,
		schema_version,
		content_hash,
		validation_report,
		model_id::text,
		tool_bindings::text,
		retrieval_config::text,
		budgets::text,
		stop_conditions::text,
		guardrails::text,
		status::text,
		created_at`
}

func agentSpecArgs(spec *model.AgentSpec) pgx.NamedArgs {
	log.Trace("agentSpecArgs")

	toolBindings, _ := jsonBytes(spec.ToolBindings)
	budgets, _ := jsonBytes(spec.Budgets)
	return pgx.NamedArgs{
		"org_id":            nullableUUID(spec.OrgID),
		"agent_lineage":     spec.AgentLineage,
		"system_prompt":     spec.SystemPrompt,
		"source_yaml":       spec.SourceYAML,
		"canonical_json":    string(spec.CanonicalJSON),
		"schema_version":    spec.SchemaVersion,
		"content_hash":      spec.ContentHash,
		"validation_report": spec.ValidationReport,
		"model_id":          nullableUUID(spec.ModelID),
		"tool_bindings":     string(toolBindings),
		"retrieval_config":  string(spec.RetrievalConfig),
		"budgets":           string(budgets),
		"stop_conditions":   string(spec.StopConditions),
		"guardrails":        string(spec.Guardrails),
		"status":            spec.Status.String(),
	}
}

func scanAgentSpec(row pgx.Row) (*model.AgentSpec, error) {
	log.Trace("scanAgentSpec")

	var agentSpecID, orgID, canonicalJSON, modelID, toolBindings, retrievalConfig, budgets, stopConditions, guardrails, status string
	record := &model.AgentSpec{}
	if err := row.Scan(
		&agentSpecID,
		&orgID,
		&record.AgentLineage,
		&record.SystemPrompt,
		&record.SourceYAML,
		&canonicalJSON,
		&record.SchemaVersion,
		&record.ContentHash,
		&record.ValidationReport,
		&modelID,
		&toolBindings,
		&retrievalConfig,
		&budgets,
		&stopConditions,
		&guardrails,
		&status,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	parsedAgentSpecID, err := uuid.Parse(agentSpecID)
	if err != nil {
		return nil, fmt.Errorf("parse agent spec id: %w", err)
	}
	parsedOrgID, err := uuid.Parse(orgID)
	if err != nil {
		return nil, fmt.Errorf("parse agent spec org id: %w", err)
	}
	parsedModelID, err := uuid.Parse(modelID)
	if err != nil {
		return nil, fmt.Errorf("parse agent spec model id: %w", err)
	}
	record.AgentSpecID = parsedAgentSpecID
	record.OrgID = parsedOrgID
	record.CanonicalJSON = []byte(canonicalJSON)
	record.ModelID = parsedModelID
	_ = unmarshalJSON([]byte(toolBindings), &record.ToolBindings)
	record.RetrievalConfig = []byte(retrievalConfig)
	_ = unmarshalJSON([]byte(budgets), &record.Budgets)
	record.StopConditions = []byte(stopConditions)
	record.Guardrails = []byte(guardrails)
	parsedStatus, err := model.ToAgentSpecStatus(status)
	if err != nil {
		return nil, fmt.Errorf("parse agent spec status: %w", err)
	}
	record.Status = parsedStatus
	return record, nil
}

type CapabilityReportRepository struct {
	coreDB.Database
}

func NewCapabilityReportRepository(db *coreDB.Database) *CapabilityReportRepository {
	log.Trace("NewCapabilityReportRepository")

	return &CapabilityReportRepository{Database: *db}
}

func (r *CapabilityReportRepository) RecordCapabilityReport(ctx context.Context, report *model.CapabilityReport) (*model.CapabilityReport, error) {
	log.Trace("CapabilityReportRepository RecordCapabilityReport")

	query := `INSERT INTO ` + r.Name + `.capability_reports (
		org_id, model_id, supports_chat, supports_tool_calls,
		supports_system_prompt, context_window_tokens, max_output_tokens
	) VALUES (
		@org_id, @model_id, @supports_chat, @supports_tool_calls,
		@supports_system_prompt, @context_window_tokens, @max_output_tokens
	)
	ON CONFLICT (org_id, model_id) DO UPDATE SET
		supports_chat = EXCLUDED.supports_chat,
		supports_tool_calls = EXCLUDED.supports_tool_calls,
		supports_system_prompt = EXCLUDED.supports_system_prompt,
		context_window_tokens = EXCLUDED.context_window_tokens,
		max_output_tokens = EXCLUDED.max_output_tokens
	RETURNING capability_report_id::text,
		org_id::text,
		COALESCE(model_id::text, ''),
		supports_chat,
		supports_tool_calls,
		supports_system_prompt,
		context_window_tokens,
		max_output_tokens,
		created_at`
	record, err := scanCapabilityReport(r.Pool.QueryRow(ctx, query, capabilityReportArgs(report)))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "record capability report failed", err)
		return nil, fmt.Errorf("record capability report: %w", err)
	}
	return record, nil
}

func (r *CapabilityReportRepository) ReadCapabilityReportForModel(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.CapabilityReport, error) {
	log.Trace("CapabilityReportRepository ReadCapabilityReportForModel")

	query := `SELECT capability_report_id::text,
			org_id::text,
			COALESCE(model_id::text, ''),
			supports_chat,
			supports_tool_calls,
			supports_system_prompt,
			context_window_tokens,
			max_output_tokens,
			created_at
		FROM ` + r.Name + `.capability_reports
		WHERE org_id = @org_id AND model_id = @model_id
		ORDER BY created_at DESC
		LIMIT 1`
	record, err := scanCapabilityReport(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id": nullableUUID(modelID),
		"org_id":   nullableUUID(orgID),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotReady.Extend("model capability report is required")
		}
		return nil, fmt.Errorf("read capability report: %w", err)
	}
	return record, nil
}

func capabilityReportArgs(report *model.CapabilityReport) pgx.NamedArgs {
	log.Trace("capabilityReportArgs")

	return pgx.NamedArgs{
		"org_id":                 nullableUUID(report.OrgID),
		"model_id":               nullableUUID(report.ModelID),
		"supports_chat":          report.SupportsChat,
		"supports_tool_calls":    report.SupportsToolCalls,
		"supports_system_prompt": report.SupportsSystemPrompt,
		"context_window_tokens":  report.ContextWindowTokens,
		"max_output_tokens":      report.MaxOutputTokens,
	}
}

func scanCapabilityReport(row pgx.Row) (*model.CapabilityReport, error) {
	log.Trace("scanCapabilityReport")

	var capabilityReportID, orgID, modelID string
	record := &model.CapabilityReport{}
	if err := row.Scan(
		&capabilityReportID,
		&orgID,
		&modelID,
		&record.SupportsChat,
		&record.SupportsToolCalls,
		&record.SupportsSystemPrompt,
		&record.ContextWindowTokens,
		&record.MaxOutputTokens,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	parsedCapabilityReportID, err := uuid.Parse(capabilityReportID)
	if err != nil {
		return nil, fmt.Errorf("parse capability report id: %w", err)
	}
	parsedOrgID, err := uuid.Parse(orgID)
	if err != nil {
		return nil, fmt.Errorf("parse capability report org id: %w", err)
	}
	record.CapabilityReportID = parsedCapabilityReportID
	record.OrgID = parsedOrgID
	if modelID != "" {
		parsedModelID, err := uuid.Parse(modelID)
		if err != nil {
			return nil, fmt.Errorf("parse capability report model id: %w", err)
		}
		record.ModelID = parsedModelID
	}
	return record, nil
}

func jsonBytes(value any) ([]byte, error) {
	log.Trace("jsonBytes")

	return json.Marshal(value)
}

func unmarshalJSON(data []byte, value any) error {
	log.Trace("unmarshalJSON")

	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, value)
}
