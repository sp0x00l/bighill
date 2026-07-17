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

	toolBindings, _ := jsonBytes(agentToolBindingRecords(spec.ToolBindings))
	budgets, _ := jsonBytes(newAgentBudgetRecord(spec.Budgets))
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
	record.ToolBindings = agentToolBindingsFromRecord([]byte(toolBindings))
	record.RetrievalConfig = []byte(retrievalConfig)
	record.Budgets = agentBudgetFromRecord([]byte(budgets))
	record.StopConditions = []byte(stopConditions)
	record.Guardrails = []byte(guardrails)
	parsedStatus, err := model.ToAgentSpecStatus(status)
	if err != nil {
		return nil, fmt.Errorf("parse agent spec status: %w", err)
	}
	record.Status = parsedStatus
	return record, nil
}

type agentToolBindingRecord struct {
	Name       string          `json:"name"`
	Required   bool            `json:"required"`
	ToolChoice string          `json:"tool_choice,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`
}

type agentBudgetRecord struct {
	MaxSteps int `json:"max_steps"`
	Token    int `json:"token"`
	WallMs   int `json:"wall_ms"`
}

func agentToolBindingRecords(bindings []model.ToolBinding) []agentToolBindingRecord {
	log.Trace("agentToolBindingRecords")

	records := make([]agentToolBindingRecord, 0, len(bindings))
	for _, binding := range bindings {
		records = append(records, agentToolBindingRecord{
			Name:       binding.Name,
			Required:   binding.Required,
			ToolChoice: binding.ToolChoice,
			Config:     binding.Config,
		})
	}
	return records
}

func agentToolBindingsFromRecord(raw []byte) []model.ToolBinding {
	log.Trace("agentToolBindingsFromRecord")

	var records []agentToolBindingRecord
	if err := unmarshalJSON(raw, &records); err != nil {
		return nil
	}
	bindings := make([]model.ToolBinding, 0, len(records))
	for _, record := range records {
		bindings = append(bindings, model.ToolBinding{
			Name:       record.Name,
			Required:   record.Required,
			ToolChoice: record.ToolChoice,
			Config:     record.Config,
		})
	}
	return bindings
}

func newAgentBudgetRecord(budget model.AgentBudgets) agentBudgetRecord {
	log.Trace("newAgentBudgetRecord")

	return agentBudgetRecord{
		MaxSteps: budget.MaxSteps,
		Token:    budget.Token,
		WallMs:   budget.WallMs,
	}
}

func agentBudgetFromRecord(raw []byte) model.AgentBudgets {
	log.Trace("agentBudgetFromRecord")

	var record agentBudgetRecord
	if err := unmarshalJSON(raw, &record); err != nil {
		return model.AgentBudgets{}
	}
	return model.AgentBudgets{
		MaxSteps: record.MaxSteps,
		Token:    record.Token,
		WallMs:   record.WallMs,
	}
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
		effective_base_id, supports_chat, supports_tool_calls,
		supports_system_prompt
	) VALUES (
		@effective_base_id, @supports_chat, @supports_tool_calls,
		@supports_system_prompt
	)
	ON CONFLICT (effective_base_id) DO UPDATE SET
		supports_chat = EXCLUDED.supports_chat,
		supports_tool_calls = EXCLUDED.supports_tool_calls,
		supports_system_prompt = EXCLUDED.supports_system_prompt
	RETURNING capability_report_id::text,
		effective_base_id,
		supports_chat,
		supports_tool_calls,
		supports_system_prompt,
		created_at`
	record, err := scanCapabilityReport(r.Pool.QueryRow(ctx, query, capabilityReportArgs(report)))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "record capability report failed", err)
		return nil, fmt.Errorf("record capability report: %w", err)
	}
	return record, nil
}

func (r *CapabilityReportRepository) ReadCapabilityReportForEffectiveBase(ctx context.Context, effectiveBaseID string) (*model.CapabilityReport, error) {
	log.Trace("CapabilityReportRepository ReadCapabilityReportForEffectiveBase")

	query := `SELECT capability_report_id::text,
			effective_base_id,
			supports_chat,
			supports_tool_calls,
			supports_system_prompt,
			created_at
		FROM ` + r.Name + `.capability_reports
		WHERE effective_base_id = @effective_base_id
		ORDER BY created_at DESC
		LIMIT 1`
	record, err := scanCapabilityReport(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"effective_base_id": effectiveBaseID,
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
		"effective_base_id":      report.EffectiveBaseID,
		"supports_chat":          report.SupportsChat,
		"supports_tool_calls":    report.SupportsToolCalls,
		"supports_system_prompt": report.SupportsSystemPrompt,
	}
}

func scanCapabilityReport(row pgx.Row) (*model.CapabilityReport, error) {
	log.Trace("scanCapabilityReport")

	var capabilityReportID, effectiveBaseID string
	record := &model.CapabilityReport{}
	if err := row.Scan(
		&capabilityReportID,
		&effectiveBaseID,
		&record.SupportsChat,
		&record.SupportsToolCalls,
		&record.SupportsSystemPrompt,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	parsedCapabilityReportID, err := uuid.Parse(capabilityReportID)
	if err != nil {
		return nil, fmt.Errorf("parse capability report id: %w", err)
	}
	record.CapabilityReportID = parsedCapabilityReportID
	record.EffectiveBaseID = effectiveBaseID
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
