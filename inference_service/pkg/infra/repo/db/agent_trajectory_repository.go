package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type AgentTrajectoryRepository struct {
	coreDB.Database
}

func NewAgentTrajectoryRepository(db *coreDB.Database) *AgentTrajectoryRepository {
	log.Trace("NewAgentTrajectoryRepository")

	return &AgentTrajectoryRepository{Database: *db}
}

func (r *AgentTrajectoryRepository) RecordAgentRun(ctx context.Context, run *model.AgentRun) (*model.AgentRun, error) {
	log.Trace("AgentTrajectoryRepository RecordAgentRun")

	if run.RunID != uuid.Nil {
		return r.updateAgentRun(ctx, run)
	}
	query := `INSERT INTO ` + r.Name + `.agent_runs (
		org_id, user_id, endpoint_id, agent_spec_hash,
		toolset_hash, trajectory_schema_version, decoding_params,
		status, stop_reason, total_tokens, wall_ms
	) VALUES (
		@org_id, @user_id, @endpoint_id, @agent_spec_hash,
		@toolset_hash, @trajectory_schema_version, @decoding_params::jsonb,
		@status::agent_run_status_enum, @stop_reason::agent_stop_reason_enum,
		@total_tokens, @wall_ms
	)
	RETURNING run_id::text, started_at, started_at + (wall_ms * interval '1 millisecond')`
	args, err := agentRunArgs(run)
	if err != nil {
		return nil, err
	}
	recorded := *run
	if err := scanReturnedAgentRunIdentity(r.Pool.QueryRow(ctx, query, args), &recorded); err != nil {
		r.LogPoolStatsOnError(ctx, "record agent run failed", err)
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrModelNotReady.Extend("tenant or endpoint projection is not ready")
		}
		return nil, fmt.Errorf("record agent run: %w", err)
	}
	return &recorded, nil
}

func (r *AgentTrajectoryRepository) updateAgentRun(ctx context.Context, run *model.AgentRun) (*model.AgentRun, error) {
	log.Trace("AgentTrajectoryRepository updateAgentRun")

	query := `UPDATE ` + r.Name + `.agent_runs SET
		status = @status::agent_run_status_enum,
		stop_reason = @stop_reason::agent_stop_reason_enum,
		finished_at = CASE WHEN @status::text = 'RUNNING' THEN NULL ELSE now() END,
		total_tokens = @total_tokens
	WHERE run_id = @run_id AND org_id = @org_id
	RETURNING run_id::text, started_at, started_at + (wall_ms * interval '1 millisecond')`
	args, err := agentRunArgs(run)
	if err != nil {
		return nil, err
	}
	recorded := *run
	if err := scanReturnedAgentRunIdentity(r.Pool.QueryRow(ctx, query, args), &recorded); err != nil {
		r.LogPoolStatsOnError(ctx, "record agent run failed", err)
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrModelNotReady.Extend("tenant or endpoint projection is not ready")
		}
		return nil, fmt.Errorf("record agent run: %w", err)
	}
	return &recorded, nil
}

func (r *AgentTrajectoryRepository) RecordAgentStep(ctx context.Context, step *model.AgentStep) (*model.AgentStep, error) {
	log.Trace("AgentTrajectoryRepository RecordAgentStep")

	query := `INSERT INTO ` + r.Name + `.agent_steps (
		run_id, org_id, step_index, presented_tool_schemas, generation_result,
		finish_reason, prompt_tokens, completion_tokens
	) VALUES (
		@run_id, @org_id, @step_index, @presented_tool_schemas::jsonb, @generation_result::jsonb,
		@finish_reason, @prompt_tokens, @completion_tokens
	)
	ON CONFLICT (run_id, step_index) DO UPDATE SET
		presented_tool_schemas = EXCLUDED.presented_tool_schemas,
		generation_result = EXCLUDED.generation_result,
		finish_reason = EXCLUDED.finish_reason,
		prompt_tokens = EXCLUDED.prompt_tokens,
		completion_tokens = EXCLUDED.completion_tokens
	RETURNING step_id::text, created_at`
	args, err := agentStepArgs(step)
	if err != nil {
		return nil, err
	}
	recorded := *step
	if err := scanReturnedAgentStepIdentity(r.Pool.QueryRow(ctx, query, args), &recorded); err != nil {
		r.LogPoolStatsOnError(ctx, "record agent step failed", err)
		return nil, fmt.Errorf("record agent step: %w", err)
	}
	return &recorded, nil
}

func (r *AgentTrajectoryRepository) RecordToolInvocation(ctx context.Context, invocation *model.AgentToolInvocation) (*model.AgentToolInvocation, error) {
	log.Trace("AgentTrajectoryRepository RecordToolInvocation")

	query := `INSERT INTO ` + r.Name + `.agent_tool_invocations (
		invocation_id, step_id, run_id, org_id, tool_name, tool_impl_version,
		arguments, result, error_type, latency_ms
	) VALUES (
		COALESCE(@invocation_id::uuid, gen_random_uuid()), @step_id, @run_id, @org_id, @tool_name, @tool_impl_version,
		@arguments::jsonb, @result::jsonb, @error_type::tool_error_type_enum, @latency_ms
	)
	RETURNING invocation_id::text, created_at`
	args, err := agentToolInvocationArgs(invocation)
	if err != nil {
		return nil, err
	}
	recorded := *invocation
	if err := scanReturnedAgentToolInvocationIdentity(r.Pool.QueryRow(ctx, query, args), &recorded); err != nil {
		r.LogPoolStatsOnError(ctx, "record agent tool invocation failed", err)
		return nil, fmt.Errorf("record agent tool invocation: %w", err)
	}
	return &recorded, nil
}

func (r *AgentTrajectoryRepository) ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) (*model.AgentTrajectory, error) {
	log.Trace("AgentTrajectoryRepository ReadAgentTrajectory")

	run, err := r.readAgentRun(ctx, orgID, runID)
	if err != nil {
		return nil, err
	}
	steps, err := r.readAgentSteps(ctx, orgID, runID)
	if err != nil {
		return nil, err
	}
	invocations, err := r.readAgentToolInvocations(ctx, orgID, runID)
	if err != nil {
		return nil, err
	}
	return &model.AgentTrajectory{
		Run:             run,
		Steps:           steps,
		ToolInvocations: invocations,
	}, nil
}

func (r *AgentTrajectoryRepository) FailExpiredAgentRuns(ctx context.Context, safetyMultiplier int) (int64, error) {
	log.Trace("AgentTrajectoryRepository FailExpiredAgentRuns")

	query := `UPDATE ` + r.Name + `.agent_runs SET
		status = 'FAILED'::agent_run_status_enum,
		stop_reason = 'RUNTIME_ERROR'::agent_stop_reason_enum,
		finished_at = now()
	WHERE status = 'RUNNING'::agent_run_status_enum
		AND started_at + (wall_ms * @safety_multiplier * interval '1 millisecond') < now()`
	tag, err := r.Pool.Exec(ctxutil.WithSystemContext(ctx), query, pgx.NamedArgs{
		"safety_multiplier": safetyMultiplier,
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "fail expired agent runs failed", err)
		return 0, fmt.Errorf("fail expired agent runs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *AgentTrajectoryRepository) readAgentRun(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) (*model.AgentRun, error) {
	log.Trace("AgentTrajectoryRepository readAgentRun")

	query := `SELECT
		run_id::text, org_id::text, user_id::text, COALESCE(endpoint_id::text, ''),
		agent_spec_hash,
		toolset_hash, trajectory_schema_version,
		decoding_params::text, status::text, COALESCE(stop_reason::text, ''),
		started_at, started_at + (wall_ms * interval '1 millisecond'),
		COALESCE(finished_at, 'epoch'::timestamptz), total_tokens, wall_ms
	FROM ` + r.Name + `.agent_runs
	WHERE org_id = @org_id AND run_id = @run_id`
	record, err := scanAgentRun(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id": pgtype.UUID{Bytes: orgID, Valid: true},
		"run_id": pgtype.UUID{Bytes: runID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentRunNotFound
		}
		r.LogPoolStatsOnError(ctx, "read agent run failed", err)
		return nil, fmt.Errorf("read agent run: %w", err)
	}
	return record, nil
}

func (r *AgentTrajectoryRepository) readAgentSteps(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) ([]*model.AgentStep, error) {
	log.Trace("AgentTrajectoryRepository readAgentSteps")

	query := `SELECT
		step_id::text, run_id::text, org_id::text, step_index,
		presented_tool_schemas::text, generation_result::text,
		finish_reason, prompt_tokens, completion_tokens, created_at
	FROM ` + r.Name + `.agent_steps
	WHERE org_id = @org_id AND run_id = @run_id
	ORDER BY step_index ASC`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id": pgtype.UUID{Bytes: orgID, Valid: true},
		"run_id": pgtype.UUID{Bytes: runID, Valid: true},
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "read agent steps failed", err)
		return nil, fmt.Errorf("read agent steps: %w", err)
	}
	defer rows.Close()
	steps := []*model.AgentStep{}
	for rows.Next() {
		step, err := scanAgentStep(rows)
		if err != nil {
			return nil, fmt.Errorf("scan agent step: %w", err)
		}
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent steps: %w", err)
	}
	return steps, nil
}

func (r *AgentTrajectoryRepository) readAgentToolInvocations(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) ([]*model.AgentToolInvocation, error) {
	log.Trace("AgentTrajectoryRepository readAgentToolInvocations")

	query := `SELECT
		invocation_id::text, step_id::text, run_id::text, org_id::text,
		tool_name, tool_impl_version, arguments::text, result::text,
		COALESCE(error_type::text, ''), latency_ms, created_at
	FROM ` + r.Name + `.agent_tool_invocations
	WHERE org_id = @org_id AND run_id = @run_id
	ORDER BY created_at ASC`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id": pgtype.UUID{Bytes: orgID, Valid: true},
		"run_id": pgtype.UUID{Bytes: runID, Valid: true},
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "read agent tool invocations failed", err)
		return nil, fmt.Errorf("read agent tool invocations: %w", err)
	}
	defer rows.Close()
	invocations := []*model.AgentToolInvocation{}
	for rows.Next() {
		invocation, err := scanAgentToolInvocation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan agent tool invocation: %w", err)
		}
		invocations = append(invocations, invocation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent tool invocations: %w", err)
	}
	return invocations, nil
}

func agentRunArgs(run *model.AgentRun) (pgx.NamedArgs, error) {
	log.Trace("agentRunArgs")

	decodingParams, err := requiredJSONRawMessage(run.DecodingParams, "decoding_params")
	if err != nil {
		return nil, err
	}
	return pgx.NamedArgs{
		"run_id":                    nullableUUID(run.RunID),
		"org_id":                    nullableUUID(run.OrgID),
		"user_id":                   nullableUUID(run.UserID),
		"endpoint_id":               nullableUUID(run.EndpointID),
		"agent_spec_hash":           run.AgentSpecHash,
		"toolset_hash":              run.ToolsetHash,
		"trajectory_schema_version": run.TrajectorySchemaVersion,
		"decoding_params":           decodingParams,
		"status":                    run.Status.String(),
		"stop_reason":               nullableAgentStopReason(run.StopReason),
		"total_tokens":              run.TotalTokens,
		"wall_ms":                   run.WallMs,
	}, nil
}

func agentStepArgs(step *model.AgentStep) (pgx.NamedArgs, error) {
	log.Trace("agentStepArgs")

	presentedToolSchemas, err := requiredJSONRawMessage(step.PresentedToolSchemas, "presented_tool_schemas")
	if err != nil {
		return nil, err
	}
	generationResult, err := requiredJSONRawMessage(step.GenerationResult, "generation_result")
	if err != nil {
		return nil, err
	}
	return pgx.NamedArgs{
		"run_id":                 nullableUUID(step.RunID),
		"org_id":                 nullableUUID(step.OrgID),
		"step_index":             step.StepIndex,
		"presented_tool_schemas": presentedToolSchemas,
		"generation_result":      generationResult,
		"finish_reason":          string(step.FinishReason),
		"prompt_tokens":          step.PromptTokens,
		"completion_tokens":      step.CompletionTokens,
	}, nil
}

func agentToolInvocationArgs(invocation *model.AgentToolInvocation) (pgx.NamedArgs, error) {
	log.Trace("agentToolInvocationArgs")

	arguments, err := requiredJSONRawMessage(invocation.Arguments, "arguments")
	if err != nil {
		return nil, err
	}
	result, err := requiredJSONRawMessage(invocation.Result, "result")
	if err != nil {
		return nil, err
	}
	return pgx.NamedArgs{
		"invocation_id":     nullableUUID(invocation.InvocationID),
		"step_id":           nullableUUID(invocation.StepID),
		"run_id":            nullableUUID(invocation.RunID),
		"org_id":            nullableUUID(invocation.OrgID),
		"tool_name":         invocation.ToolName,
		"tool_impl_version": invocation.ToolImplVersion,
		"arguments":         arguments,
		"result":            result,
		"error_type":        nullableToolErrorType(invocation.ErrorType),
		"latency_ms":        invocation.LatencyMs,
	}, nil
}

func requiredJSONRawMessage(value []byte, field string) (string, error) {
	log.Trace("requiredJSONRawMessage")

	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return "", domain.ErrValidationFailed.Extend(field + " is required")
	}
	if !json.Valid(trimmed) {
		return "", domain.ErrValidationFailed.Extend(field + " must contain valid JSON")
	}
	return string(trimmed), nil
}

func nullableAgentStopReason(value model.AgentStopReason) pgtype.Text {
	log.Trace("nullableAgentStopReason")

	if value == model.AgentStopReasonUnknown {
		return pgtype.Text{}
	}
	return pgtype.Text{String: value.String(), Valid: true}
}

func nullableToolErrorType(value model.ToolErrorType) pgtype.Text {
	log.Trace("nullableToolErrorType")

	if value == model.ToolErrorTypeUnknown {
		return pgtype.Text{}
	}
	return pgtype.Text{String: value.String(), Valid: true}
}

func scanReturnedAgentRunIdentity(row pgx.Row, target *model.AgentRun) error {
	log.Trace("scanReturnedAgentRunIdentity")

	var raw string
	if err := row.Scan(&raw, &target.StartedAt, &target.DeadlineAt); err != nil {
		return err
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse returned uuid: %w", err)
	}
	target.RunID = id
	return nil
}

func scanReturnedAgentStepIdentity(row pgx.Row, target *model.AgentStep) error {
	log.Trace("scanReturnedAgentStepIdentity")

	var raw string
	if err := row.Scan(&raw, &target.CreatedAt); err != nil {
		return err
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse returned uuid: %w", err)
	}
	target.StepID = id
	return nil
}

func scanReturnedAgentToolInvocationIdentity(row pgx.Row, target *model.AgentToolInvocation) error {
	log.Trace("scanReturnedAgentToolInvocationIdentity")

	var raw string
	if err := row.Scan(&raw, &target.CreatedAt); err != nil {
		return err
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse returned uuid: %w", err)
	}
	target.InvocationID = id
	return nil
}

func scanAgentRun(row pgx.Row) (*model.AgentRun, error) {
	log.Trace("scanAgentRun")

	var (
		runIDRaw          string
		orgIDRaw          string
		userIDRaw         string
		endpointIDRaw     string
		decodingParamsRaw string
		statusRaw         string
		stopReasonRaw     string
		run               model.AgentRun
	)
	if err := row.Scan(
		&runIDRaw,
		&orgIDRaw,
		&userIDRaw,
		&endpointIDRaw,
		&run.AgentSpecHash,
		&run.ToolsetHash,
		&run.TrajectorySchemaVersion,
		&decodingParamsRaw,
		&statusRaw,
		&stopReasonRaw,
		&run.StartedAt,
		&run.DeadlineAt,
		&run.FinishedAt,
		&run.TotalTokens,
		&run.WallMs,
	); err != nil {
		return nil, err
	}
	var err error
	run.RunID, err = parseAgentUUID(runIDRaw, "run_id")
	if err != nil {
		return nil, err
	}
	run.OrgID, err = parseAgentUUID(orgIDRaw, "org_id")
	if err != nil {
		return nil, err
	}
	run.UserID, err = parseAgentUUID(userIDRaw, "user_id")
	if err != nil {
		return nil, err
	}
	run.EndpointID, err = parseOptionalAgentUUID(endpointIDRaw, "endpoint_id")
	if err != nil {
		return nil, err
	}
	run.Status, err = model.ToAgentRunStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	if stopReasonRaw != "" {
		run.StopReason, err = model.ToAgentStopReason(stopReasonRaw)
		if err != nil {
			return nil, err
		}
	}
	run.DecodingParams = []byte(decodingParamsRaw)
	return &run, nil
}

func scanAgentStep(row interface{ Scan(dest ...any) error }) (*model.AgentStep, error) {
	log.Trace("scanAgentStep")

	var (
		stepIDRaw               string
		runIDRaw                string
		orgIDRaw                string
		presentedToolSchemasRaw string
		generationResultRaw     string
		finishReasonRaw         string
		step                    model.AgentStep
	)
	if err := row.Scan(
		&stepIDRaw,
		&runIDRaw,
		&orgIDRaw,
		&step.StepIndex,
		&presentedToolSchemasRaw,
		&generationResultRaw,
		&finishReasonRaw,
		&step.PromptTokens,
		&step.CompletionTokens,
		&step.CreatedAt,
	); err != nil {
		return nil, err
	}
	var err error
	step.StepID, err = parseAgentUUID(stepIDRaw, "step_id")
	if err != nil {
		return nil, err
	}
	step.RunID, err = parseAgentUUID(runIDRaw, "run_id")
	if err != nil {
		return nil, err
	}
	step.OrgID, err = parseAgentUUID(orgIDRaw, "org_id")
	if err != nil {
		return nil, err
	}
	step.PresentedToolSchemas = []byte(presentedToolSchemasRaw)
	step.GenerationResult = []byte(generationResultRaw)
	step.FinishReason = model.GenerationFinishReason(finishReasonRaw)
	return &step, nil
}

func scanAgentToolInvocation(row interface{ Scan(dest ...any) error }) (*model.AgentToolInvocation, error) {
	log.Trace("scanAgentToolInvocation")

	var (
		invocationIDRaw string
		stepIDRaw       string
		runIDRaw        string
		orgIDRaw        string
		argumentsRaw    string
		resultRaw       string
		errorTypeRaw    string
		invocation      model.AgentToolInvocation
	)
	if err := row.Scan(
		&invocationIDRaw,
		&stepIDRaw,
		&runIDRaw,
		&orgIDRaw,
		&invocation.ToolName,
		&invocation.ToolImplVersion,
		&argumentsRaw,
		&resultRaw,
		&errorTypeRaw,
		&invocation.LatencyMs,
		&invocation.CreatedAt,
	); err != nil {
		return nil, err
	}
	var err error
	invocation.InvocationID, err = parseAgentUUID(invocationIDRaw, "invocation_id")
	if err != nil {
		return nil, err
	}
	invocation.StepID, err = parseAgentUUID(stepIDRaw, "step_id")
	if err != nil {
		return nil, err
	}
	invocation.RunID, err = parseAgentUUID(runIDRaw, "run_id")
	if err != nil {
		return nil, err
	}
	invocation.OrgID, err = parseAgentUUID(orgIDRaw, "org_id")
	if err != nil {
		return nil, err
	}
	if errorTypeRaw != "" {
		invocation.ErrorType, err = model.ToToolErrorType(errorTypeRaw)
		if err != nil {
			return nil, err
		}
	}
	invocation.Arguments = []byte(argumentsRaw)
	invocation.Result = []byte(resultRaw)
	return &invocation, nil
}

func parseAgentUUID(value string, field string) (uuid.UUID, error) {
	log.Trace("parseAgentUUID")

	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse %s: %w", field, err)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("parse %s: nil uuid", field)
	}
	return id, nil
}

func parseOptionalAgentUUID(value string, field string) (uuid.UUID, error) {
	log.Trace("parseOptionalAgentUUID")

	if value == "" {
		return uuid.Nil, nil
	}
	return parseAgentUUID(value, field)
}
