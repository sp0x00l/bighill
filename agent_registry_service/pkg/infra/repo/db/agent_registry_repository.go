package db

import (
	"context"
	"errors"
	"fmt"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type AgentRegistryRepository struct {
	coreDB.Database
}

func NewAgentRegistryRepository(db *coreDB.Database) *AgentRegistryRepository {
	log.Trace("NewAgentRegistryRepository")

	return &AgentRegistryRepository{Database: *db}
}

func (r *AgentRegistryRepository) EnsureLineage(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, agentLineage string, userID uuid.UUID) error {
	log.Trace("AgentRegistryRepository EnsureLineage")

	_, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.agent_lineages (
		org_id, agent_lineage, created_by_user_id
	) VALUES (
		@org_id, @agent_lineage, @created_by_user_id
	) ON CONFLICT (org_id, agent_lineage) DO NOTHING`, pgx.NamedArgs{
		"org_id":             pgtype.UUID{Bytes: orgID, Valid: true},
		"agent_lineage":      agentLineage,
		"created_by_user_id": pgtype.UUID{Bytes: userID, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("ensure agent lineage: %w", err)
	}
	return nil
}

func (r *AgentRegistryRepository) UpsertAgentSpecVersion(ctx context.Context, tx pgx.Tx, version *model.AgentSpecVersion) (*model.AgentSpecVersion, error) {
	log.Trace("AgentRegistryRepository UpsertAgentSpecVersion")

	query := `INSERT INTO ` + r.Name + `.agent_spec_versions (
		org_id, agent_lineage, agent_spec_hash, model_id, registered_by_user_id
	) VALUES (
		@org_id, @agent_lineage, @agent_spec_hash, @model_id, @registered_by_user_id
	) ON CONFLICT (org_id, agent_spec_hash) DO UPDATE SET
		agent_lineage = EXCLUDED.agent_lineage,
		model_id = EXCLUDED.model_id
	RETURNING org_id::text, agent_lineage, agent_spec_hash, model_id::text,
		registered_by_user_id::text, registered_at`
	return scanAgentSpecVersion(tx.QueryRow(ctx, query, agentSpecVersionArgs(version)))
}

func (r *AgentRegistryRepository) UpsertEndpointBinding(ctx context.Context, tx pgx.Tx, binding *model.AgentEndpointBinding) (*model.AgentEndpointBinding, error) {
	log.Trace("AgentRegistryRepository UpsertEndpointBinding")

	query := `INSERT INTO ` + r.Name + `.agent_endpoint_bindings (
		org_id, agent_lineage, endpoint_id, created_by_user_id
	) VALUES (
		@org_id, @agent_lineage, @endpoint_id, @created_by_user_id
	) ON CONFLICT (org_id, endpoint_id) DO UPDATE SET
		agent_lineage = EXCLUDED.agent_lineage
	RETURNING org_id::text, agent_lineage, endpoint_id::text, created_by_user_id::text, created_at`
	return scanEndpointBinding(tx.QueryRow(ctx, query, endpointBindingArgs(binding)))
}

func (r *AgentRegistryRepository) ReadSpecVersion(ctx context.Context, orgID uuid.UUID, agentSpecHash string) (*model.AgentSpecVersion, error) {
	log.Trace("AgentRegistryRepository ReadSpecVersion")

	query := `SELECT org_id::text, agent_lineage, agent_spec_hash, model_id::text,
		registered_by_user_id::text, registered_at
		FROM ` + r.Name + `.agent_spec_versions
		WHERE org_id = @org_id AND agent_spec_hash = @agent_spec_hash`
	record, err := scanAgentSpecVersion(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":          pgtype.UUID{Bytes: orgID, Valid: true},
		"agent_spec_hash": agentSpecHash,
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentVersionNotFound
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) RecordChampionState(ctx context.Context, tx pgx.Tx, state *model.AgentChampionState) (*model.AgentChampionState, error) {
	log.Trace("AgentRegistryRepository RecordChampionState")

	query := `INSERT INTO ` + r.Name + `.agent_champion_states (
		org_id, agent_lineage, champion_agent_spec_hash, champion_adapter_id, serving_model_id,
		previous_agent_spec_hash, decision_id, decided_by, decided_at
	) VALUES (
		@org_id, @agent_lineage, @champion_agent_spec_hash, @champion_adapter_id, @serving_model_id,
		'', @decision_id, @decided_by, @decided_at
	) ON CONFLICT (org_id, agent_lineage) DO UPDATE SET
		champion_agent_spec_hash = EXCLUDED.champion_agent_spec_hash,
		champion_adapter_id = EXCLUDED.champion_adapter_id,
		serving_model_id = EXCLUDED.serving_model_id,
		previous_agent_spec_hash = ` + r.Name + `.agent_champion_states.champion_agent_spec_hash,
		decision_id = EXCLUDED.decision_id,
		decided_by = EXCLUDED.decided_by,
		decided_at = EXCLUDED.decided_at
	RETURNING org_id::text, agent_lineage, champion_agent_spec_hash,
		COALESCE(champion_adapter_id::text, ''), COALESCE(serving_model_id::text, ''), previous_agent_spec_hash,
		decision_id::text, decided_by::text, decided_at`
	return scanChampionState(tx.QueryRow(ctx, query, championStateArgs(state)))
}

func (r *AgentRegistryRepository) ListEndpointBindings(ctx context.Context, orgID uuid.UUID, agentLineage string) ([]*model.AgentEndpointBinding, error) {
	log.Trace("AgentRegistryRepository ListEndpointBindings")

	query := `SELECT org_id::text, agent_lineage, endpoint_id::text, created_by_user_id::text, created_at
		FROM ` + r.Name + `.agent_endpoint_bindings
		WHERE org_id = @org_id AND agent_lineage = @agent_lineage
		ORDER BY endpoint_id::text`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id":        pgtype.UUID{Bytes: orgID, Valid: true},
		"agent_lineage": agentLineage,
	})
	if err != nil {
		return nil, fmt.Errorf("list agent endpoint bindings: %w", err)
	}
	defer rows.Close()
	out := []*model.AgentEndpointBinding{}
	for rows.Next() {
		record, err := scanEndpointBinding(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *AgentRegistryRepository) CreateGoldenTask(ctx context.Context, tx pgx.Tx, task *model.GoldenTask) (*model.GoldenTask, error) {
	log.Trace("AgentRegistryRepository CreateGoldenTask")

	query := `INSERT INTO ` + r.Name + `.golden_tasks (
		org_id, agent_lineage, split, split_version, group_key, prompt, normalized_prompt_hash,
		content_fingerprint, near_duplicate_fingerprint, expected_tool_plan_hash, expected_answer, expected_answer_rubric_id, labels_hash, created_by_user_id
	) VALUES (
		@org_id, @agent_lineage, @split, @split_version, @group_key, @prompt, @normalized_prompt_hash,
		@content_fingerprint, @near_duplicate_fingerprint, @expected_tool_plan_hash, @expected_answer, @expected_answer_rubric_id, @labels_hash, @created_by_user_id
	) RETURNING task_id::text, org_id::text, agent_lineage, split::text, split_version, group_key, prompt,
		normalized_prompt_hash, content_fingerprint, near_duplicate_fingerprint, expected_tool_plan_hash, expected_answer, expected_answer_rubric_id,
		labels_hash, created_by_user_id::text, created_at`
	return scanGoldenTask(tx.QueryRow(ctx, query, goldenTaskArgs(task)))
}

func (r *AgentRegistryRepository) FindGoldenTaskLeakConflicts(ctx context.Context, tx pgx.Tx, task *model.GoldenTask) ([]model.GoldenTaskLeakConflict, error) {
	log.Trace("AgentRegistryRepository FindGoldenTaskLeakConflicts")

	query := `SELECT task_id::text, split::text, group_key, content_fingerprint, near_duplicate_fingerprint
		FROM ` + r.Name + `.golden_tasks
		WHERE org_id = @org_id
			AND agent_lineage = @agent_lineage
			AND split_version = @split_version
			AND split <> @split::` + r.Name + `.golden_task_split_enum
			AND (
				content_fingerprint = @content_fingerprint
				OR near_duplicate_fingerprint = @near_duplicate_fingerprint
				OR (@group_key <> '' AND group_key = @group_key)
			)
		ORDER BY created_at
		LIMIT 10`
	rows, err := tx.Query(ctx, query, goldenTaskArgs(task))
	if err != nil {
		return nil, fmt.Errorf("find golden task leak conflicts: %w", err)
	}
	defer rows.Close()
	conflicts := []model.GoldenTaskLeakConflict{}
	for rows.Next() {
		var taskID string
		var split string
		conflict := model.GoldenTaskLeakConflict{}
		if err := rows.Scan(&taskID, &split, &conflict.GroupKey, &conflict.ContentFingerprint, &conflict.NearDuplicateFingerprint); err != nil {
			return nil, err
		}
		conflict.TaskID = uuid.MustParse(taskID)
		parsedSplit, err := model.ToGoldenTaskSplit(split)
		if err != nil {
			return nil, err
		}
		conflict.Split = parsedSplit
		conflicts = append(conflicts, conflict)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return conflicts, nil
}

func (r *AgentRegistryRepository) ListGoldenTasks(ctx context.Context, command model.ListGoldenTasksCommand) ([]*model.GoldenTask, error) {
	log.Trace("AgentRegistryRepository ListGoldenTasks")

	query := `SELECT task_id::text, org_id::text, agent_lineage, split::text, split_version, group_key, prompt,
		normalized_prompt_hash, content_fingerprint, near_duplicate_fingerprint, expected_tool_plan_hash, expected_answer, expected_answer_rubric_id,
		labels_hash, created_by_user_id::text, created_at
		FROM ` + r.Name + `.golden_tasks
		WHERE org_id = @org_id
			AND agent_lineage = @agent_lineage
			AND split_version = @split_version
			AND split = @split::` + r.Name + `.golden_task_split_enum
		ORDER BY created_at, task_id::text`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id":        pgtype.UUID{Bytes: command.OrgID, Valid: true},
		"agent_lineage": command.AgentLineage,
		"split_version": command.SplitVersion,
		"split":         command.Split.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list golden tasks: %w", err)
	}
	defer rows.Close()
	out := []*model.GoldenTask{}
	for rows.Next() {
		record, err := scanGoldenTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *AgentRegistryRepository) RecordAgentRunLabel(ctx context.Context, tx pgx.Tx, label *model.AgentRunLabel) (*model.AgentRunLabel, error) {
	log.Trace("AgentRegistryRepository RecordAgentRunLabel")

	query := `INSERT INTO ` + r.Name + `.agent_run_labels (
		org_id, run_id, agent_lineage, agent_spec_hash, toolset_hash, effective_base_id,
		data_snapshot_hash, content_fingerprint, near_duplicate_fingerprint, evaluator, task_success, tool_selection_score,
		groundedness, policy_violations, confidence, label_source, rubric_version, created_by_user_id
	) VALUES (
		@org_id, @run_id, @agent_lineage, @agent_spec_hash, @toolset_hash, @effective_base_id,
		@data_snapshot_hash, @content_fingerprint, @near_duplicate_fingerprint, @evaluator, @task_success, @tool_selection_score,
		@groundedness, @policy_violations, @confidence, @label_source, @rubric_version, @created_by_user_id
	) ON CONFLICT (org_id, run_id, evaluator, rubric_version) DO UPDATE SET
		content_fingerprint = EXCLUDED.content_fingerprint,
		near_duplicate_fingerprint = EXCLUDED.near_duplicate_fingerprint,
		task_success = EXCLUDED.task_success,
		tool_selection_score = EXCLUDED.tool_selection_score,
		groundedness = EXCLUDED.groundedness,
		policy_violations = EXCLUDED.policy_violations,
		confidence = EXCLUDED.confidence,
		label_source = EXCLUDED.label_source
	RETURNING label_id::text, org_id::text, run_id::text, agent_lineage, agent_spec_hash, toolset_hash,
		effective_base_id, data_snapshot_hash, content_fingerprint, near_duplicate_fingerprint, evaluator, task_success,
		tool_selection_score::float8, groundedness::float8, policy_violations, confidence::float8,
		label_source, rubric_version, created_by_user_id::text, created_at`
	return scanAgentRunLabel(tx.QueryRow(ctx, query, agentRunLabelArgs(label)))
}

func (r *AgentRegistryRepository) ListAgentRunLabels(ctx context.Context, command model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error) {
	log.Trace("AgentRegistryRepository ListAgentRunLabels")

	query := `SELECT label_id::text, org_id::text, run_id::text, agent_lineage, agent_spec_hash, toolset_hash,
		effective_base_id, data_snapshot_hash, content_fingerprint, near_duplicate_fingerprint, evaluator, task_success,
		tool_selection_score::float8, groundedness::float8, policy_violations, confidence::float8,
		label_source, rubric_version, created_by_user_id::text, created_at
		FROM ` + r.Name + `.agent_run_labels
		WHERE org_id = @org_id AND agent_lineage = @agent_lineage
		ORDER BY created_at, label_id::text`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id":        pgtype.UUID{Bytes: command.OrgID, Valid: true},
		"agent_lineage": command.AgentLineage,
	})
	if err != nil {
		return nil, fmt.Errorf("list agent run labels: %w", err)
	}
	defer rows.Close()
	out := []*model.AgentRunLabel{}
	for rows.Next() {
		record, err := scanAgentRunLabel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *AgentRegistryRepository) RecordTrajectoryDataset(ctx context.Context, tx pgx.Tx, dataset *model.AgentTrajectoryDataset) (*model.AgentTrajectoryDataset, error) {
	log.Trace("AgentRegistryRepository RecordTrajectoryDataset")

	query := `INSERT INTO ` + r.Name + `.agent_trajectory_datasets (
		org_id, agent_lineage, golden_split_version, content_hash, dataset_uri, format, label_count,
		manifest, effective_base_id, agent_spec_hash, toolset_hash, data_snapshot_hash, created_by_user_id
	) VALUES (
		@org_id, @agent_lineage, @golden_split_version, @content_hash, @dataset_uri, @format, @label_count,
		@manifest::jsonb, @effective_base_id, @agent_spec_hash, @toolset_hash, @data_snapshot_hash, @created_by_user_id
	) ON CONFLICT (content_hash) DO UPDATE SET
		dataset_uri = EXCLUDED.dataset_uri
	RETURNING dataset_id::text, org_id::text, agent_lineage, golden_split_version, content_hash, dataset_uri,
		format, label_count, manifest, effective_base_id, agent_spec_hash, toolset_hash, data_snapshot_hash,
		created_by_user_id::text, created_at`
	return scanTrajectoryDataset(tx.QueryRow(ctx, query, trajectoryDatasetArgs(dataset)))
}

func (r *AgentRegistryRepository) ReadTrajectoryDataset(ctx context.Context, orgID uuid.UUID, datasetID uuid.UUID) (*model.AgentTrajectoryDataset, error) {
	log.Trace("AgentRegistryRepository ReadTrajectoryDataset")

	query := `SELECT dataset_id::text, org_id::text, agent_lineage, golden_split_version, content_hash, dataset_uri,
		format, label_count, manifest, effective_base_id, agent_spec_hash, toolset_hash, data_snapshot_hash,
		created_by_user_id::text, created_at
		FROM ` + r.Name + `.agent_trajectory_datasets
		WHERE org_id = @org_id AND dataset_id = @dataset_id`
	record, err := scanTrajectoryDataset(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":     pgtype.UUID{Bytes: orgID, Valid: true},
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTrajectoryDatasetNotFound
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) RecordAgentAdapter(ctx context.Context, tx pgx.Tx, adapter *model.AgentAdapter) (*model.AgentAdapter, error) {
	log.Trace("AgentRegistryRepository RecordAgentAdapter")

	query := `INSERT INTO ` + r.Name + `.agent_adapters (
		org_id, agent_lineage, dataset_id, training_run_id, serving_model_id, adapter_uri, adapter_checksum,
		training_provider, trained_against_effective_base_id, trained_against_agent_spec_hash,
		trained_against_toolset_hash, trained_against_data_snapshot_hash, trained_against_rubric_version,
		trained_against_golden_split_version, status, promotion_passed, created_by_user_id
	) VALUES (
		@org_id, @agent_lineage, @dataset_id, @training_run_id, @serving_model_id, @adapter_uri, @adapter_checksum,
		@training_provider, @trained_against_effective_base_id, @trained_against_agent_spec_hash,
		@trained_against_toolset_hash, @trained_against_data_snapshot_hash, @trained_against_rubric_version,
		@trained_against_golden_split_version, @status, @promotion_passed, @created_by_user_id
	) RETURNING ` + agentAdapterColumns()
	return scanAgentAdapter(tx.QueryRow(ctx, query, agentAdapterArgs(adapter)))
}

func (r *AgentRegistryRepository) ReadAgentAdapter(ctx context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentAdapter, error) {
	log.Trace("AgentRegistryRepository ReadAgentAdapter")

	query := `SELECT ` + agentAdapterColumns() + `
		FROM ` + r.Name + `.agent_adapters
		WHERE org_id = @org_id AND adapter_id = @adapter_id`
	record, err := scanAgentAdapter(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":     pgtype.UUID{Bytes: orgID, Valid: true},
		"adapter_id": pgtype.UUID{Bytes: adapterID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentAdapterNotFound
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) CompleteAgentAdapterTraining(ctx context.Context, tx pgx.Tx, completion model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error) {
	log.Trace("AgentRegistryRepository CompleteAgentAdapterTraining")

	query := `UPDATE ` + r.Name + `.agent_adapters
		SET serving_model_id = @serving_model_id,
			adapter_uri = @adapter_uri,
			adapter_checksum = @adapter_checksum,
			training_provider = @training_provider,
			status = @status
		WHERE org_id = @org_id
			AND training_run_id = @training_run_id
			AND status = 'TRAINING'
		RETURNING ` + agentAdapterColumns()
	record, err := scanAgentAdapter(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":            pgtype.UUID{Bytes: completion.OrgID, Valid: true},
		"training_run_id":   pgtype.UUID{Bytes: completion.TrainingRunID, Valid: true},
		"serving_model_id":  pgtype.UUID{Bytes: completion.ServingModelID, Valid: true},
		"adapter_uri":       completion.AdapterURI,
		"adapter_checksum":  completion.AdapterChecksum,
		"training_provider": completion.TrainingProvider,
		"status":            model.AgentAdapterStatusCandidate.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentAdapterNotFound
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) FailAgentAdapterTraining(ctx context.Context, tx pgx.Tx, failure model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error) {
	log.Trace("AgentRegistryRepository FailAgentAdapterTraining")

	query := `UPDATE ` + r.Name + `.agent_adapters
		SET status = @status
		WHERE org_id = @org_id
			AND training_run_id = @training_run_id
			AND status = 'TRAINING'
		RETURNING ` + agentAdapterColumns()
	record, err := scanAgentAdapter(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":          pgtype.UUID{Bytes: failure.OrgID, Valid: true},
		"training_run_id": pgtype.UUID{Bytes: failure.TrainingRunID, Valid: true},
		"status":          model.AgentAdapterStatusFailed.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentAdapterNotFound
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) UpdateAgentAdapterPromotion(ctx context.Context, tx pgx.Tx, adapterID uuid.UUID, status model.AgentAdapterStatus, promotionPassed bool) (*model.AgentAdapter, error) {
	log.Trace("AgentRegistryRepository UpdateAgentAdapterPromotion")

	query := `UPDATE ` + r.Name + `.agent_adapters
		SET status = @status, promotion_passed = @promotion_passed
		WHERE adapter_id = @adapter_id
		RETURNING ` + agentAdapterColumns()
	return scanAgentAdapter(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"adapter_id":       pgtype.UUID{Bytes: adapterID, Valid: true},
		"status":           status.String(),
		"promotion_passed": promotionPassed,
	}))
}

func (r *AgentRegistryRepository) ReadChampionState(ctx context.Context, orgID uuid.UUID, agentLineage string) (*model.AgentChampionState, error) {
	log.Trace("AgentRegistryRepository ReadChampionState")

	query := `SELECT org_id::text, agent_lineage, champion_agent_spec_hash,
		COALESCE(champion_adapter_id::text, ''), COALESCE(serving_model_id::text, ''), previous_agent_spec_hash,
		decision_id::text, decided_by::text, decided_at
		FROM ` + r.Name + `.agent_champion_states
		WHERE org_id = @org_id AND agent_lineage = @agent_lineage`
	record, err := scanChampionState(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":        pgtype.UUID{Bytes: orgID, Valid: true},
		"agent_lineage": agentLineage,
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentChampionNotFound
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) ReadLatestEvalReportForAdapter(ctx context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentEvalReport, error) {
	log.Trace("AgentRegistryRepository ReadLatestEvalReportForAdapter")

	query := `SELECT report_id::text, org_id::text, agent_lineage, agent_spec_hash, COALESCE(adapter_id::text, ''),
		endpoint_id::text, split::text, split_version, rubric_version, task_count, task_success_rate, tool_success_rate, groundedness_rate,
		passed, gate_reason, COALESCE(promoted_decision_id::text, ''), evaluated_by::text, evaluated_at
		FROM ` + r.Name + `.agent_eval_reports
		WHERE org_id = @org_id AND adapter_id = @adapter_id
		ORDER BY evaluated_at DESC
		LIMIT 1`
	record, err := scanAgentEvalReport(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":     pgtype.UUID{Bytes: orgID, Valid: true},
		"adapter_id": pgtype.UUID{Bytes: adapterID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrAgentEvalFailed.Extend("no eval report exists for adapter")
		}
		return nil, err
	}
	return record, nil
}

func (r *AgentRegistryRepository) RecordAgentEvalReport(ctx context.Context, tx pgx.Tx, report *model.AgentEvalReport) (*model.AgentEvalReport, error) {
	log.Trace("AgentRegistryRepository RecordAgentEvalReport")

	query := `INSERT INTO ` + r.Name + `.agent_eval_reports (
		org_id, agent_lineage, agent_spec_hash, adapter_id, endpoint_id, split, split_version, rubric_version,
		task_count, task_success_rate, tool_success_rate, groundedness_rate, passed, gate_reason,
		promoted_decision_id, evaluated_by
	) VALUES (
		@org_id, @agent_lineage, @agent_spec_hash, @adapter_id, @endpoint_id, @split, @split_version, @rubric_version,
		@task_count, @task_success_rate, @tool_success_rate, @groundedness_rate, @passed, @gate_reason,
		@promoted_decision_id, @evaluated_by
	) RETURNING report_id::text, org_id::text, agent_lineage, agent_spec_hash, COALESCE(adapter_id::text, ''), endpoint_id::text, split::text,
		split_version, rubric_version, task_count, task_success_rate, tool_success_rate, groundedness_rate,
		passed, gate_reason, COALESCE(promoted_decision_id::text, ''), evaluated_by::text, evaluated_at`
	record, err := scanAgentEvalReport(tx.QueryRow(ctx, query, agentEvalReportArgs(report)))
	if err != nil {
		return nil, err
	}
	for _, result := range report.TaskResults {
		if result == nil {
			continue
		}
		result.OrgID = record.OrgID
		result.ReportID = record.ReportID
		if _, err := tx.Exec(ctx, `INSERT INTO `+r.Name+`.agent_eval_task_results (
			org_id, report_id, task_id, run_id, status, stop_reason, task_success, tool_success, groundedness, failure_reason
		) VALUES (
			@org_id, @report_id, @task_id, @run_id, @status, @stop_reason, @task_success, @tool_success, @groundedness, @failure_reason
		)`, agentEvalTaskResultArgs(result)); err != nil {
			return nil, fmt.Errorf("record agent eval task result: %w", err)
		}
		record.TaskResults = append(record.TaskResults, result)
	}
	return record, nil
}

func (r *AgentRegistryRepository) ReadAgentEvalReport(ctx context.Context, orgID uuid.UUID, reportID uuid.UUID) (*model.AgentEvalReport, error) {
	log.Trace("AgentRegistryRepository ReadAgentEvalReport")

	query := `SELECT report_id::text, org_id::text, agent_lineage, agent_spec_hash, COALESCE(adapter_id::text, ''), endpoint_id::text, split::text,
		split_version, rubric_version, task_count, task_success_rate, tool_success_rate, groundedness_rate,
		passed, gate_reason, COALESCE(promoted_decision_id::text, ''), evaluated_by::text, evaluated_at
		FROM ` + r.Name + `.agent_eval_reports
		WHERE org_id = @org_id AND report_id = @report_id`
	report, err := scanAgentEvalReport(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":    pgtype.UUID{Bytes: orgID, Valid: true},
		"report_id": pgtype.UUID{Bytes: reportID, Valid: true},
	}))
	if err != nil {
		return nil, err
	}
	results, err := r.readAgentEvalTaskResults(ctx, report.OrgID, report.ReportID)
	if err != nil {
		return nil, err
	}
	report.TaskResults = results
	return report, nil
}

func (r *AgentRegistryRepository) readAgentEvalTaskResults(ctx context.Context, orgID uuid.UUID, reportID uuid.UUID) ([]*model.AgentEvalTaskResult, error) {
	log.Trace("AgentRegistryRepository readAgentEvalTaskResults")

	query := `SELECT org_id::text, report_id::text, task_id::text, COALESCE(run_id::text, ''), status, stop_reason,
		task_success, tool_success, groundedness, failure_reason
		FROM ` + r.Name + `.agent_eval_task_results
		WHERE org_id = @org_id AND report_id = @report_id
		ORDER BY task_id::text`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id":    pgtype.UUID{Bytes: orgID, Valid: true},
		"report_id": pgtype.UUID{Bytes: reportID, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("read agent eval task results: %w", err)
	}
	defer rows.Close()
	results := []*model.AgentEvalTaskResult{}
	for rows.Next() {
		result, err := scanAgentEvalTaskResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func agentSpecVersionArgs(version *model.AgentSpecVersion) pgx.NamedArgs {
	log.Trace("agentSpecVersionArgs")

	return pgx.NamedArgs{
		"org_id":                pgtype.UUID{Bytes: version.OrgID, Valid: true},
		"agent_lineage":         version.AgentLineage,
		"agent_spec_hash":       version.AgentSpecHash,
		"model_id":              pgtype.UUID{Bytes: version.ModelID, Valid: true},
		"registered_by_user_id": pgtype.UUID{Bytes: version.RegisteredByUserID, Valid: true},
	}
}

func endpointBindingArgs(binding *model.AgentEndpointBinding) pgx.NamedArgs {
	log.Trace("endpointBindingArgs")

	return pgx.NamedArgs{
		"org_id":             pgtype.UUID{Bytes: binding.OrgID, Valid: true},
		"agent_lineage":      binding.AgentLineage,
		"endpoint_id":        pgtype.UUID{Bytes: binding.EndpointID, Valid: true},
		"created_by_user_id": pgtype.UUID{Bytes: binding.CreatedByUserID, Valid: true},
	}
}

func championStateArgs(state *model.AgentChampionState) pgx.NamedArgs {
	log.Trace("championStateArgs")

	championAdapterID := pgtype.UUID{Valid: false}
	if state.ChampionAdapterID != uuid.Nil {
		championAdapterID = pgtype.UUID{Bytes: state.ChampionAdapterID, Valid: true}
	}
	servingModelID := pgtype.UUID{Valid: false}
	if state.ServingModelID != uuid.Nil {
		servingModelID = pgtype.UUID{Bytes: state.ServingModelID, Valid: true}
	}
	return pgx.NamedArgs{
		"org_id":                   pgtype.UUID{Bytes: state.OrgID, Valid: true},
		"agent_lineage":            state.AgentLineage,
		"champion_agent_spec_hash": state.ChampionAgentSpecHash,
		"champion_adapter_id":      championAdapterID,
		"serving_model_id":         servingModelID,
		"decision_id":              pgtype.UUID{Bytes: state.DecisionID, Valid: true},
		"decided_by":               pgtype.UUID{Bytes: state.DecidedBy, Valid: true},
		"decided_at":               state.DecidedAt,
	}
}

func goldenTaskArgs(task *model.GoldenTask) pgx.NamedArgs {
	log.Trace("goldenTaskArgs")

	return pgx.NamedArgs{
		"org_id":                     pgtype.UUID{Bytes: task.OrgID, Valid: true},
		"agent_lineage":              task.AgentLineage,
		"split":                      task.Split.String(),
		"split_version":              task.SplitVersion,
		"group_key":                  task.GroupKey,
		"prompt":                     task.Prompt,
		"normalized_prompt_hash":     task.NormalizedPromptHash,
		"content_fingerprint":        task.ContentFingerprint,
		"near_duplicate_fingerprint": task.NearDuplicateFingerprint,
		"expected_tool_plan_hash":    task.ExpectedToolPlanHash,
		"expected_answer":            task.ExpectedAnswer,
		"expected_answer_rubric_id":  task.ExpectedAnswerRubricID,
		"labels_hash":                task.LabelsHash,
		"created_by_user_id":         pgtype.UUID{Bytes: task.CreatedByUserID, Valid: true},
	}
}

func agentRunLabelArgs(label *model.AgentRunLabel) pgx.NamedArgs {
	log.Trace("agentRunLabelArgs")

	return pgx.NamedArgs{
		"org_id":                     pgtype.UUID{Bytes: label.OrgID, Valid: true},
		"run_id":                     pgtype.UUID{Bytes: label.RunID, Valid: true},
		"agent_lineage":              label.AgentLineage,
		"agent_spec_hash":            label.AgentSpecHash,
		"toolset_hash":               label.ToolsetHash,
		"effective_base_id":          label.EffectiveBaseID,
		"data_snapshot_hash":         label.DataSnapshotHash,
		"content_fingerprint":        label.ContentFingerprint,
		"near_duplicate_fingerprint": label.NearDuplicateFingerprint,
		"evaluator":                  label.Evaluator,
		"task_success":               label.TaskSuccess,
		"tool_selection_score":       label.ToolSelectionScore,
		"groundedness":               label.Groundedness,
		"policy_violations":          label.PolicyViolations,
		"confidence":                 label.Confidence,
		"label_source":               label.LabelSource,
		"rubric_version":             label.RubricVersion,
		"created_by_user_id":         pgtype.UUID{Bytes: label.CreatedByUserID, Valid: true},
	}
}

func trajectoryDatasetArgs(dataset *model.AgentTrajectoryDataset) pgx.NamedArgs {
	log.Trace("trajectoryDatasetArgs")

	return pgx.NamedArgs{
		"org_id":               pgtype.UUID{Bytes: dataset.OrgID, Valid: true},
		"agent_lineage":        dataset.AgentLineage,
		"golden_split_version": dataset.GoldenSplitVersion,
		"content_hash":         dataset.ContentHash,
		"dataset_uri":          dataset.DatasetURI,
		"format":               dataset.Format,
		"label_count":          dataset.LabelCount,
		"manifest":             dataset.Manifest,
		"effective_base_id":    dataset.EffectiveBaseID,
		"agent_spec_hash":      dataset.AgentSpecHash,
		"toolset_hash":         dataset.ToolsetHash,
		"data_snapshot_hash":   dataset.DataSnapshotHash,
		"created_by_user_id":   pgtype.UUID{Bytes: dataset.CreatedByUserID, Valid: true},
	}
}

func agentAdapterArgs(adapter *model.AgentAdapter) pgx.NamedArgs {
	log.Trace("agentAdapterArgs")

	trainingRunID := pgtype.UUID{Valid: false}
	if adapter.TrainingRunID != uuid.Nil {
		trainingRunID = pgtype.UUID{Bytes: adapter.TrainingRunID, Valid: true}
	}
	servingModelID := pgtype.UUID{Valid: false}
	if adapter.ServingModelID != uuid.Nil {
		servingModelID = pgtype.UUID{Bytes: adapter.ServingModelID, Valid: true}
	}
	return pgx.NamedArgs{
		"org_id":                               pgtype.UUID{Bytes: adapter.OrgID, Valid: true},
		"agent_lineage":                        adapter.AgentLineage,
		"dataset_id":                           pgtype.UUID{Bytes: adapter.DatasetID, Valid: true},
		"training_run_id":                      trainingRunID,
		"serving_model_id":                     servingModelID,
		"adapter_uri":                          adapter.AdapterURI,
		"adapter_checksum":                     adapter.AdapterChecksum,
		"training_provider":                    adapter.TrainingProvider,
		"trained_against_effective_base_id":    adapter.TrainedAgainstEffectiveBaseID,
		"trained_against_agent_spec_hash":      adapter.TrainedAgainstAgentSpecHash,
		"trained_against_toolset_hash":         adapter.TrainedAgainstToolsetHash,
		"trained_against_data_snapshot_hash":   adapter.TrainedAgainstDataSnapshotHash,
		"trained_against_rubric_version":       adapter.TrainedAgainstRubricVersion,
		"trained_against_golden_split_version": adapter.TrainedAgainstGoldenSplitVersion,
		"status":                               adapter.Status.String(),
		"promotion_passed":                     adapter.PromotionPassed,
		"created_by_user_id":                   pgtype.UUID{Bytes: adapter.CreatedByUserID, Valid: true},
	}
}

func agentAdapterColumns() string {
	return `adapter_id::text, org_id::text, agent_lineage, dataset_id::text, COALESCE(training_run_id::text, ''),
		COALESCE(serving_model_id::text, ''), adapter_uri, adapter_checksum, training_provider,
		trained_against_effective_base_id, trained_against_agent_spec_hash, trained_against_toolset_hash,
		trained_against_data_snapshot_hash, trained_against_rubric_version, trained_against_golden_split_version,
		status, promotion_passed, created_by_user_id::text, created_at, updated_at`
}

func agentEvalReportArgs(report *model.AgentEvalReport) pgx.NamedArgs {
	log.Trace("agentEvalReportArgs")

	promotedDecisionID := pgtype.UUID{Valid: false}
	if report.PromotedDecisionID != uuid.Nil {
		promotedDecisionID = pgtype.UUID{Bytes: report.PromotedDecisionID, Valid: true}
	}
	adapterID := pgtype.UUID{Valid: false}
	if report.AdapterID != uuid.Nil {
		adapterID = pgtype.UUID{Bytes: report.AdapterID, Valid: true}
	}
	return pgx.NamedArgs{
		"org_id":               pgtype.UUID{Bytes: report.OrgID, Valid: true},
		"agent_lineage":        report.AgentLineage,
		"agent_spec_hash":      report.AgentSpecHash,
		"adapter_id":           adapterID,
		"endpoint_id":          pgtype.UUID{Bytes: report.EndpointID, Valid: true},
		"split":                report.Split.String(),
		"split_version":        report.SplitVersion,
		"rubric_version":       report.RubricVersion,
		"task_count":           report.TaskCount,
		"task_success_rate":    report.TaskSuccessRate,
		"tool_success_rate":    report.ToolSuccessRate,
		"groundedness_rate":    report.GroundednessRate,
		"passed":               report.Passed,
		"gate_reason":          report.GateReason,
		"promoted_decision_id": promotedDecisionID,
		"evaluated_by":         pgtype.UUID{Bytes: report.EvaluatedBy, Valid: true},
	}
}

func agentEvalTaskResultArgs(result *model.AgentEvalTaskResult) pgx.NamedArgs {
	log.Trace("agentEvalTaskResultArgs")

	runID := pgtype.UUID{Valid: false}
	if result.RunID != uuid.Nil {
		runID = pgtype.UUID{Bytes: result.RunID, Valid: true}
	}
	return pgx.NamedArgs{
		"org_id":         pgtype.UUID{Bytes: result.OrgID, Valid: true},
		"report_id":      pgtype.UUID{Bytes: result.ReportID, Valid: true},
		"task_id":        pgtype.UUID{Bytes: result.TaskID, Valid: true},
		"run_id":         runID,
		"status":         result.Status,
		"stop_reason":    result.StopReason,
		"task_success":   result.TaskSuccess,
		"tool_success":   result.ToolSuccess,
		"groundedness":   result.Groundedness,
		"failure_reason": result.FailureReason,
	}
}

func scanAgentSpecVersion(row pgx.Row) (*model.AgentSpecVersion, error) {
	log.Trace("scanAgentSpecVersion")

	var orgID, modelID, registeredByUserID string
	record := &model.AgentSpecVersion{}
	if err := row.Scan(
		&orgID,
		&record.AgentLineage,
		&record.AgentSpecHash,
		&modelID,
		&registeredByUserID,
		&record.RegisteredAt,
	); err != nil {
		return nil, err
	}
	record.OrgID = uuid.MustParse(orgID)
	record.ModelID = uuid.MustParse(modelID)
	record.RegisteredByUserID = uuid.MustParse(registeredByUserID)
	return record, nil
}

func scanEndpointBinding(row pgx.Row) (*model.AgentEndpointBinding, error) {
	log.Trace("scanEndpointBinding")

	var orgID, endpointID, createdByUserID string
	record := &model.AgentEndpointBinding{}
	if err := row.Scan(
		&orgID,
		&record.AgentLineage,
		&endpointID,
		&createdByUserID,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	record.OrgID = uuid.MustParse(orgID)
	record.EndpointID = uuid.MustParse(endpointID)
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	return record, nil
}

func scanChampionState(row pgx.Row) (*model.AgentChampionState, error) {
	log.Trace("scanChampionState")

	var orgID, championAdapterID, servingModelID, decisionID, decidedBy string
	record := &model.AgentChampionState{}
	if err := row.Scan(
		&orgID,
		&record.AgentLineage,
		&record.ChampionAgentSpecHash,
		&championAdapterID,
		&servingModelID,
		&record.PreviousAgentSpecHash,
		&decisionID,
		&decidedBy,
		&record.DecidedAt,
	); err != nil {
		return nil, err
	}
	record.OrgID = uuid.MustParse(orgID)
	if championAdapterID != "" {
		record.ChampionAdapterID = uuid.MustParse(championAdapterID)
	}
	if servingModelID != "" {
		record.ServingModelID = uuid.MustParse(servingModelID)
	}
	record.DecisionID = uuid.MustParse(decisionID)
	record.DecidedBy = uuid.MustParse(decidedBy)
	return record, nil
}

func scanGoldenTask(row pgx.Row) (*model.GoldenTask, error) {
	log.Trace("scanGoldenTask")

	var taskID, orgID, split, createdByUserID string
	record := &model.GoldenTask{}
	if err := row.Scan(
		&taskID,
		&orgID,
		&record.AgentLineage,
		&split,
		&record.SplitVersion,
		&record.GroupKey,
		&record.Prompt,
		&record.NormalizedPromptHash,
		&record.ContentFingerprint,
		&record.NearDuplicateFingerprint,
		&record.ExpectedToolPlanHash,
		&record.ExpectedAnswer,
		&record.ExpectedAnswerRubricID,
		&record.LabelsHash,
		&createdByUserID,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	parsedSplit, err := model.ToGoldenTaskSplit(split)
	if err != nil {
		return nil, err
	}
	record.TaskID = uuid.MustParse(taskID)
	record.OrgID = uuid.MustParse(orgID)
	record.Split = parsedSplit
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	return record, nil
}

func scanAgentRunLabel(row pgx.Row) (*model.AgentRunLabel, error) {
	log.Trace("scanAgentRunLabel")

	var labelID, orgID, runID, createdByUserID string
	record := &model.AgentRunLabel{}
	if err := row.Scan(
		&labelID,
		&orgID,
		&runID,
		&record.AgentLineage,
		&record.AgentSpecHash,
		&record.ToolsetHash,
		&record.EffectiveBaseID,
		&record.DataSnapshotHash,
		&record.ContentFingerprint,
		&record.NearDuplicateFingerprint,
		&record.Evaluator,
		&record.TaskSuccess,
		&record.ToolSelectionScore,
		&record.Groundedness,
		&record.PolicyViolations,
		&record.Confidence,
		&record.LabelSource,
		&record.RubricVersion,
		&createdByUserID,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	record.LabelID = uuid.MustParse(labelID)
	record.OrgID = uuid.MustParse(orgID)
	record.RunID = uuid.MustParse(runID)
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	return record, nil
}

func scanTrajectoryDataset(row pgx.Row) (*model.AgentTrajectoryDataset, error) {
	log.Trace("scanTrajectoryDataset")

	var datasetID, orgID, createdByUserID string
	record := &model.AgentTrajectoryDataset{}
	if err := row.Scan(
		&datasetID,
		&orgID,
		&record.AgentLineage,
		&record.GoldenSplitVersion,
		&record.ContentHash,
		&record.DatasetURI,
		&record.Format,
		&record.LabelCount,
		&record.Manifest,
		&record.EffectiveBaseID,
		&record.AgentSpecHash,
		&record.ToolsetHash,
		&record.DataSnapshotHash,
		&createdByUserID,
		&record.CreatedAt,
	); err != nil {
		return nil, err
	}
	record.DatasetID = uuid.MustParse(datasetID)
	record.OrgID = uuid.MustParse(orgID)
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	return record, nil
}

func scanAgentAdapter(row pgx.Row) (*model.AgentAdapter, error) {
	log.Trace("scanAgentAdapter")

	var adapterID, orgID, datasetID, trainingRunID, servingModelID, createdByUserID, status string
	record := &model.AgentAdapter{}
	if err := row.Scan(
		&adapterID,
		&orgID,
		&record.AgentLineage,
		&datasetID,
		&trainingRunID,
		&servingModelID,
		&record.AdapterURI,
		&record.AdapterChecksum,
		&record.TrainingProvider,
		&record.TrainedAgainstEffectiveBaseID,
		&record.TrainedAgainstAgentSpecHash,
		&record.TrainedAgainstToolsetHash,
		&record.TrainedAgainstDataSnapshotHash,
		&record.TrainedAgainstRubricVersion,
		&record.TrainedAgainstGoldenSplitVersion,
		&status,
		&record.PromotionPassed,
		&createdByUserID,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return nil, err
	}
	parsedStatus, err := model.ToAgentAdapterStatus(status)
	if err != nil {
		return nil, err
	}
	record.AdapterID = uuid.MustParse(adapterID)
	record.OrgID = uuid.MustParse(orgID)
	record.DatasetID = uuid.MustParse(datasetID)
	if trainingRunID != "" {
		record.TrainingRunID = uuid.MustParse(trainingRunID)
	}
	if servingModelID != "" {
		record.ServingModelID = uuid.MustParse(servingModelID)
	}
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	record.Status = parsedStatus
	return record, nil
}

func scanAgentEvalReport(row pgx.Row) (*model.AgentEvalReport, error) {
	log.Trace("scanAgentEvalReport")

	var reportID, orgID, adapterID, endpointID, split, promotedDecisionID, evaluatedBy string
	record := &model.AgentEvalReport{}
	if err := row.Scan(
		&reportID,
		&orgID,
		&record.AgentLineage,
		&record.AgentSpecHash,
		&adapterID,
		&endpointID,
		&split,
		&record.SplitVersion,
		&record.RubricVersion,
		&record.TaskCount,
		&record.TaskSuccessRate,
		&record.ToolSuccessRate,
		&record.GroundednessRate,
		&record.Passed,
		&record.GateReason,
		&promotedDecisionID,
		&evaluatedBy,
		&record.EvaluatedAt,
	); err != nil {
		return nil, err
	}
	parsedSplit, err := model.ToGoldenTaskSplit(split)
	if err != nil {
		return nil, err
	}
	record.ReportID = uuid.MustParse(reportID)
	record.OrgID = uuid.MustParse(orgID)
	if adapterID != "" {
		record.AdapterID = uuid.MustParse(adapterID)
	}
	record.EndpointID = uuid.MustParse(endpointID)
	record.Split = parsedSplit
	if promotedDecisionID != "" {
		record.PromotedDecisionID = uuid.MustParse(promotedDecisionID)
	}
	record.EvaluatedBy = uuid.MustParse(evaluatedBy)
	return record, nil
}

func scanAgentEvalTaskResult(row pgx.Row) (*model.AgentEvalTaskResult, error) {
	log.Trace("scanAgentEvalTaskResult")

	var orgID, reportID, taskID, runID string
	record := &model.AgentEvalTaskResult{}
	if err := row.Scan(
		&orgID,
		&reportID,
		&taskID,
		&runID,
		&record.Status,
		&record.StopReason,
		&record.TaskSuccess,
		&record.ToolSuccess,
		&record.Groundedness,
		&record.FailureReason,
	); err != nil {
		return nil, err
	}
	record.OrgID = uuid.MustParse(orgID)
	record.ReportID = uuid.MustParse(reportID)
	record.TaskID = uuid.MustParse(taskID)
	if runID != "" {
		record.RunID = uuid.MustParse(runID)
	}
	return record, nil
}
