package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type InferenceModelRepository struct {
	coreDB.Database
}

func NewInferenceModelRepository(db *coreDB.Database) *InferenceModelRepository {
	log.Trace("NewInferenceModelRepository")

	return &InferenceModelRepository{
		Database: *db,
	}
}

func (r *InferenceModelRepository) UpsertModel(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceModelRepository UpsertModel")

	query := `INSERT INTO ` + r.Name + `.inference_models (
		model_id, user_id, org_id, training_run_id, dataset_id, idempotency_key,
		model_kind, source, source_uri, source_metadata,
		name, lineage_name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, serving_protocol, serving_load_status,
		effective_base_id, metrics_metadata, status, failure_reason
	) VALUES (
		@model_id, @user_id, @org_id, @training_run_id, @dataset_id, @idempotency_key,
		@model_kind::inference_model_kind_enum, @source::inference_model_source_enum, @source_uri, @source_metadata::jsonb,
		@name, @lineage_name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes,
		@adapter_uri, @serving_target, @serving_model, NULLIF(@serving_protocol, '')::serving_protocol_enum, @serving_load_status::inference_model_load_status_enum,
		@effective_base_id, @metrics_metadata::jsonb, @status::inference_model_status_enum, @failure_reason
	)
	ON CONFLICT (model_id) DO UPDATE SET
		training_run_id = EXCLUDED.training_run_id,
		user_id = EXCLUDED.user_id,
		org_id = EXCLUDED.org_id,
		dataset_id = EXCLUDED.dataset_id,
		idempotency_key = EXCLUDED.idempotency_key,
		model_kind = EXCLUDED.model_kind,
		source = EXCLUDED.source,
		source_uri = EXCLUDED.source_uri,
		source_metadata = EXCLUDED.source_metadata,
		name = EXCLUDED.name,
		lineage_name = EXCLUDED.lineage_name,
		model_version = EXCLUDED.model_version,
		base_model = EXCLUDED.base_model,
		artifact_location = EXCLUDED.artifact_location,
		artifact_format = EXCLUDED.artifact_format,
		artifact_checksum = EXCLUDED.artifact_checksum,
		artifact_size_bytes = EXCLUDED.artifact_size_bytes,
		adapter_uri = EXCLUDED.adapter_uri,
		serving_target = EXCLUDED.serving_target,
		serving_model = EXCLUDED.serving_model,
		serving_protocol = EXCLUDED.serving_protocol,
		serving_load_status = EXCLUDED.serving_load_status,
		effective_base_id = EXCLUDED.effective_base_id,
		metrics_metadata = EXCLUDED.metrics_metadata,
		status = EXCLUDED.status,
		failure_reason = EXCLUDED.failure_reason
	RETURNING ` + modelColumns()

	record, err := scanInferenceModel(r.Pool.QueryRow(ctx, query, modelArgs(inferenceModel, idempotencyKey)))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "upsert inference model failed", err)
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("upsert inference model: %w", err)
	}
	return record, nil
}

func (r *InferenceModelRepository) ReadByID(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceModelRepository ReadByID")

	query := `SELECT ` + modelColumns() + ` FROM ` + r.Name + `.inference_models WHERE model_id = @model_id AND (org_id = @org_id OR org_id IS NULL)`
	record, err := scanInferenceModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id": pgtype.UUID{Bytes: modelID, Valid: true},
		"org_id":   pgtype.UUID{Bytes: orgID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "read inference model failed", err)
		return nil, fmt.Errorf("read inference model: %w", err)
	}
	return record, nil
}

func modelColumns() string {
	log.Trace("modelColumns")

	return `model_id::text, COALESCE(user_id::text, ''), COALESCE(org_id::text, ''), COALESCE(training_run_id::text, ''), COALESCE(dataset_id::text, ''),
		model_kind::text, source::text, source_uri, source_metadata::text,
		name, lineage_name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, COALESCE(serving_protocol::text, ''), serving_load_status::text, effective_base_id, metrics_metadata::text,
		status::text, failure_reason`
}

func modelArgs(inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("modelArgs")

	return pgx.NamedArgs{
		"model_id":            pgtype.UUID{Bytes: inferenceModel.ModelID, Valid: true},
		"user_id":             nullableUUID(inferenceModel.UserID),
		"org_id":              nullableUUID(inferenceModel.OrgID),
		"training_run_id":     nullableUUID(inferenceModel.TrainingRunID),
		"dataset_id":          nullableUUID(inferenceModel.DatasetID),
		"idempotency_key":     pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"model_kind":          inferenceModel.ModelKind.String(),
		"source":              inferenceModel.Source.String(),
		"source_uri":          inferenceModel.SourceURI,
		"source_metadata":     jsonObjectOrDefault(inferenceModel.SourceMetadata),
		"name":                inferenceModel.Name,
		"lineage_name":        lineageNameForInferenceModel(inferenceModel),
		"model_version":       inferenceModel.ModelVersion,
		"base_model":          inferenceModel.BaseModel,
		"artifact_location":   inferenceModel.ArtifactLocation,
		"artifact_format":     inferenceModel.ArtifactFormat,
		"artifact_checksum":   inferenceModel.ArtifactChecksum,
		"artifact_size_bytes": inferenceModel.ArtifactSizeBytes,
		"adapter_uri":         inferenceModel.AdapterURI,
		"serving_target":      inferenceModel.ServingTarget,
		"serving_model":       inferenceModel.ServingModel,
		"serving_protocol":    inferenceModel.ServingProtocol.String(),
		"serving_load_status": inferenceModel.ServingLoadStatus.String(),
		"effective_base_id":   inferenceModel.EffectiveBaseID,
		"metrics_metadata":    inferenceModel.MetricsMetadata,
		"status":              inferenceModel.Status.String(),
		"failure_reason":      inferenceModel.FailureReason,
	}
}

func lineageNameForInferenceModel(inferenceModel *model.InferenceModel) string {
	log.Trace("lineageNameForInferenceModel")

	lineageName := strings.TrimSpace(inferenceModel.LineageName)
	if lineageName == "" {
		lineageName = strings.TrimSpace(inferenceModel.Name)
	}
	return lineageName
}

func scanInferenceModel(row pgx.Row) (*model.InferenceModel, error) {
	log.Trace("scanInferenceModel")

	var modelID string
	var userID string
	var orgID string
	var trainingRunID string
	var datasetID string
	var modelKindRaw string
	var sourceRaw string
	var statusRaw string
	var servingProtocolRaw string
	var servingLoadStatusRaw string
	record := &model.InferenceModel{}
	if err := row.Scan(
		&modelID,
		&userID,
		&orgID,
		&trainingRunID,
		&datasetID,
		&modelKindRaw,
		&sourceRaw,
		&record.SourceURI,
		&record.SourceMetadata,
		&record.Name,
		&record.LineageName,
		&record.ModelVersion,
		&record.BaseModel,
		&record.ArtifactLocation,
		&record.ArtifactFormat,
		&record.ArtifactChecksum,
		&record.ArtifactSizeBytes,
		&record.AdapterURI,
		&record.ServingTarget,
		&record.ServingModel,
		&servingProtocolRaw,
		&servingLoadStatusRaw,
		&record.EffectiveBaseID,
		&record.MetricsMetadata,
		&statusRaw,
		&record.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToModelStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	servingLoadStatus, err := model.ToModelLoadStatus(servingLoadStatusRaw)
	if err != nil {
		return nil, err
	}
	servingProtocol, err := model.ToServingProtocol(servingProtocolRaw)
	if err != nil {
		return nil, err
	}
	record.ModelID = uuid.MustParse(modelID)
	record.UserID = parseOptionalUUID(userID)
	record.OrgID = parseOptionalUUID(orgID)
	record.TrainingRunID = parseOptionalUUID(trainingRunID)
	record.DatasetID = parseOptionalUUID(datasetID)
	record.ModelKind = model.ToModelKind(modelKindRaw)
	record.Source = model.ToModelSource(sourceRaw)
	record.Status = status
	record.ServingProtocol = servingProtocol
	record.ServingLoadStatus = servingLoadStatus
	return record, nil
}
