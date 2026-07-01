package db

import (
	"context"
	"errors"
	"fmt"

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
		model_id, training_run_id, dataset_id, idempotency_key, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes, metrics_metadata, status, failure_reason
	) VALUES (
		@model_id, @training_run_id, @dataset_id, @idempotency_key, @name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes, @metrics_metadata::jsonb, @status, @failure_reason
	)
	ON CONFLICT (model_id) DO UPDATE SET
		training_run_id = EXCLUDED.training_run_id,
		dataset_id = EXCLUDED.dataset_id,
		idempotency_key = EXCLUDED.idempotency_key,
		name = EXCLUDED.name,
		model_version = EXCLUDED.model_version,
		base_model = EXCLUDED.base_model,
		artifact_location = EXCLUDED.artifact_location,
		artifact_format = EXCLUDED.artifact_format,
		artifact_checksum = EXCLUDED.artifact_checksum,
		artifact_size_bytes = EXCLUDED.artifact_size_bytes,
		metrics_metadata = EXCLUDED.metrics_metadata,
		status = EXCLUDED.status,
		failure_reason = EXCLUDED.failure_reason
	RETURNING ` + modelColumns()

	record, err := scanInferenceModel(r.Pool.QueryRow(ctx, query, modelArgs(inferenceModel, idempotencyKey)))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "upsert inference model failed", err)
		return nil, fmt.Errorf("upsert inference model: %w", err)
	}
	return record, nil
}

func (r *InferenceModelRepository) ReadByID(ctx context.Context, modelID uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceModelRepository ReadByID")

	query := `SELECT ` + modelColumns() + ` FROM ` + r.Name + `.inference_models WHERE model_id = @model_id`
	record, err := scanInferenceModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id": pgtype.UUID{Bytes: modelID, Valid: true},
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

	return `model_id::text, training_run_id::text, dataset_id::text, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes, metrics_metadata::text,
		status::text, failure_reason`
}

func modelArgs(inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("modelArgs")

	return pgx.NamedArgs{
		"model_id":            pgtype.UUID{Bytes: inferenceModel.ModelID, Valid: true},
		"training_run_id":     pgtype.UUID{Bytes: inferenceModel.TrainingRunID, Valid: true},
		"dataset_id":          pgtype.UUID{Bytes: inferenceModel.DatasetID, Valid: true},
		"idempotency_key":     pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"name":                inferenceModel.Name,
		"model_version":       inferenceModel.ModelVersion,
		"base_model":          inferenceModel.BaseModel,
		"artifact_location":   inferenceModel.ArtifactLocation,
		"artifact_format":     inferenceModel.ArtifactFormat,
		"artifact_checksum":   inferenceModel.ArtifactChecksum,
		"artifact_size_bytes": inferenceModel.ArtifactSizeBytes,
		"metrics_metadata":    inferenceModel.MetricsMetadata,
		"status":              inferenceModel.Status.String(),
		"failure_reason":      inferenceModel.FailureReason,
	}
}

func scanInferenceModel(row pgx.Row) (*model.InferenceModel, error) {
	log.Trace("scanInferenceModel")

	var modelID string
	var trainingRunID string
	var datasetID string
	var statusRaw string
	record := &model.InferenceModel{}
	if err := row.Scan(
		&modelID,
		&trainingRunID,
		&datasetID,
		&record.Name,
		&record.ModelVersion,
		&record.BaseModel,
		&record.ArtifactLocation,
		&record.ArtifactFormat,
		&record.ArtifactChecksum,
		&record.ArtifactSizeBytes,
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
	record.ModelID = uuid.MustParse(modelID)
	record.TrainingRunID = uuid.MustParse(trainingRunID)
	record.DatasetID = uuid.MustParse(datasetID)
	record.Status = status
	return record, nil
}
