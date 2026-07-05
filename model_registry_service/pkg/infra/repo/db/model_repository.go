package db

import (
	"context"
	"errors"
	"fmt"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type ModelRepository struct {
	coreDB.Database
}

func NewModelRepository(db *coreDB.Database) *ModelRepository {
	log.Trace("NewModelRepository")

	return &ModelRepository{
		Database: *db,
	}
}

func (r *ModelRepository) Create(ctx context.Context, tx pgx.Tx, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository Create")

	query := `INSERT INTO ` + r.Name + `.models (
		model_id, user_id, idempotency_key, training_run_id, dataset_id, model_kind, source, source_uri, source_metadata,
		name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, serving_load_status,
		metrics_metadata, promotion_report_uri, promotion_deltas, promotion_decision, status, failure_reason
	) VALUES (
		@model_id, @user_id, @idempotency_key, @training_run_id, @dataset_id, @model_kind, @source, @source_uri, @source_metadata::jsonb,
		@name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes,
		@adapter_uri, @serving_target, @serving_model, @serving_load_status,
		@metrics_metadata::jsonb, @promotion_report_uri, @promotion_deltas::jsonb, @promotion_decision, @status, @failure_reason
	)
	RETURNING ` + modelColumns()

	modelRecord, err := scanModel(tx.QueryRow(ctx, query, modelArgs(registeredModel, idempotencyKey)))
	if err != nil {
		if isUniqueViolation(err) {
			return nil, domain.ErrModelExists
		}
		if coreDB.IsForeignKeyViolation(err) {
			return nil, fmt.Errorf("%w: tenant projection is not ready", domain.ErrValidationFailed)
		}
		r.LogPoolStatsOnError(ctx, "insert model failed", err)
		return nil, fmt.Errorf("insert model: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) ReadByID(ctx context.Context, modelID uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository ReadByID")

	query := `SELECT ` + modelColumns() + ` FROM ` + r.Name + `.models WHERE model_id = @model_id`
	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id": pgtype.UUID{Bytes: modelID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "read model failed", err)
		return nil, fmt.Errorf("read model: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) ReadByTrainingRunID(ctx context.Context, trainingRunID uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository ReadByTrainingRunID")

	query := `SELECT ` + modelColumns() + ` FROM ` + r.Name + `.models WHERE training_run_id = @training_run_id`
	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"training_run_id": pgtype.UUID{Bytes: trainingRunID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "read model by training run failed", err)
		return nil, fmt.Errorf("read model by training run: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) ReadChampion(ctx context.Context, lineage model.Lineage) (*model.Model, error) {
	log.Trace("ModelRepository ReadChampion")

	query := `SELECT ` + modelColumns() + ` FROM ` + r.Name + `.models
		WHERE user_id = @user_id
			AND name = @name
			AND status = @status
			AND serving_load_status = @serving_load_status
		ORDER BY model_version DESC
		LIMIT 1`
	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"user_id":             pgtype.UUID{Bytes: lineage.UserID, Valid: lineage.UserID != uuid.Nil},
		"name":                lineage.Name,
		"status":              model.ModelStatusReady.String(),
		"serving_load_status": model.ModelLoadStatusLoaded.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "read champion model failed", err)
		return nil, fmt.Errorf("read champion model: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) UpdateStatus(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRepository UpdateStatus")

	query := `UPDATE ` + r.Name + `.models
		SET status = @status,
			artifact_location = COALESCE(NULLIF(@artifact_location, ''), artifact_location),
			failure_reason = @failure_reason
		WHERE model_id = @model_id
		RETURNING ` + modelColumns()
	modelRecord, err := scanModel(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id":          pgtype.UUID{Bytes: modelID, Valid: true},
		"status":            status.String(),
		"artifact_location": artifactLocation,
		"failure_reason":    failureReason,
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "update model status failed", err)
		return nil, fmt.Errorf("update model status: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) UpdateServingStatus(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, failureReason string, idempotencyKey uuid.UUID) (*model.Model, bool, error) {
	log.Trace("ModelRepository UpdateServingStatus")

	query := `UPDATE ` + r.Name + `.models
		SET status = @status,
			serving_load_status = @serving_load_status,
			serving_target = @serving_target,
			serving_model = @serving_model,
			failure_reason = @failure_reason,
			serving_status_idempotency_key = @serving_status_idempotency_key
		WHERE model_id = @model_id
			AND serving_status_idempotency_key IS DISTINCT FROM @serving_status_idempotency_key
			AND (
				status IS DISTINCT FROM @status
				OR serving_load_status IS DISTINCT FROM @serving_load_status
				OR serving_target IS DISTINCT FROM @serving_target
				OR serving_model IS DISTINCT FROM @serving_model
				OR failure_reason IS DISTINCT FROM @failure_reason
			)
		RETURNING ` + modelColumns()
	modelRecord, err := scanModel(tx.QueryRow(ctx, query, servingStatusArgs(modelID, status, servingLoadStatus, servingTarget, servingModel, failureReason, idempotencyKey)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			existing, readErr := readModelByIDTx(ctx, tx, r.Name, modelID)
			return existing, false, readErr
		}
		return nil, false, fmt.Errorf("update model serving status: %w", err)
	}
	return modelRecord, true, nil
}

func (r *ModelRepository) UpdatePromotionDecision(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, promotionReportURI string, promotionDeltas string, promotionDecision string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRepository UpdatePromotionDecision")

	query := `UPDATE ` + r.Name + `.models
		SET status = @status,
			promotion_report_uri = @promotion_report_uri,
			promotion_deltas = @promotion_deltas::jsonb,
			promotion_decision = @promotion_decision,
			failure_reason = @failure_reason
		WHERE model_id = @model_id
		RETURNING ` + modelColumns()
	modelRecord, err := scanModel(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id":             pgtype.UUID{Bytes: modelID, Valid: true},
		"status":               status.String(),
		"promotion_report_uri": promotionReportURI,
		"promotion_deltas":     withDefaultJSON(promotionDeltas),
		"promotion_decision":   promotionDecision,
		"failure_reason":       failureReason,
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "update model promotion decision failed", err)
		return nil, fmt.Errorf("update model promotion decision: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) Close() {
	log.Trace("ModelRepository Close")

	r.Database.Close()
}

func modelArgs(registeredModel *model.Model, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("modelArgs")

	return pgx.NamedArgs{
		"model_id":             pgtype.UUID{Bytes: registeredModel.ModelID, Valid: true},
		"user_id":              pgtype.UUID{Bytes: registeredModel.UserID, Valid: registeredModel.UserID != uuid.Nil},
		"idempotency_key":      pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"training_run_id":      pgtype.UUID{Bytes: registeredModel.TrainingRunID, Valid: registeredModel.TrainingRunID != uuid.Nil},
		"dataset_id":           pgtype.UUID{Bytes: registeredModel.DatasetID, Valid: registeredModel.DatasetID != uuid.Nil},
		"model_kind":           registeredModel.ModelKind.String(),
		"source":               registeredModel.Source.String(),
		"source_uri":           registeredModel.SourceURI,
		"source_metadata":      withDefaultJSON(registeredModel.SourceMetadata),
		"name":                 registeredModel.Name,
		"model_version":        registeredModel.ModelVersion,
		"base_model":           registeredModel.BaseModel,
		"artifact_location":    registeredModel.ArtifactLocation,
		"artifact_format":      registeredModel.ArtifactFormat,
		"artifact_checksum":    registeredModel.ArtifactChecksum,
		"artifact_size_bytes":  registeredModel.ArtifactSizeBytes,
		"adapter_uri":          registeredModel.AdapterURI,
		"serving_target":       registeredModel.ServingTarget,
		"serving_model":        registeredModel.ServingModel,
		"serving_load_status":  registeredModel.ServingLoadStatus.String(),
		"metrics_metadata":     registeredModel.MetricsMetadata,
		"promotion_report_uri": registeredModel.PromotionReportURI,
		"promotion_deltas":     withDefaultJSON(registeredModel.PromotionDeltas),
		"promotion_decision":   registeredModel.PromotionDecision,
		"status":               registeredModel.Status.String(),
		"failure_reason":       registeredModel.FailureReason,
	}
}

func servingStatusArgs(modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, failureReason string, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("servingStatusArgs")

	return pgx.NamedArgs{
		"model_id":                       pgtype.UUID{Bytes: modelID, Valid: true},
		"status":                         status.String(),
		"serving_load_status":            servingLoadStatus.String(),
		"serving_target":                 servingTarget,
		"serving_model":                  servingModel,
		"failure_reason":                 failureReason,
		"serving_status_idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
	}
}

func modelColumns() string {
	log.Trace("modelColumns")

	return `model_id::text, COALESCE(user_id::text, ''), COALESCE(training_run_id::text, ''), COALESCE(dataset_id::text, ''),
		model_kind::text, source::text, source_uri, source_metadata::text, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, serving_load_status::text,
		metrics_metadata::text, promotion_report_uri, promotion_deltas::text, promotion_decision, status::text, failure_reason`
}

func scanModel(row pgx.Row) (*model.Model, error) {
	log.Trace("scanModel")

	var modelID, userID, trainingRunID, datasetID, modelKindRaw, modelSourceRaw, statusRaw, servingLoadStatusRaw string
	modelRecord := &model.Model{}
	if err := row.Scan(
		&modelID,
		&userID,
		&trainingRunID,
		&datasetID,
		&modelKindRaw,
		&modelSourceRaw,
		&modelRecord.SourceURI,
		&modelRecord.SourceMetadata,
		&modelRecord.Name,
		&modelRecord.ModelVersion,
		&modelRecord.BaseModel,
		&modelRecord.ArtifactLocation,
		&modelRecord.ArtifactFormat,
		&modelRecord.ArtifactChecksum,
		&modelRecord.ArtifactSizeBytes,
		&modelRecord.AdapterURI,
		&modelRecord.ServingTarget,
		&modelRecord.ServingModel,
		&servingLoadStatusRaw,
		&modelRecord.MetricsMetadata,
		&modelRecord.PromotionReportURI,
		&modelRecord.PromotionDeltas,
		&modelRecord.PromotionDecision,
		&statusRaw,
		&modelRecord.FailureReason,
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
	modelRecord.ModelID = uuid.MustParse(modelID)
	if userID != "" {
		modelRecord.UserID = uuid.MustParse(userID)
	}
	if trainingRunID != "" {
		modelRecord.TrainingRunID = uuid.MustParse(trainingRunID)
	}
	if datasetID != "" {
		modelRecord.DatasetID = uuid.MustParse(datasetID)
	}
	modelRecord.ModelKind = model.ToModelKind(modelKindRaw)
	modelRecord.Source = model.ToModelSource(modelSourceRaw)
	modelRecord.Status = status
	modelRecord.ServingLoadStatus = servingLoadStatus
	return modelRecord, nil
}

func readModelByIDTx(ctx context.Context, tx pgx.Tx, schemaName string, modelID uuid.UUID) (*model.Model, error) {
	log.Trace("readModelByIDTx")

	query := `SELECT ` + modelColumns() + ` FROM ` + schemaName + `.models WHERE model_id = @model_id`
	modelRecord, err := scanModel(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id": pgtype.UUID{Bytes: modelID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		return nil, fmt.Errorf("read unchanged model serving status: %w", err)
	}
	return modelRecord, nil
}

func isUniqueViolation(err error) bool {
	log.Trace("isUniqueViolation")

	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

func withDefaultJSON(value string) string {
	if value == "" {
		return "{}"
	}
	return value
}
