package db

import (
	"context"
	"errors"
	"fmt"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ModelRepository struct {
	coreDB.Database
	outbox msgConn.OrderedOutbox
	topic  string
}

type ModelRepositoryOption func(*ModelRepository)

func WithTransactionalOutbox(outbox msgConn.OrderedOutbox, topic string) ModelRepositoryOption {
	log.Trace("WithTransactionalOutbox")

	return func(r *ModelRepository) {
		r.outbox = outbox
		r.topic = topic
	}
}

func NewModelRepository(db *coreDB.Database, opts ...ModelRepositoryOption) *ModelRepository {
	log.Trace("NewModelRepository")

	repository := &ModelRepository{
		Database: *db,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(repository)
		}
	}
	return repository
}

func (r *ModelRepository) Create(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository Create")

	if r.outbox != nil {
		return r.createTx(ctx, registeredModel, idempotencyKey)
	}

	query := `INSERT INTO ` + r.Name + `.models (
		model_id, idempotency_key, training_run_id, dataset_id, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes, metrics_metadata, status, failure_reason
	) VALUES (
		@model_id, @idempotency_key, @training_run_id, @dataset_id, @name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes, @metrics_metadata::jsonb, @status, @failure_reason
	)
	RETURNING ` + modelColumns()

	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, modelArgs(registeredModel, idempotencyKey)))
	if err != nil {
		if isUniqueViolation(err) {
			return nil, domain.ErrModelExists
		}
		r.LogPoolStatsOnError(ctx, "insert model failed", err)
		return nil, fmt.Errorf("insert model: %w", err)
	}
	return modelRecord, nil
}

func (r *ModelRepository) createTx(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository createTx")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin model create transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO ` + r.Name + `.models (
		model_id, idempotency_key, training_run_id, dataset_id, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes, metrics_metadata, status, failure_reason
	) VALUES (
		@model_id, @idempotency_key, @training_run_id, @dataset_id, @name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes, @metrics_metadata::jsonb, @status, @failure_reason
	)
	RETURNING ` + modelColumns()

	modelRecord, err := scanModel(tx.QueryRow(ctx, query, modelArgs(registeredModel, idempotencyKey)))
	if err != nil {
		if isUniqueViolation(err) {
			return nil, domain.ErrModelExists
		}
		return nil, fmt.Errorf("insert model: %w", err)
	}
	if err := r.outbox.EnqueueTx(ctx, tx, modelUpdatedMessage(r.topic, modelRecord)); err != nil {
		return nil, fmt.Errorf("enqueue model updated: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit model create transaction: %w", err)
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

func (r *ModelRepository) UpdateStatus(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRepository UpdateStatus")

	if r.outbox != nil {
		return r.updateStatusTx(ctx, modelID, status, artifactLocation, failureReason)
	}

	query := `UPDATE ` + r.Name + `.models
		SET status = @status,
			artifact_location = COALESCE(NULLIF(@artifact_location, ''), artifact_location),
			failure_reason = @failure_reason
		WHERE model_id = @model_id
		RETURNING ` + modelColumns()
	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
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

func (r *ModelRepository) updateStatusTx(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRepository updateStatusTx")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin model status transaction: %w", err)
	}
	defer tx.Rollback(ctx)

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
		return nil, fmt.Errorf("update model status: %w", err)
	}
	if err := r.outbox.EnqueueTx(ctx, tx, modelUpdatedMessage(r.topic, modelRecord)); err != nil {
		return nil, fmt.Errorf("enqueue model updated: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit model status transaction: %w", err)
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
		"model_id":            pgtype.UUID{Bytes: registeredModel.ModelID, Valid: true},
		"idempotency_key":     pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"training_run_id":     pgtype.UUID{Bytes: registeredModel.TrainingRunID, Valid: true},
		"dataset_id":          pgtype.UUID{Bytes: registeredModel.DatasetID, Valid: true},
		"name":                registeredModel.Name,
		"model_version":       registeredModel.ModelVersion,
		"base_model":          registeredModel.BaseModel,
		"artifact_location":   registeredModel.ArtifactLocation,
		"artifact_format":     registeredModel.ArtifactFormat,
		"artifact_checksum":   registeredModel.ArtifactChecksum,
		"artifact_size_bytes": registeredModel.ArtifactSizeBytes,
		"metrics_metadata":    registeredModel.MetricsMetadata,
		"status":              registeredModel.Status.String(),
		"failure_reason":      registeredModel.FailureReason,
	}
}

func modelColumns() string {
	log.Trace("modelColumns")

	return `model_id::text, training_run_id::text, dataset_id::text, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes, metrics_metadata::text, status::text, failure_reason`
}

func scanModel(row pgx.Row) (*model.Model, error) {
	log.Trace("scanModel")

	var modelID, trainingRunID, datasetID, statusRaw string
	modelRecord := &model.Model{}
	if err := row.Scan(
		&modelID,
		&trainingRunID,
		&datasetID,
		&modelRecord.Name,
		&modelRecord.ModelVersion,
		&modelRecord.BaseModel,
		&modelRecord.ArtifactLocation,
		&modelRecord.ArtifactFormat,
		&modelRecord.ArtifactChecksum,
		&modelRecord.ArtifactSizeBytes,
		&modelRecord.MetricsMetadata,
		&statusRaw,
		&modelRecord.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToModelStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	modelRecord.ModelID = uuid.MustParse(modelID)
	modelRecord.TrainingRunID = uuid.MustParse(trainingRunID)
	modelRecord.DatasetID = uuid.MustParse(datasetID)
	modelRecord.Status = status
	return modelRecord, nil
}

func isUniqueViolation(err error) bool {
	log.Trace("isUniqueViolation")

	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

func modelUpdatedMessage(topic string, modelRecord *model.Model) msgConn.OutboundMessage {
	log.Trace("modelUpdatedMessage")

	payload := mustMarshal(&modelregistrypb.ModelUpdatedEvent{
		ModelId:           modelRecord.ModelID.String(),
		TrainingRunId:     modelRecord.TrainingRunID.String(),
		DatasetId:         modelRecord.DatasetID.String(),
		Name:              modelRecord.Name,
		ModelVersion:      int32(modelRecord.ModelVersion),
		BaseModel:         modelRecord.BaseModel,
		ArtifactLocation:  modelRecord.ArtifactLocation,
		ArtifactFormat:    modelRecord.ArtifactFormat,
		ArtifactChecksum:  modelRecord.ArtifactChecksum,
		ArtifactSizeBytes: modelRecord.ArtifactSizeBytes,
		MetricsMetadata:   modelRecord.MetricsMetadata,
		Status:            modelRecord.Status.String(),
		FailureReason:     modelRecord.FailureReason,
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: modelRecord.ModelID,
			MsgType:     msgConn.MsgTypeModelUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("model_updated:%s:%s:%d", modelRecord.ModelID, modelRecord.Status.String(), modelRecord.ModelVersion),
	}
}

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}
