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
	"lib/shared_lib/uuidutil"

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
	outbox       msgConn.OrderedOutbox
	topic        string
	outboxSignal func()
}

type ModelRepositoryOption func(*ModelRepository)

func WithTransactionalOutbox(outbox msgConn.OrderedOutbox, topic string) ModelRepositoryOption {
	log.Trace("WithTransactionalOutbox")

	return func(r *ModelRepository) {
		r.outbox = outbox
		r.topic = topic
	}
}

func WithOutboxSignal(signal func()) ModelRepositoryOption {
	log.Trace("WithOutboxSignal")

	return func(r *ModelRepository) {
		r.outboxSignal = signal
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
		model_id, user_id, idempotency_key, training_run_id, dataset_id, model_kind, source, source_uri, source_metadata,
		name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, serving_load_status,
		metrics_metadata, status, failure_reason
	) VALUES (
		@model_id, @user_id, @idempotency_key, @training_run_id, @dataset_id, @model_kind, @source, @source_uri, @source_metadata::jsonb,
		@name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes,
		@adapter_uri, @serving_target, @serving_model, @serving_load_status,
		@metrics_metadata::jsonb, @status, @failure_reason
	)
	RETURNING ` + modelColumns()

	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, modelArgs(registeredModel, idempotencyKey)))
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

func (r *ModelRepository) createTx(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository createTx")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin model create transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO ` + r.Name + `.models (
		model_id, user_id, idempotency_key, training_run_id, dataset_id, model_kind, source, source_uri, source_metadata,
		name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, serving_load_status,
		metrics_metadata, status, failure_reason
	) VALUES (
		@model_id, @user_id, @idempotency_key, @training_run_id, @dataset_id, @model_kind, @source, @source_uri, @source_metadata::jsonb,
		@name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes,
		@adapter_uri, @serving_target, @serving_model, @serving_load_status,
		@metrics_metadata::jsonb, @status, @failure_reason
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
		return nil, fmt.Errorf("insert model: %w", err)
	}
	if err := r.outbox.EnqueueTx(ctx, tx, modelUpdatedMessage(r.topic, modelRecord)); err != nil {
		return nil, fmt.Errorf("enqueue model updated: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit model create transaction: %w", err)
	}
	r.notifyOutbox()
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

func (r *ModelRepository) UpdateServingStatus(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, failureReason string, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository UpdateServingStatus")

	if r.outbox != nil {
		return r.updateServingStatusTx(ctx, modelID, status, servingLoadStatus, servingTarget, servingModel, failureReason, idempotencyKey)
	}

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
	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, servingStatusArgs(modelID, status, servingLoadStatus, servingTarget, servingModel, failureReason, idempotencyKey)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.ReadByID(ctx, modelID)
		}
		r.LogPoolStatsOnError(ctx, "update model serving status failed", err)
		return nil, fmt.Errorf("update model serving status: %w", err)
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
	r.notifyOutbox()
	return modelRecord, nil
}

func (r *ModelRepository) updateServingStatusTx(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, failureReason string, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRepository updateServingStatusTx")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin model serving status transaction: %w", err)
	}
	defer tx.Rollback(ctx)

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
			if readErr != nil {
				return nil, readErr
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit unchanged model serving status transaction: %w", err)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("update model serving status: %w", err)
	}
	if err := r.outbox.EnqueueTx(ctx, tx, modelUpdatedMessage(r.topic, modelRecord)); err != nil {
		return nil, fmt.Errorf("enqueue model updated: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit model serving status transaction: %w", err)
	}
	r.notifyOutbox()
	return modelRecord, nil
}

func (r *ModelRepository) notifyOutbox() {
	log.Trace("ModelRepository notifyOutbox")

	if r.outboxSignal != nil {
		r.outboxSignal()
	}
}

func (r *ModelRepository) Close() {
	log.Trace("ModelRepository Close")

	r.Database.Close()
}

func modelArgs(registeredModel *model.Model, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("modelArgs")

	return pgx.NamedArgs{
		"model_id":            pgtype.UUID{Bytes: registeredModel.ModelID, Valid: true},
		"user_id":             pgtype.UUID{Bytes: registeredModel.UserID, Valid: registeredModel.UserID != uuid.Nil},
		"idempotency_key":     pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"training_run_id":     pgtype.UUID{Bytes: registeredModel.TrainingRunID, Valid: registeredModel.TrainingRunID != uuid.Nil},
		"dataset_id":          pgtype.UUID{Bytes: registeredModel.DatasetID, Valid: registeredModel.DatasetID != uuid.Nil},
		"model_kind":          registeredModel.ModelKind.String(),
		"source":              registeredModel.Source.String(),
		"source_uri":          registeredModel.SourceURI,
		"source_metadata":     withDefaultJSON(registeredModel.SourceMetadata),
		"name":                registeredModel.Name,
		"model_version":       registeredModel.ModelVersion,
		"base_model":          registeredModel.BaseModel,
		"artifact_location":   registeredModel.ArtifactLocation,
		"artifact_format":     registeredModel.ArtifactFormat,
		"artifact_checksum":   registeredModel.ArtifactChecksum,
		"artifact_size_bytes": registeredModel.ArtifactSizeBytes,
		"adapter_uri":         registeredModel.AdapterURI,
		"serving_target":      registeredModel.ServingTarget,
		"serving_model":       registeredModel.ServingModel,
		"serving_load_status": registeredModel.ServingLoadStatus.String(),
		"metrics_metadata":    registeredModel.MetricsMetadata,
		"status":              registeredModel.Status.String(),
		"failure_reason":      registeredModel.FailureReason,
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
		metrics_metadata::text, status::text, failure_reason`
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

func modelUpdatedMessage(topic string, modelRecord *model.Model) msgConn.OutboundMessage {
	log.Trace("modelUpdatedMessage")

	payload := mustMarshal(&modelregistrypb.ModelUpdatedEvent{
		ModelId:           modelRecord.ModelID.String(),
		TrainingRunId:     uuidutil.StringOrEmpty(modelRecord.TrainingRunID),
		DatasetId:         uuidutil.StringOrEmpty(modelRecord.DatasetID),
		ModelKind:         modelRecord.ModelKind.String(),
		Source:            modelRecord.Source.String(),
		SourceUri:         modelRecord.SourceURI,
		SourceMetadata:    withDefaultJSON(modelRecord.SourceMetadata),
		Name:              modelRecord.Name,
		ModelVersion:      int32(modelRecord.ModelVersion),
		BaseModel:         modelRecord.BaseModel,
		ArtifactLocation:  modelRecord.ArtifactLocation,
		ArtifactFormat:    modelRecord.ArtifactFormat,
		ArtifactChecksum:  modelRecord.ArtifactChecksum,
		ArtifactSizeBytes: modelRecord.ArtifactSizeBytes,
		AdapterUri:        modelRecord.AdapterURI,
		ServingTarget:     modelRecord.ServingTarget,
		ServingModel:      modelRecord.ServingModel,
		ServingLoadStatus: modelRecord.ServingLoadStatus.String(),
		MetricsMetadata:   modelRecord.MetricsMetadata,
		Status:            modelRecord.Status.String(),
		FailureReason:     modelRecord.FailureReason,
		UserId:            uuidutil.StringOrEmpty(modelRecord.UserID),
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

func withDefaultJSON(value string) string {
	if value == "" {
		return "{}"
	}
	return value
}

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}
