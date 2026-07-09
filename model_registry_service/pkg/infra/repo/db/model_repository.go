package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"
	transport "lib/shared_lib/transport"

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

	query := `WITH tenant_projection AS (
		SELECT id AS user_id
		FROM ` + r.Name + `.tenants
		WHERE id = @user_id AND deleted = false AND @user_id::uuid IS NOT NULL
	)
	INSERT INTO ` + r.Name + `.models (
		model_id, user_id, org_id, idempotency_key, training_run_id, dataset_id, model_kind, source, source_uri, source_metadata,
		name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, serving_protocol, serving_load_status,
		metrics_metadata, promotion_report_uri, promotion_deltas, promotion_decision, promotion_reason, status, failure_reason
	)
		SELECT
		@model_id,
		tenant_projection.user_id,
		@org_id::uuid,
		@idempotency_key, @training_run_id, @dataset_id, @model_kind::model_kind_enum, @source::model_source_enum, @source_uri, @source_metadata::jsonb,
		@name, @model_version, @base_model,
		@artifact_location, @artifact_format, @artifact_checksum, @artifact_size_bytes,
		@adapter_uri, @serving_target, @serving_model, NULLIF(@serving_protocol, '')::serving_protocol_enum, @serving_load_status::model_load_status_enum,
		@metrics_metadata::jsonb, @promotion_report_uri, @promotion_deltas::jsonb, NULLIF(@promotion_decision, '')::promotion_decision_enum, @promotion_reason, @status::model_status_enum, @failure_reason
	FROM tenant_projection
	RETURNING ` + modelColumns()

	modelRecord, err := scanModel(tx.QueryRow(ctx, query, modelArgs(registeredModel, idempotencyKey)))
	if err != nil {
		if isUniqueViolation(err) {
			return nil, domain.ErrModelExists
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: tenant projection is not ready", domain.ErrValidationFailed)
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
		WHERE org_id = @org_id
			AND name = @name
			AND status = @status::model_status_enum
			AND serving_load_status = @serving_load_status::model_load_status_enum
		ORDER BY model_version DESC, created_at DESC, model_id DESC
		LIMIT 1`
	modelRecord, err := scanModel(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":              pgtype.UUID{Bytes: lineage.OrgID, Valid: lineage.OrgID != uuid.Nil},
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

func (r *ModelRepository) List(ctx context.Context, pagination transport.Pagination, filter model.ListFilter) ([]*model.Model, int, error) {
	log.Trace("ModelRepository List")

	args := modelListArgs(ctx, pagination, filter)
	where := ` WHERE (@model_kind = '' OR model_kind::text = @model_kind)
		AND (@source = '' OR source::text = @source)
		AND (@status = '' OR status::text = @status)
		AND (
			@trainable = false
			OR (
				status = 'READY'::model_status_enum
				AND (
					model_kind = 'BASE'::model_kind_enum
					OR org_id = @org_id
				)
			)
		)`
	countQuery := `SELECT count(*) FROM ` + r.Name + `.models` + where
	var total int
	if err := r.Pool.QueryRow(ctx, countQuery, args).Scan(&total); err != nil {
		r.LogPoolStatsOnError(ctx, "count models failed", err)
		return nil, 0, fmt.Errorf("count models: %w", err)
	}

	query := `SELECT ` + modelColumns() + ` FROM ` + r.Name + `.models` + where + `
		ORDER BY updated_at DESC, model_version DESC, name ASC
		LIMIT @limit OFFSET @offset`
	rows, err := r.Pool.Query(ctx, query, args)
	if err != nil {
		r.LogPoolStatsOnError(ctx, "list models failed", err)
		return nil, 0, fmt.Errorf("list models: %w", err)
	}
	models, err := scanModels(rows)
	if err != nil {
		r.LogPoolStatsOnError(ctx, "scan models failed", err)
		return nil, 0, fmt.Errorf("scan models: %w", err)
	}
	return models, total, nil
}

func (r *ModelRepository) UpdateStatus(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRepository UpdateStatus")

	query := `UPDATE ` + r.Name + `.models
		SET status = @status::model_status_enum,
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

func (r *ModelRepository) UpdateServingStatus(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, servingProtocol model.ServingProtocol, failureReason string, idempotencyKey uuid.UUID) (*model.Model, bool, error) {
	log.Trace("ModelRepository UpdateServingStatus")

	query := `UPDATE ` + r.Name + `.models
		SET status = @status::model_status_enum,
			serving_load_status = @serving_load_status::model_load_status_enum,
			serving_target = @serving_target,
			serving_model = @serving_model,
			serving_protocol = NULLIF(@serving_protocol, '')::serving_protocol_enum,
			failure_reason = @failure_reason,
			serving_status_idempotency_key = @serving_status_idempotency_key
		WHERE model_id = @model_id
			AND serving_status_idempotency_key IS DISTINCT FROM @serving_status_idempotency_key
			AND (
				status IS DISTINCT FROM @status::model_status_enum
				OR serving_load_status IS DISTINCT FROM @serving_load_status::model_load_status_enum
				OR serving_target IS DISTINCT FROM @serving_target
				OR serving_model IS DISTINCT FROM @serving_model
				OR serving_protocol IS DISTINCT FROM NULLIF(@serving_protocol, '')::serving_protocol_enum
				OR failure_reason IS DISTINCT FROM @failure_reason
			)
		RETURNING ` + modelColumns()
	modelRecord, err := scanModel(tx.QueryRow(ctx, query, servingStatusArgs(modelID, status, servingLoadStatus, servingTarget, servingModel, servingProtocol, failureReason, idempotencyKey)))
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
		SET status = @status::model_status_enum,
			promotion_report_uri = @promotion_report_uri,
			promotion_deltas = @promotion_deltas::jsonb,
			promotion_decision = NULLIF(@promotion_decision, '')::promotion_decision_enum,
			promotion_reason = @promotion_reason,
			failure_reason = @failure_reason
		WHERE model_id = @model_id
		RETURNING ` + modelColumns()
	promotionOutcome, promotionReason := splitPromotionDecision(promotionDecision)
	modelRecord, err := scanModel(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id":             pgtype.UUID{Bytes: modelID, Valid: true},
		"status":               status.String(),
		"promotion_report_uri": promotionReportURI,
		"promotion_deltas":     withDefaultJSON(promotionDeltas),
		"promotion_decision":   promotionOutcome,
		"promotion_reason":     promotionReason,
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
		"org_id":               pgtype.UUID{Bytes: registeredModel.OrgID, Valid: registeredModel.OrgID != uuid.Nil},
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
		"serving_protocol":     registeredModel.ServingProtocol.String(),
		"serving_load_status":  registeredModel.ServingLoadStatus.String(),
		"metrics_metadata":     registeredModel.MetricsMetadata,
		"promotion_report_uri": registeredModel.PromotionReportURI,
		"promotion_deltas":     withDefaultJSON(registeredModel.PromotionDeltas),
		"promotion_decision":   promotionDecisionOutcome(registeredModel.PromotionDecision),
		"promotion_reason":     registeredModel.PromotionReason,
		"status":               registeredModel.Status.String(),
		"failure_reason":       registeredModel.FailureReason,
	}
}

func servingStatusArgs(modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, servingProtocol model.ServingProtocol, failureReason string, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("servingStatusArgs")

	return pgx.NamedArgs{
		"model_id":                       pgtype.UUID{Bytes: modelID, Valid: true},
		"status":                         status.String(),
		"serving_load_status":            servingLoadStatus.String(),
		"serving_target":                 servingTarget,
		"serving_model":                  servingModel,
		"serving_protocol":               servingProtocol.String(),
		"failure_reason":                 failureReason,
		"serving_status_idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
	}
}

func modelListArgs(ctx context.Context, pagination transport.Pagination, filter model.ListFilter) pgx.NamedArgs {
	log.Trace("modelListArgs")

	modelKind := ""
	if filter.KindSet {
		modelKind = filter.Kind.String()
	}
	source := ""
	if filter.SourceSet {
		source = filter.Source.String()
	}
	status := ""
	if filter.StatusSet {
		status = filter.Status.String()
	}
	return pgx.NamedArgs{
		"model_kind": modelKind,
		"source":     source,
		"status":     status,
		"org_id":     pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
		"trainable":  filter.Trainable,
		"limit":      pagination.Limit,
		"offset":     pagination.GetOffset(),
	}
}

func modelColumns() string {
	log.Trace("modelColumns")

	return `model_id::text, COALESCE(user_id::text, ''), COALESCE(org_id::text, ''), COALESCE(training_run_id::text, ''), COALESCE(dataset_id::text, ''),
		model_kind::text, source::text, source_uri, source_metadata::text, name, model_version, base_model,
		artifact_location, artifact_format, artifact_checksum, artifact_size_bytes,
		adapter_uri, serving_target, serving_model, COALESCE(serving_protocol::text, ''), serving_load_status::text,
		metrics_metadata::text, promotion_report_uri, promotion_deltas::text, COALESCE(promotion_decision::text, ''), promotion_reason, status::text, failure_reason`
}

func scanModels(rows pgx.Rows) ([]*model.Model, error) {
	log.Trace("scanModels")

	defer rows.Close()
	models := []*model.Model{}
	for rows.Next() {
		modelRecord, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		models = append(models, modelRecord)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return models, nil
}

func scanModel(row pgx.Row) (*model.Model, error) {
	log.Trace("scanModel")

	var modelID, userID, orgID, trainingRunID, datasetID, modelKindRaw, modelSourceRaw, statusRaw, servingProtocolRaw, servingLoadStatusRaw string
	modelRecord := &model.Model{}
	if err := row.Scan(
		&modelID,
		&userID,
		&orgID,
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
		&servingProtocolRaw,
		&servingLoadStatusRaw,
		&modelRecord.MetricsMetadata,
		&modelRecord.PromotionReportURI,
		&modelRecord.PromotionDeltas,
		&modelRecord.PromotionDecision,
		&modelRecord.PromotionReason,
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
	servingProtocol, err := model.ToServingProtocol(servingProtocolRaw)
	if err != nil {
		return nil, err
	}
	modelRecord.ModelID = uuid.MustParse(modelID)
	if userID != "" {
		modelRecord.UserID = uuid.MustParse(userID)
	}
	if orgID != "" {
		modelRecord.OrgID = uuid.MustParse(orgID)
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
	modelRecord.ServingProtocol = servingProtocol
	modelRecord.ServingLoadStatus = servingLoadStatus
	return modelRecord, nil
}

func orgIDFromContext(ctx context.Context) uuid.UUID {
	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return uuid.Nil
	}
	return orgID
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

func promotionDecisionOutcome(decision string) string {
	log.Trace("promotionDecisionOutcome")

	outcome, _ := splitPromotionDecision(decision)
	return outcome
}

func splitPromotionDecision(decision string) (string, string) {
	log.Trace("splitPromotionDecision")

	parts := strings.SplitN(strings.TrimSpace(decision), ":", 2)
	outcome := strings.TrimSpace(parts[0])
	reason := ""
	if len(parts) > 1 {
		reason = strings.TrimSpace(parts[1])
	}
	return outcome, reason
}
