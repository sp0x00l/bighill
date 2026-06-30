package db

import (
	"context"
	"errors"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type SnapshotRepository struct {
	coreDB.Database
}

func NewSnapshotRepository(db *coreDB.Database) *SnapshotRepository {
	log.Trace("NewSnapshotRepository")

	return &SnapshotRepository{
		Database: *db,
	}
}

func (r *SnapshotRepository) SavePendingRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingRawSnapshot")

	query := `INSERT INTO ` + r.Name + `.raw_snapshots (
		dataset_id, user_id, idempotency_key, source_storage_location, storage_location, content_type, file_extension,
		table_namespace, table_name, table_format, catalog_provider, status
	) VALUES (
		@dataset_id, @user_id, @idempotency_key, @source_storage_location, @storage_location, @content_type, @file_extension,
		@table_namespace, @table_name, @table_format, @catalog_provider, @status
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + rawSnapshotColumns()

	row := r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"dataset_id":              pgtype.UUID{Bytes: datasetFile.DatasetID, Valid: true},
		"user_id":                 pgtype.UUID{Bytes: datasetFile.UserID, Valid: true},
		"idempotency_key":         pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"source_storage_location": datasetFile.StorageLocation,
		"storage_location":        datasetFile.StorageLocation,
		"content_type":            datasetFile.ContentType,
		"file_extension":          datasetFile.FileExtension,
		"table_namespace":         datasetFile.TableNamespace,
		"table_name":              datasetFile.TableName,
		"table_format":            datasetFile.TableFormat,
		"catalog_provider":        datasetFile.CatalogProvider,
		"status":                  model.SnapshotStatusPending.String(),
	})
	rawSnapshot, err := scanRawSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, r.rawSnapshotAlreadyMaterializedByIdempotencyKey(ctx, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "insert raw snapshot failed", err)
		return nil, fmt.Errorf("insert raw snapshot: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) MarkRawReady(ctx context.Context, rawSnapshotID uuid.UUID, storageLocation string) error {
	log.Trace("SnapshotRepository MarkRawReady")

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status, storage_location = @storage_location, failure_reason = NULL
		WHERE raw_snapshot_id = @raw_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":  pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"storage_location": storageLocation,
		"status":           model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark raw snapshot ready failed", err)
		return fmt.Errorf("mark raw snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotNotFound, rawSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) MarkRawFailed(ctx context.Context, rawSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkRawFailed")

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status, failure_reason = @failure_reason
		WHERE raw_snapshot_id = @raw_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id": pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"failure_reason":  reason,
		"status":          model.SnapshotStatusFailed.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark raw snapshot failed failed", err)
		return fmt.Errorf("mark raw snapshot failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotNotFound, rawSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) ReadRawByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository ReadRawByIdempotencyKey")

	query := `SELECT ` + rawSnapshotColumns() + ` FROM ` + r.Name + `.raw_snapshots WHERE idempotency_key = @idempotency_key`
	rawSnapshot, err := scanRawSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: idempotency_key=%s", domain.ErrRawSnapshotNotFound, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "read raw snapshot by idempotency key failed", err)
		return nil, fmt.Errorf("read raw snapshot by idempotency key: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) ReadRawSnapshot(ctx context.Context, rawSnapshotID uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository ReadRawSnapshot")

	query := `SELECT ` + rawSnapshotColumns() + ` FROM ` + r.Name + `.raw_snapshots WHERE raw_snapshot_id = @raw_snapshot_id`
	rawSnapshot, err := scanRawSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id": pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotNotFound, rawSnapshotID)
		}
		r.LogPoolStatsOnError(ctx, "read raw snapshot failed", err)
		return nil, fmt.Errorf("read raw snapshot: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) SavePendingFeatureSnapshot(ctx context.Context, rawSnapshotID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingFeatureSnapshot")

	rawSnapshot, err := r.ReadRawSnapshot(ctx, rawSnapshotID)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO ` + r.Name + `.feature_snapshots (
		raw_snapshot_id, dataset_id, idempotency_key, table_namespace, table_name, table_format, catalog_provider, status
	) VALUES (
		@raw_snapshot_id, @dataset_id, @idempotency_key, @table_namespace, @table_name, @table_format, @catalog_provider, @status
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + featureSnapshotColumns()

	featureSnapshot, err := scanFeatureSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":  pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"dataset_id":       pgtype.UUID{Bytes: rawSnapshot.DatasetID, Valid: true},
		"idempotency_key":  pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"table_namespace":  rawSnapshot.TableNamespace,
		"table_name":       rawSnapshot.TableName,
		"table_format":     rawSnapshot.TableFormat,
		"catalog_provider": rawSnapshot.CatalogProvider,
		"status":           model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, r.featureSnapshotAlreadyBuiltByIdempotencyKey(ctx, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "insert feature snapshot failed", err)
		return nil, fmt.Errorf("insert feature snapshot: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) MarkFeatureReady(ctx context.Context, featureSnapshotID uuid.UUID, storageLocation string) error {
	log.Trace("SnapshotRepository MarkFeatureReady")

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status, storage_location = @storage_location, failure_reason = NULL
		WHERE feature_snapshot_id = @feature_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"storage_location":    storageLocation,
		"status":              model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark feature snapshot ready failed", err)
		return fmt.Errorf("mark feature snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotNotFound, featureSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) MarkFeatureFailed(ctx context.Context, featureSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkFeatureFailed")

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status, failure_reason = @failure_reason
		WHERE feature_snapshot_id = @feature_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"failure_reason":      reason,
		"status":              model.SnapshotStatusFailed.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark feature snapshot failed failed", err)
		return fmt.Errorf("mark feature snapshot failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotNotFound, featureSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) ReadFeatureByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository ReadFeatureByIdempotencyKey")

	query := `SELECT ` + featureSnapshotColumns() + ` FROM ` + r.Name + `.feature_snapshots WHERE idempotency_key = @idempotency_key`
	featureSnapshot, err := scanFeatureSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: idempotency_key=%s", domain.ErrFeatureSnapshotNotFound, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "read feature snapshot by idempotency key failed", err)
		return nil, fmt.Errorf("read feature snapshot by idempotency key: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) ReadFeatureSnapshot(ctx context.Context, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository ReadFeatureSnapshot")

	query := `SELECT ` + featureSnapshotColumns() + ` FROM ` + r.Name + `.feature_snapshots WHERE feature_snapshot_id = @feature_snapshot_id`
	featureSnapshot, err := scanFeatureSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotNotFound, featureSnapshotID)
		}
		r.LogPoolStatsOnError(ctx, "read feature snapshot failed", err)
		return nil, fmt.Errorf("read feature snapshot: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) SavePendingEmbeddingSnapshot(ctx context.Context, featureSnapshotID, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingEmbeddingSnapshot")

	featureSnapshot, err := r.ReadFeatureSnapshot(ctx, featureSnapshotID)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO ` + r.Name + `.embedding_snapshots (
		feature_snapshot_id, dataset_id, idempotency_key, status
	) VALUES (
		@feature_snapshot_id, @dataset_id, @idempotency_key, @status
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + embeddingSnapshotColumns()

	embeddingSnapshot, err := scanEmbeddingSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"dataset_id":          pgtype.UUID{Bytes: featureSnapshot.DatasetID, Valid: true},
		"idempotency_key":     pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"status":              model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, r.embeddingsAlreadyMaterializedByIdempotencyKey(ctx, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "insert embedding snapshot failed", err)
		return nil, fmt.Errorf("insert embedding snapshot: %w", err)
	}
	return embeddingSnapshot, nil
}

func (r *SnapshotRepository) MarkEmbeddingReady(ctx context.Context, embeddingSnapshotID uuid.UUID, vectorStore, collectionName string) error {
	log.Trace("SnapshotRepository MarkEmbeddingReady")

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status, vector_store = @vector_store, collection_name = @collection_name, failure_reason = NULL
		WHERE embedding_snapshot_id = @embedding_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshotID, Valid: true},
		"vector_store":          vectorStore,
		"collection_name":       collectionName,
		"status":                model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark embedding snapshot ready failed", err)
		return fmt.Errorf("mark embedding snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotNotFound, embeddingSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) MarkEmbeddingFailed(ctx context.Context, embeddingSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkEmbeddingFailed")

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status, failure_reason = @failure_reason
		WHERE embedding_snapshot_id = @embedding_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshotID, Valid: true},
		"failure_reason":        reason,
		"status":                model.SnapshotStatusFailed.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark embedding snapshot failed failed", err)
		return fmt.Errorf("mark embedding snapshot failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotNotFound, embeddingSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) ReadEmbeddingByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository ReadEmbeddingByIdempotencyKey")

	query := `SELECT ` + embeddingSnapshotColumns() + ` FROM ` + r.Name + `.embedding_snapshots WHERE idempotency_key = @idempotency_key`
	embeddingSnapshot, err := scanEmbeddingSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: idempotency_key=%s", domain.ErrEmbeddingSnapshotNotFound, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "read embedding snapshot by idempotency key failed", err)
		return nil, fmt.Errorf("read embedding snapshot by idempotency key: %w", err)
	}
	return embeddingSnapshot, nil
}

func (r *SnapshotRepository) rawSnapshotAlreadyMaterializedByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) error {
	existing, err := r.ReadRawByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return fmt.Errorf("raw snapshot already materialized but lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	return &domain.RawSnapshotAlreadyMaterializedError{Record: existing}
}

func (r *SnapshotRepository) featureSnapshotAlreadyBuiltByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) error {
	existing, err := r.ReadFeatureByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return fmt.Errorf("feature snapshot already built but lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	return &domain.FeatureSnapshotAlreadyBuiltError{Record: existing}
}

func (r *SnapshotRepository) embeddingsAlreadyMaterializedByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) error {
	existing, err := r.ReadEmbeddingByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return fmt.Errorf("embeddings already materialized but lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	return &domain.EmbeddingsAlreadyMaterializedError{Record: existing}
}

func rawSnapshotColumns() string {
	return `raw_snapshot_id::text, dataset_id::text, user_id::text, storage_location, content_type, file_extension,
		table_namespace, table_name, table_format, catalog_provider, status::text, COALESCE(failure_reason, '')`
}

func featureSnapshotColumns() string {
	return `feature_snapshot_id::text, raw_snapshot_id::text, dataset_id::text, storage_location,
		table_namespace, table_name, table_format, catalog_provider, status::text, COALESCE(failure_reason, '')`
}

func embeddingSnapshotColumns() string {
	return `embedding_snapshot_id::text, feature_snapshot_id::text, dataset_id::text,
		vector_store, collection_name, status::text, COALESCE(failure_reason, '')`
}

func scanRawSnapshot(row pgx.Row) (*model.RawSnapshot, error) {
	var rawSnapshotID, datasetID, userID, statusRaw string
	rawSnapshot := &model.RawSnapshot{}
	if err := row.Scan(
		&rawSnapshotID,
		&datasetID,
		&userID,
		&rawSnapshot.StorageLocation,
		&rawSnapshot.ContentType,
		&rawSnapshot.FileExtension,
		&rawSnapshot.TableNamespace,
		&rawSnapshot.TableName,
		&rawSnapshot.TableFormat,
		&rawSnapshot.CatalogProvider,
		&statusRaw,
		&rawSnapshot.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToSnapshotStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	rawSnapshot.RawSnapshotID = uuid.MustParse(rawSnapshotID)
	rawSnapshot.DatasetID = uuid.MustParse(datasetID)
	rawSnapshot.UserID = uuid.MustParse(userID)
	rawSnapshot.Status = status
	return rawSnapshot, nil
}

func scanFeatureSnapshot(row pgx.Row) (*model.FeatureSnapshot, error) {
	var featureSnapshotID, rawSnapshotID, datasetID, statusRaw string
	featureSnapshot := &model.FeatureSnapshot{}
	if err := row.Scan(
		&featureSnapshotID,
		&rawSnapshotID,
		&datasetID,
		&featureSnapshot.StorageLocation,
		&featureSnapshot.TableNamespace,
		&featureSnapshot.TableName,
		&featureSnapshot.TableFormat,
		&featureSnapshot.CatalogProvider,
		&statusRaw,
		&featureSnapshot.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToSnapshotStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	featureSnapshot.FeatureSnapshotID = uuid.MustParse(featureSnapshotID)
	featureSnapshot.RawSnapshotID = uuid.MustParse(rawSnapshotID)
	featureSnapshot.DatasetID = uuid.MustParse(datasetID)
	featureSnapshot.Status = status
	return featureSnapshot, nil
}

func scanEmbeddingSnapshot(row pgx.Row) (*model.EmbeddingSnapshot, error) {
	var embeddingSnapshotID, featureSnapshotID, datasetID, statusRaw string
	embeddingSnapshot := &model.EmbeddingSnapshot{}
	if err := row.Scan(
		&embeddingSnapshotID,
		&featureSnapshotID,
		&datasetID,
		&embeddingSnapshot.VectorStore,
		&embeddingSnapshot.CollectionName,
		&statusRaw,
		&embeddingSnapshot.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToSnapshotStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	embeddingSnapshot.EmbeddingSnapshotID = uuid.MustParse(embeddingSnapshotID)
	embeddingSnapshot.FeatureSnapshotID = uuid.MustParse(featureSnapshotID)
	embeddingSnapshot.DatasetID = uuid.MustParse(datasetID)
	embeddingSnapshot.Status = status
	return embeddingSnapshot, nil
}
