package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type SnapshotRepository struct {
	coreDB.Database
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func NewSnapshotRepository(db *coreDB.Database) *SnapshotRepository {
	log.Trace("NewSnapshotRepository")

	return &SnapshotRepository{
		Database: *db,
	}
}

func (r *SnapshotRepository) SavePendingRawSnapshot(ctx context.Context, tx pgx.Tx, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingRawSnapshot")

	query := `INSERT INTO ` + r.Name + `.raw_snapshots (
		dataset_id, user_id, org_id, idempotency_key, source_storage_location, storage_location, content_type, file_extension,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, status
	) VALUES (
		@dataset_id, @user_id, @org_id, @idempotency_key, @source_storage_location, @storage_location, @content_type, @file_extension,
		@table_namespace, @table_name, @table_format::table_format_enum, @catalog_provider::catalog_provider_enum, @processing_profile::processing_profile_enum, @status::snapshot_status_enum
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + rawSnapshotColumns()

	row := tx.QueryRow(ctx, query, pgx.NamedArgs{
		"dataset_id":              pgtype.UUID{Bytes: datasetFile.DatasetID, Valid: true},
		"user_id":                 pgtype.UUID{Bytes: datasetFile.UserID, Valid: true},
		"org_id":                  pgtype.UUID{Bytes: datasetFile.OrgID, Valid: datasetFile.OrgID != uuid.Nil},
		"idempotency_key":         pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"source_storage_location": datasetFile.StorageLocation,
		"storage_location":        datasetFile.StorageLocation,
		"content_type":            datasetFile.ContentType,
		"file_extension":          datasetFile.FileExtension,
		"table_namespace":         datasetFile.TableNamespace,
		"table_name":              datasetFile.TableName,
		"table_format":            datasetFile.TableFormat,
		"catalog_provider":        datasetFile.CatalogProvider,
		"processing_profile":      datasetFile.ProcessingProfile.String(),
		"status":                  model.SnapshotStatusPending.String(),
	})
	rawSnapshot, err := scanRawSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.resolveRawSnapshotIdempotencyConflict(ctx, tx, idempotencyKey)
		}
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		r.LogPoolStatsOnError(ctx, "insert raw snapshot failed", err)
		return nil, fmt.Errorf("insert raw snapshot: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) MarkRawReady(ctx context.Context, tx pgx.Tx, rawSnapshot *model.RawSnapshot) error {
	log.Trace("SnapshotRepository MarkRawReady")

	eventSeq, err := r.nextMaterializationEventSeq(ctx, tx, rawSnapshot.DatasetID, rawSnapshot.OrgID)
	if err != nil {
		return err
	}
	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status::snapshot_status_enum,
			materialization_event_seq = @materialization_event_seq,
			storage_location = @storage_location,
			table_format = @table_format::table_format_enum,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			failure_reason = NULL
		WHERE raw_snapshot_id = @raw_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":           pgtype.UUID{Bytes: rawSnapshot.RawSnapshotID, Valid: true},
		"materialization_event_seq": eventSeq,
		"storage_location":          rawSnapshot.StorageLocation,
		"table_format":              rawSnapshot.TableFormat,
		"schema_version":            rawSnapshot.SchemaVersion,
		"schema_metadata":           rawSnapshot.SchemaMetadata,
		"status":                    model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark raw snapshot ready failed", err)
		return fmt.Errorf("mark raw snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotNotFound, rawSnapshot.RawSnapshotID)
	}
	rawSnapshot.MaterializationEventSeq = eventSeq
	return nil
}

func (r *SnapshotRepository) MarkRawFailed(ctx context.Context, tx pgx.Tx, rawSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkRawFailed")

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status::snapshot_status_enum, failure_reason = @failure_reason
		WHERE raw_snapshot_id = @raw_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
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

	return r.readRawByIdempotencyKey(ctx, r.Pool, idempotencyKey)
}

func (r *SnapshotRepository) readRawByIdempotencyKey(ctx context.Context, queryer rowQuerier, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository readRawByIdempotencyKey")

	query := `SELECT ` + rawSnapshotColumns() + ` FROM ` + r.Name + `.raw_snapshots WHERE idempotency_key = @idempotency_key`
	rawSnapshot, err := scanRawSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
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

	return r.readRawSnapshot(ctx, r.Pool, rawSnapshotID)
}

func (r *SnapshotRepository) readRawSnapshot(ctx context.Context, queryer rowQuerier, rawSnapshotID uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository readRawSnapshot")

	query := `SELECT ` + rawSnapshotColumns() + ` FROM ` + r.Name + `.raw_snapshots WHERE raw_snapshot_id = @raw_snapshot_id`
	rawSnapshot, err := scanRawSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
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

func (r *SnapshotRepository) SavePendingFeatureSnapshot(ctx context.Context, tx pgx.Tx, rawSnapshotID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingFeatureSnapshot")

	rawSnapshot, err := r.readRawSnapshot(ctx, tx, rawSnapshotID)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO ` + r.Name + `.feature_snapshots (
		raw_snapshot_id, dataset_id, user_id, org_id, idempotency_key, table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata, status
	) VALUES (
		@raw_snapshot_id, @dataset_id, @user_id, @org_id, @idempotency_key, @table_namespace, @table_name, @table_format::table_format_enum, @catalog_provider::catalog_provider_enum, @processing_profile::processing_profile_enum, @schema_version, @schema_metadata::jsonb, @status::snapshot_status_enum
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + featureSnapshotColumns()

	featureSnapshot, err := scanFeatureSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":    pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"dataset_id":         pgtype.UUID{Bytes: rawSnapshot.DatasetID, Valid: true},
		"user_id":            pgtype.UUID{Bytes: rawSnapshot.UserID, Valid: true},
		"org_id":             pgtype.UUID{Bytes: rawSnapshot.OrgID, Valid: rawSnapshot.OrgID != uuid.Nil},
		"idempotency_key":    pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"table_namespace":    rawSnapshot.TableNamespace,
		"table_name":         rawSnapshot.TableName,
		"table_format":       rawSnapshot.TableFormat,
		"catalog_provider":   rawSnapshot.CatalogProvider,
		"processing_profile": rawSnapshot.ProcessingProfile.String(),
		"schema_version":     rawSnapshot.SchemaVersion,
		"schema_metadata":    rawSnapshot.SchemaMetadata,
		"status":             model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.resolveFeatureSnapshotIdempotencyConflict(ctx, tx, idempotencyKey)
		}
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		r.LogPoolStatsOnError(ctx, "insert feature snapshot failed", err)
		return nil, fmt.Errorf("insert feature snapshot: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) MarkFeatureReady(ctx context.Context, tx pgx.Tx, featureSnapshot *model.FeatureSnapshot) error {
	log.Trace("SnapshotRepository MarkFeatureReady")

	eventSeq, err := r.nextMaterializationEventSeq(ctx, tx, featureSnapshot.DatasetID, featureSnapshot.OrgID)
	if err != nil {
		return err
	}
	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status::snapshot_status_enum,
			materialization_event_seq = @materialization_event_seq,
			storage_location = @storage_location,
			table_format = @table_format::table_format_enum,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			failure_reason = NULL
		WHERE feature_snapshot_id = @feature_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id":       pgtype.UUID{Bytes: featureSnapshot.FeatureSnapshotID, Valid: true},
		"materialization_event_seq": eventSeq,
		"storage_location":          featureSnapshot.StorageLocation,
		"table_format":              featureSnapshot.TableFormat,
		"schema_version":            featureSnapshot.SchemaVersion,
		"schema_metadata":           featureSnapshot.SchemaMetadata,
		"status":                    model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark feature snapshot ready failed", err)
		return fmt.Errorf("mark feature snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotNotFound, featureSnapshot.FeatureSnapshotID)
	}
	featureSnapshot.MaterializationEventSeq = eventSeq
	return nil
}

func (r *SnapshotRepository) MarkFeatureFailed(ctx context.Context, tx pgx.Tx, featureSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkFeatureFailed")

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status::snapshot_status_enum, failure_reason = @failure_reason
		WHERE feature_snapshot_id = @feature_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
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

	return r.readFeatureByIdempotencyKey(ctx, r.Pool, idempotencyKey)
}

func (r *SnapshotRepository) readFeatureByIdempotencyKey(ctx context.Context, queryer rowQuerier, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository readFeatureByIdempotencyKey")

	query := `SELECT ` + featureSnapshotColumns() + ` FROM ` + r.Name + `.feature_snapshots WHERE idempotency_key = @idempotency_key`
	featureSnapshot, err := scanFeatureSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
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

	return r.readFeatureSnapshot(ctx, r.Pool, featureSnapshotID)
}

func (r *SnapshotRepository) readFeatureSnapshot(ctx context.Context, queryer rowQuerier, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository readFeatureSnapshot")

	query := `SELECT ` + featureSnapshotColumns() + ` FROM ` + r.Name + `.feature_snapshots WHERE feature_snapshot_id = @feature_snapshot_id`
	featureSnapshot, err := scanFeatureSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
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

func (r *SnapshotRepository) SavePendingEmbeddingSnapshot(ctx context.Context, tx pgx.Tx, featureSnapshotID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingEmbeddingSnapshot")

	strategy = model.NormalizeEmbeddingStrategy(strategy)
	if err := model.ValidateEmbeddingStrategy(strategy); err != nil {
		return nil, domain.ErrValidationFailed.Extend(err.Error())
	}
	featureSnapshot, err := r.readFeatureSnapshot(ctx, tx, featureSnapshotID)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO ` + r.Name + `.embedding_snapshots (
		feature_snapshot_id, dataset_id, user_id, org_id, idempotency_key, strategy_version,
		extractor_name, extractor_version, cleaner_name, cleaner_version,
		chunker_name, chunker_version, chunk_size, chunk_overlap, embedding_provider, embedding_model,
		embedding_dimensions, status
	) VALUES (
		@feature_snapshot_id, @dataset_id, @user_id, @org_id, @idempotency_key, @strategy_version,
		@extractor_name, @extractor_version, @cleaner_name, @cleaner_version,
		@chunker_name, @chunker_version, @chunk_size, @chunk_overlap, @embedding_provider, @embedding_model,
		@embedding_dimensions, @status::snapshot_status_enum
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + embeddingSnapshotColumns()

	embeddingSnapshot, err := scanEmbeddingSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id":  pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"dataset_id":           pgtype.UUID{Bytes: featureSnapshot.DatasetID, Valid: true},
		"user_id":              pgtype.UUID{Bytes: featureSnapshot.UserID, Valid: true},
		"org_id":               pgtype.UUID{Bytes: featureSnapshot.OrgID, Valid: featureSnapshot.OrgID != uuid.Nil},
		"idempotency_key":      pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"strategy_version":     strategy.StrategyVersion,
		"extractor_name":       strategy.ExtractorName,
		"extractor_version":    strategy.ExtractorVersion,
		"cleaner_name":         strategy.CleanerName,
		"cleaner_version":      strategy.CleanerVersion,
		"chunker_name":         strategy.ChunkerName,
		"chunker_version":      strategy.ChunkerVersion,
		"chunk_size":           strategy.ChunkSize,
		"chunk_overlap":        strategy.ChunkOverlap,
		"embedding_provider":   strategy.EmbeddingProvider,
		"embedding_model":      strategy.EmbeddingModel,
		"embedding_dimensions": strategy.EmbeddingDimensions,
		"status":               model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.resolveEmbeddingSnapshotIdempotencyConflict(ctx, tx, idempotencyKey)
		}
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		r.LogPoolStatsOnError(ctx, "insert embedding snapshot failed", err)
		return nil, fmt.Errorf("insert embedding snapshot: %w", err)
	}
	return embeddingSnapshot, nil
}

func (r *SnapshotRepository) SaveEmbeddingRecords(ctx context.Context, tx pgx.Tx, records []model.EmbeddingRecord) error {
	log.Trace("SnapshotRepository SaveEmbeddingRecords")

	if len(records) == 0 {
		return nil
	}

	query := `INSERT INTO ` + r.Name + `.embedding_records (
		embedding_snapshot_id, dataset_id, user_id, org_id, chunk_index, source_text, embedding
	) VALUES (
		@embedding_snapshot_id, @dataset_id, @user_id, @org_id, @chunk_index, @source_text, @embedding
	)`

	for _, record := range records {
		if _, err := tx.Exec(ctx, query, pgx.NamedArgs{
			"embedding_snapshot_id": pgtype.UUID{Bytes: record.EmbeddingSnapshotID, Valid: true},
			"dataset_id":            pgtype.UUID{Bytes: record.DatasetID, Valid: true},
			"user_id":               pgtype.UUID{Bytes: record.UserID, Valid: true},
			"org_id":                pgtype.UUID{Bytes: record.OrgID, Valid: record.OrgID != uuid.Nil},
			"chunk_index":           record.ChunkIndex,
			"source_text":           record.SourceText,
			"embedding":             vectorLiteral(record.Vector),
		}); err != nil {
			if coreDB.IsForeignKeyViolation(err) {
				return domain.ErrValidationFailed.Extend("tenant projection is not ready")
			}
			return fmt.Errorf("insert embedding record: %w", err)
		}
	}
	return nil
}

func (r *SnapshotRepository) MarkEmbeddingReady(ctx context.Context, tx pgx.Tx, embeddingSnapshot *model.EmbeddingSnapshot) error {
	log.Trace("SnapshotRepository MarkEmbeddingReady")

	if embeddingSnapshot == nil {
		return domain.ErrValidationFailed.Extend("embedding snapshot is required")
	}

	if err := r.markEmbeddingReadyTx(ctx, tx, embeddingSnapshot); err != nil {
		return err
	}
	return nil
}

func (r *SnapshotRepository) markEmbeddingReadyTx(ctx context.Context, tx pgx.Tx, embeddingSnapshot *model.EmbeddingSnapshot) error {
	log.Trace("SnapshotRepository markEmbeddingReadyTx")

	eventSeq, err := r.nextMaterializationEventSeq(ctx, tx, embeddingSnapshot.DatasetID, embeddingSnapshot.OrgID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+r.Name+`.embedding_snapshots
		SET active_for_retrieval = false
		WHERE dataset_id = @dataset_id
			AND org_id = @org_id
			AND active_for_retrieval = true
			AND embedding_snapshot_id != @embedding_snapshot_id`, pgx.NamedArgs{
		"dataset_id":            pgtype.UUID{Bytes: embeddingSnapshot.DatasetID, Valid: true},
		"org_id":                pgtype.UUID{Bytes: embeddingSnapshot.OrgID, Valid: embeddingSnapshot.OrgID != uuid.Nil},
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
	}); err != nil {
		return fmt.Errorf("deactivate previous active embedding snapshots: %w", err)
	}

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status::snapshot_status_enum,
			materialization_event_seq = @materialization_event_seq,
			active_for_retrieval = true,
			vector_store = @vector_store,
			collection_name = @collection_name,
			embedding_dimensions = @embedding_dimensions,
			embedding_count = @embedding_count,
			strategy_version = @strategy_version,
			extractor_name = @extractor_name,
			extractor_version = @extractor_version,
			cleaner_name = @cleaner_name,
			cleaner_version = @cleaner_version,
			chunker_name = @chunker_name,
			chunker_version = @chunker_version,
			chunk_size = @chunk_size,
			chunk_overlap = @chunk_overlap,
			embedding_provider = @embedding_provider,
			embedding_model = @embedding_model,
			failure_reason = NULL
		WHERE embedding_snapshot_id = @embedding_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id":     pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
		"materialization_event_seq": eventSeq,
		"vector_store":              embeddingSnapshot.VectorStore,
		"collection_name":           embeddingSnapshot.CollectionName,
		"embedding_dimensions":      embeddingSnapshot.EmbeddingDimensions,
		"embedding_count":           embeddingSnapshot.EmbeddingCount,
		"strategy_version":          embeddingSnapshot.StrategyVersion,
		"extractor_name":            embeddingSnapshot.ExtractorName,
		"extractor_version":         embeddingSnapshot.ExtractorVersion,
		"cleaner_name":              embeddingSnapshot.CleanerName,
		"cleaner_version":           embeddingSnapshot.CleanerVersion,
		"chunker_name":              embeddingSnapshot.ChunkerName,
		"chunker_version":           embeddingSnapshot.ChunkerVersion,
		"chunk_size":                embeddingSnapshot.ChunkSize,
		"chunk_overlap":             embeddingSnapshot.ChunkOverlap,
		"embedding_provider":        embeddingSnapshot.EmbeddingProvider,
		"embedding_model":           embeddingSnapshot.EmbeddingModel,
		"status":                    model.SnapshotStatusReady.String(),
	})
	if err != nil {
		return fmt.Errorf("mark embedding snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotNotFound, embeddingSnapshot.EmbeddingSnapshotID)
	}
	embeddingSnapshot.MaterializationEventSeq = eventSeq
	embeddingSnapshot.ActiveForRetrieval = true
	return nil
}

func (r *SnapshotRepository) nextMaterializationEventSeq(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, orgID uuid.UUID) (int64, error) {
	log.Trace("SnapshotRepository nextMaterializationEventSeq")

	var eventSeq int64
	err := tx.QueryRow(ctx, `
		INSERT INTO `+r.Name+`.dataset_materialization_event_state AS state (dataset_id, org_id, next_event_seq)
		VALUES (@dataset_id, @org_id, 2)
		ON CONFLICT (dataset_id, org_id) DO UPDATE
		SET next_event_seq = state.next_event_seq + 1,
			updated_at = now()
		RETURNING next_event_seq - 1;`, pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
		"org_id":     pgtype.UUID{Bytes: orgID, Valid: orgID != uuid.Nil},
	}).Scan(&eventSeq)
	if err != nil {
		return 0, fmt.Errorf("allocate materialization event sequence: %w", err)
	}
	return eventSeq, nil
}

func (r *SnapshotRepository) MarkEmbeddingFailed(ctx context.Context, tx pgx.Tx, embeddingSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkEmbeddingFailed")

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status::snapshot_status_enum, active_for_retrieval = false, failure_reason = @failure_reason
		WHERE embedding_snapshot_id = @embedding_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
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

	return r.readEmbeddingByIdempotencyKey(ctx, r.Pool, idempotencyKey)
}

func (r *SnapshotRepository) readEmbeddingByIdempotencyKey(ctx context.Context, queryer rowQuerier, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository readEmbeddingByIdempotencyKey")

	query := `SELECT ` + embeddingSnapshotColumns() + ` FROM ` + r.Name + `.embedding_snapshots WHERE idempotency_key = @idempotency_key`
	embeddingSnapshot, err := scanEmbeddingSnapshot(queryer.QueryRow(ctx, query, pgx.NamedArgs{
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

func (r *SnapshotRepository) ReadActiveEmbeddingSnapshot(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository ReadActiveEmbeddingSnapshot")

	query := `SELECT ` + embeddingSnapshotColumns() + ` FROM ` + r.Name + `.embedding_snapshots
		WHERE dataset_id = @dataset_id
			AND org_id = @org_id
			AND active_for_retrieval = true
			AND status = @status::snapshot_status_enum
		ORDER BY updated_at DESC
		LIMIT 1`
	embeddingSnapshot, err := scanEmbeddingSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
		"user_id":    pgtype.UUID{Bytes: userID, Valid: true},
		"org_id":     pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
		"status":     model.SnapshotStatusReady.String(),
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: dataset_id=%s", domain.ErrEmbeddingSnapshotNotFound, datasetID)
		}
		r.LogPoolStatsOnError(ctx, "read active embedding snapshot failed", err)
		return nil, fmt.Errorf("read active embedding snapshot: %w", err)
	}
	return embeddingSnapshot, nil
}

func (r *SnapshotRepository) SearchEmbeddingRecords(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot, queryVector []float32, topK int) ([]model.EmbeddingRecord, error) {
	log.Trace("SnapshotRepository SearchEmbeddingRecords")

	if embeddingSnapshot == nil {
		return nil, domain.ErrEmbeddingSnapshotNotFound.Extend("active embedding snapshot is required")
	}
	if embeddingSnapshot.EmbeddingDimensions <= 0 {
		return nil, domain.ErrValidationFailed.Extend("embedding dimensions are required")
	}
	if len(queryVector) != embeddingSnapshot.EmbeddingDimensions {
		return nil, domain.ErrValidationFailed.Extend("query vector dimensions do not match active embedding snapshot")
	}

	dimensions := embeddingSnapshot.EmbeddingDimensions
	query := fmt.Sprintf(`SELECT embedding_record_id::text, embedding_snapshot_id::text, dataset_id::text, chunk_index, source_text,
			(embedding::vector(%d) <=> @query_embedding::vector(%d))::double precision AS distance
		FROM `+r.Name+`.embedding_records
		WHERE embedding_snapshot_id = @embedding_snapshot_id
			AND dataset_id = @dataset_id
			AND org_id = @org_id
			AND vector_dims(embedding) = %d
		ORDER BY embedding::vector(%d) <=> @query_embedding::vector(%d)
		LIMIT @limit`, dimensions, dimensions, dimensions, dimensions, dimensions)

	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
		"dataset_id":            pgtype.UUID{Bytes: embeddingSnapshot.DatasetID, Valid: true},
		"user_id":               pgtype.UUID{Bytes: embeddingSnapshot.UserID, Valid: true},
		"org_id":                pgtype.UUID{Bytes: embeddingSnapshot.OrgID, Valid: embeddingSnapshot.OrgID != uuid.Nil},
		"query_embedding":       vectorLiteral(queryVector),
		"limit":                 topK,
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "search embedding records failed", err)
		return nil, fmt.Errorf("search embedding records: %w", err)
	}
	defer rows.Close()

	records := []model.EmbeddingRecord{}
	for rows.Next() {
		record, err := scanEmbeddingRecordSearchRow(rows)
		if err != nil {
			return nil, err
		}
		record.UserID = embeddingSnapshot.UserID
		record.OrgID = embeddingSnapshot.OrgID
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read embedding search rows: %w", err)
	}
	return records, nil
}

func (r *SnapshotRepository) resolveRawSnapshotIdempotencyConflict(ctx context.Context, tx pgx.Tx, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository resolveRawSnapshotIdempotencyConflict")

	existing, err := r.readRawByIdempotencyKey(ctx, tx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("raw snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.RawSnapshotAlreadyMaterializedError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenRawSnapshotForRetry(ctx, tx, existing.RawSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotInProgress, existing.RawSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported raw snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) resolveFeatureSnapshotIdempotencyConflict(ctx context.Context, tx pgx.Tx, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository resolveFeatureSnapshotIdempotencyConflict")

	existing, err := r.readFeatureByIdempotencyKey(ctx, tx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("feature snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.FeatureSnapshotAlreadyBuiltError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenFeatureSnapshotForRetry(ctx, tx, existing.FeatureSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotInProgress, existing.FeatureSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported feature snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) resolveEmbeddingSnapshotIdempotencyConflict(ctx context.Context, tx pgx.Tx, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository resolveEmbeddingSnapshotIdempotencyConflict")

	existing, err := r.readEmbeddingByIdempotencyKey(ctx, tx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("embedding snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.EmbeddingsAlreadyMaterializedError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenEmbeddingSnapshotForRetry(ctx, tx, existing.EmbeddingSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotInProgress, existing.EmbeddingSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported embedding snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) reopenRawSnapshotForRetry(ctx context.Context, tx pgx.Tx, rawSnapshotID uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository reopenRawSnapshotForRetry")

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status::snapshot_status_enum, failure_reason = NULL
		WHERE raw_snapshot_id = @raw_snapshot_id
		RETURNING ` + rawSnapshotColumns()
	rawSnapshot, err := scanRawSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id": pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"status":          model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "reopen raw snapshot for retry failed", err)
		return nil, fmt.Errorf("reopen raw snapshot for retry: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) reopenFeatureSnapshotForRetry(ctx context.Context, tx pgx.Tx, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository reopenFeatureSnapshotForRetry")

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status::snapshot_status_enum, failure_reason = NULL
		WHERE feature_snapshot_id = @feature_snapshot_id
		RETURNING ` + featureSnapshotColumns()
	featureSnapshot, err := scanFeatureSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"status":              model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "reopen feature snapshot for retry failed", err)
		return nil, fmt.Errorf("reopen feature snapshot for retry: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) reopenEmbeddingSnapshotForRetry(ctx context.Context, tx pgx.Tx, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository reopenEmbeddingSnapshotForRetry")

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status::snapshot_status_enum, failure_reason = NULL
		WHERE embedding_snapshot_id = @embedding_snapshot_id
		RETURNING ` + embeddingSnapshotColumns()
	embeddingSnapshot, err := scanEmbeddingSnapshot(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshotID, Valid: true},
		"status":                model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "reopen embedding snapshot for retry failed", err)
		return nil, fmt.Errorf("reopen embedding snapshot for retry: %w", err)
	}
	return embeddingSnapshot, nil
}

func rawSnapshotColumns() string {
	return `raw_snapshot_id::text, dataset_id::text, user_id::text, org_id::text, materialization_event_seq, storage_location, content_type, file_extension,
		table_namespace, table_name, table_format::text, catalog_provider::text, processing_profile::text, schema_version, schema_metadata::text, status::text, COALESCE(failure_reason, '')`
}

func featureSnapshotColumns() string {
	return `feature_snapshot_id::text, raw_snapshot_id::text, dataset_id::text, user_id::text, org_id::text, materialization_event_seq, storage_location,
		table_namespace, table_name, table_format::text, catalog_provider::text, processing_profile::text, schema_version, schema_metadata::text, status::text, COALESCE(failure_reason, '')`
}

func embeddingSnapshotColumns() string {
	return `embedding_snapshot_id::text, feature_snapshot_id::text, dataset_id::text, user_id::text, org_id::text,
		materialization_event_seq, vector_store, collection_name, embedding_dimensions, embedding_count, strategy_version,
		extractor_name, extractor_version, cleaner_name, cleaner_version,
		chunker_name, chunker_version, chunk_size, chunk_overlap, embedding_provider, embedding_model,
		active_for_retrieval, status::text, COALESCE(failure_reason, '')`
}

func scanRawSnapshot(row pgx.Row) (*model.RawSnapshot, error) {
	var rawSnapshotID, datasetID, userID, orgID, statusRaw, processingProfileRaw string
	rawSnapshot := &model.RawSnapshot{}
	if err := row.Scan(
		&rawSnapshotID,
		&datasetID,
		&userID,
		&orgID,
		&rawSnapshot.MaterializationEventSeq,
		&rawSnapshot.StorageLocation,
		&rawSnapshot.ContentType,
		&rawSnapshot.FileExtension,
		&rawSnapshot.TableNamespace,
		&rawSnapshot.TableName,
		&rawSnapshot.TableFormat,
		&rawSnapshot.CatalogProvider,
		&processingProfileRaw,
		&rawSnapshot.SchemaVersion,
		&rawSnapshot.SchemaMetadata,
		&statusRaw,
		&rawSnapshot.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToSnapshotStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	processingProfile, err := model.ToProcessingProfile(processingProfileRaw)
	if err != nil {
		return nil, err
	}
	rawSnapshot.RawSnapshotID = uuid.MustParse(rawSnapshotID)
	rawSnapshot.DatasetID = uuid.MustParse(datasetID)
	rawSnapshot.UserID = uuid.MustParse(userID)
	rawSnapshot.OrgID = uuid.MustParse(orgID)
	rawSnapshot.ProcessingProfile = processingProfile
	rawSnapshot.Status = status
	return rawSnapshot, nil
}

func scanFeatureSnapshot(row pgx.Row) (*model.FeatureSnapshot, error) {
	var featureSnapshotID, rawSnapshotID, datasetID, userID, orgID, statusRaw, processingProfileRaw string
	featureSnapshot := &model.FeatureSnapshot{}
	if err := row.Scan(
		&featureSnapshotID,
		&rawSnapshotID,
		&datasetID,
		&userID,
		&orgID,
		&featureSnapshot.MaterializationEventSeq,
		&featureSnapshot.StorageLocation,
		&featureSnapshot.TableNamespace,
		&featureSnapshot.TableName,
		&featureSnapshot.TableFormat,
		&featureSnapshot.CatalogProvider,
		&processingProfileRaw,
		&featureSnapshot.SchemaVersion,
		&featureSnapshot.SchemaMetadata,
		&statusRaw,
		&featureSnapshot.FailureReason,
	); err != nil {
		return nil, err
	}
	status, err := model.ToSnapshotStatus(statusRaw)
	if err != nil {
		return nil, err
	}
	processingProfile, err := model.ToProcessingProfile(processingProfileRaw)
	if err != nil {
		return nil, err
	}
	featureSnapshot.FeatureSnapshotID = uuid.MustParse(featureSnapshotID)
	featureSnapshot.RawSnapshotID = uuid.MustParse(rawSnapshotID)
	featureSnapshot.DatasetID = uuid.MustParse(datasetID)
	featureSnapshot.UserID = uuid.MustParse(userID)
	featureSnapshot.OrgID = uuid.MustParse(orgID)
	featureSnapshot.ProcessingProfile = processingProfile
	featureSnapshot.Status = status
	return featureSnapshot, nil
}

func scanEmbeddingSnapshot(row pgx.Row) (*model.EmbeddingSnapshot, error) {
	var embeddingSnapshotID, featureSnapshotID, datasetID, userID, orgID, statusRaw string
	embeddingSnapshot := &model.EmbeddingSnapshot{}
	if err := row.Scan(
		&embeddingSnapshotID,
		&featureSnapshotID,
		&datasetID,
		&userID,
		&orgID,
		&embeddingSnapshot.MaterializationEventSeq,
		&embeddingSnapshot.VectorStore,
		&embeddingSnapshot.CollectionName,
		&embeddingSnapshot.EmbeddingDimensions,
		&embeddingSnapshot.EmbeddingCount,
		&embeddingSnapshot.StrategyVersion,
		&embeddingSnapshot.ExtractorName,
		&embeddingSnapshot.ExtractorVersion,
		&embeddingSnapshot.CleanerName,
		&embeddingSnapshot.CleanerVersion,
		&embeddingSnapshot.ChunkerName,
		&embeddingSnapshot.ChunkerVersion,
		&embeddingSnapshot.ChunkSize,
		&embeddingSnapshot.ChunkOverlap,
		&embeddingSnapshot.EmbeddingProvider,
		&embeddingSnapshot.EmbeddingModel,
		&embeddingSnapshot.ActiveForRetrieval,
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
	embeddingSnapshot.UserID = uuid.MustParse(userID)
	embeddingSnapshot.OrgID = uuid.MustParse(orgID)
	embeddingSnapshot.Status = status
	return embeddingSnapshot, nil
}

func scanEmbeddingRecordSearchRow(row pgx.Row) (model.EmbeddingRecord, error) {
	var embeddingRecordID, embeddingSnapshotID, datasetID string
	var distance float64
	record := model.EmbeddingRecord{}
	if err := row.Scan(
		&embeddingRecordID,
		&embeddingSnapshotID,
		&datasetID,
		&record.ChunkIndex,
		&record.SourceText,
		&distance,
	); err != nil {
		return model.EmbeddingRecord{}, err
	}
	record.EmbeddingRecordID = uuid.MustParse(embeddingRecordID)
	record.EmbeddingSnapshotID = uuid.MustParse(embeddingSnapshotID)
	record.DatasetID = uuid.MustParse(datasetID)
	record.Distance = distance
	record.Similarity = 1 - distance
	return record, nil
}

func vectorLiteral(vector []float32) string {
	values := make([]string, len(vector))
	for i, value := range vector {
		values[i] = strconv.FormatFloat(float64(value), 'f', -1, 32)
	}
	return "[" + strings.Join(values, ",") + "]"
}

func orgIDFromContext(ctx context.Context) uuid.UUID {
	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return uuid.Nil
	}
	return orgID
}
