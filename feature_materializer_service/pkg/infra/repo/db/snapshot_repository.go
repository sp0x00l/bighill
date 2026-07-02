package db

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type SnapshotRepository struct {
	coreDB.Database
	outbox       msgConn.OrderedOutbox
	topic        string
	outboxSignal func()
}

type SnapshotRepositoryOption func(*SnapshotRepository)

func WithTransactionalOutbox(outbox msgConn.OrderedOutbox, topic string) SnapshotRepositoryOption {
	log.Trace("WithTransactionalOutbox")

	return func(r *SnapshotRepository) {
		r.outbox = outbox
		r.topic = topic
	}
}

func WithOutboxSignal(signal func()) SnapshotRepositoryOption {
	log.Trace("WithOutboxSignal")

	return func(r *SnapshotRepository) {
		r.outboxSignal = signal
	}
}

func NewSnapshotRepository(db *coreDB.Database, opts ...SnapshotRepositoryOption) *SnapshotRepository {
	log.Trace("NewSnapshotRepository")

	repository := &SnapshotRepository{
		Database: *db,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(repository)
		}
	}
	return repository
}

func (r *SnapshotRepository) SavePendingRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingRawSnapshot")

	query := `INSERT INTO ` + r.Name + `.raw_snapshots (
		dataset_id, user_id, idempotency_key, source_storage_location, storage_location, content_type, file_extension,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, status
	) VALUES (
		@dataset_id, @user_id, @idempotency_key, @source_storage_location, @storage_location, @content_type, @file_extension,
		@table_namespace, @table_name, @table_format, @catalog_provider, @processing_profile, @status
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
		"processing_profile":      datasetFile.ProcessingProfile.String(),
		"status":                  model.SnapshotStatusPending.String(),
	})
	rawSnapshot, err := scanRawSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.resolveRawSnapshotIdempotencyConflict(ctx, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "insert raw snapshot failed", err)
		return nil, fmt.Errorf("insert raw snapshot: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) MarkRawReady(ctx context.Context, rawSnapshot *model.RawSnapshot) error {
	log.Trace("SnapshotRepository MarkRawReady")

	if r.outbox != nil {
		return r.markRawReadyTx(ctx, rawSnapshot)
	}

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status,
			storage_location = @storage_location,
			table_format = @table_format,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			failure_reason = NULL
		WHERE raw_snapshot_id = @raw_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":  pgtype.UUID{Bytes: rawSnapshot.RawSnapshotID, Valid: true},
		"storage_location": rawSnapshot.StorageLocation,
		"table_format":     rawSnapshot.TableFormat,
		"schema_version":   rawSnapshot.SchemaVersion,
		"schema_metadata":  rawSnapshot.SchemaMetadata,
		"status":           model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark raw snapshot ready failed", err)
		return fmt.Errorf("mark raw snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotNotFound, rawSnapshot.RawSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) markRawReadyTx(ctx context.Context, rawSnapshot *model.RawSnapshot) error {
	log.Trace("SnapshotRepository markRawReadyTx")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin raw snapshot ready transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status,
			storage_location = @storage_location,
			table_format = @table_format,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			failure_reason = NULL
		WHERE raw_snapshot_id = @raw_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":  pgtype.UUID{Bytes: rawSnapshot.RawSnapshotID, Valid: true},
		"storage_location": rawSnapshot.StorageLocation,
		"table_format":     rawSnapshot.TableFormat,
		"schema_version":   rawSnapshot.SchemaVersion,
		"schema_metadata":  rawSnapshot.SchemaMetadata,
		"status":           model.SnapshotStatusReady.String(),
	})
	if err != nil {
		return fmt.Errorf("mark raw snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotNotFound, rawSnapshot.RawSnapshotID)
	}
	if err := r.outbox.EnqueueTx(ctx, tx, rawSnapshotReadyMessage(r.topic, rawSnapshot)); err != nil {
		return fmt.Errorf("enqueue raw snapshot ready: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit raw snapshot ready transaction: %w", err)
	}
	r.notifyOutbox()
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
		raw_snapshot_id, dataset_id, user_id, idempotency_key, table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata, status
	) VALUES (
		@raw_snapshot_id, @dataset_id, @user_id, @idempotency_key, @table_namespace, @table_name, @table_format, @catalog_provider, @processing_profile, @schema_version, @schema_metadata::jsonb, @status
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + featureSnapshotColumns()

	featureSnapshot, err := scanFeatureSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id":    pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"dataset_id":         pgtype.UUID{Bytes: rawSnapshot.DatasetID, Valid: true},
		"user_id":            pgtype.UUID{Bytes: rawSnapshot.UserID, Valid: true},
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
			return r.resolveFeatureSnapshotIdempotencyConflict(ctx, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "insert feature snapshot failed", err)
		return nil, fmt.Errorf("insert feature snapshot: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) MarkFeatureReady(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error {
	log.Trace("SnapshotRepository MarkFeatureReady")

	if r.outbox != nil {
		return r.markFeatureReadyTx(ctx, featureSnapshot)
	}

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status,
			storage_location = @storage_location,
			table_format = @table_format,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			failure_reason = NULL
		WHERE feature_snapshot_id = @feature_snapshot_id`
	tag, err := r.Pool.Exec(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshot.FeatureSnapshotID, Valid: true},
		"storage_location":    featureSnapshot.StorageLocation,
		"table_format":        featureSnapshot.TableFormat,
		"schema_version":      featureSnapshot.SchemaVersion,
		"schema_metadata":     featureSnapshot.SchemaMetadata,
		"status":              model.SnapshotStatusReady.String(),
	})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "mark feature snapshot ready failed", err)
		return fmt.Errorf("mark feature snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotNotFound, featureSnapshot.FeatureSnapshotID)
	}
	return nil
}

func (r *SnapshotRepository) markFeatureReadyTx(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error {
	log.Trace("SnapshotRepository markFeatureReadyTx")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin feature snapshot ready transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status,
			storage_location = @storage_location,
			table_format = @table_format,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			failure_reason = NULL
		WHERE feature_snapshot_id = @feature_snapshot_id`
	tag, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshot.FeatureSnapshotID, Valid: true},
		"storage_location":    featureSnapshot.StorageLocation,
		"table_format":        featureSnapshot.TableFormat,
		"schema_version":      featureSnapshot.SchemaVersion,
		"schema_metadata":     featureSnapshot.SchemaMetadata,
		"status":              model.SnapshotStatusReady.String(),
	})
	if err != nil {
		return fmt.Errorf("mark feature snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotNotFound, featureSnapshot.FeatureSnapshotID)
	}
	if err := r.outbox.EnqueueTx(ctx, tx, featureSnapshotReadyMessage(r.topic, featureSnapshot)); err != nil {
		return fmt.Errorf("enqueue feature snapshot ready: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit feature snapshot ready transaction: %w", err)
	}
	r.notifyOutbox()
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

func (r *SnapshotRepository) SavePendingEmbeddingSnapshot(ctx context.Context, featureSnapshotID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository SavePendingEmbeddingSnapshot")

	strategy = model.NormalizeEmbeddingStrategy(strategy)
	featureSnapshot, err := r.ReadFeatureSnapshot(ctx, featureSnapshotID)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO ` + r.Name + `.embedding_snapshots (
		feature_snapshot_id, dataset_id, user_id, idempotency_key, strategy_version,
		extractor_name, extractor_version, cleaner_name, cleaner_version,
		chunker_name, chunker_version, chunk_size, chunk_overlap, embedding_provider, embedding_model,
		embedding_dimensions, status
	) VALUES (
		@feature_snapshot_id, @dataset_id, @user_id, @idempotency_key, @strategy_version,
		@extractor_name, @extractor_version, @cleaner_name, @cleaner_version,
		@chunker_name, @chunker_version, @chunk_size, @chunk_overlap, @embedding_provider, @embedding_model,
		@embedding_dimensions, @status
	)
	ON CONFLICT (idempotency_key) DO NOTHING
	RETURNING ` + embeddingSnapshotColumns()

	embeddingSnapshot, err := scanEmbeddingSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id":  pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"dataset_id":           pgtype.UUID{Bytes: featureSnapshot.DatasetID, Valid: true},
		"user_id":              pgtype.UUID{Bytes: featureSnapshot.UserID, Valid: true},
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
			return r.resolveEmbeddingSnapshotIdempotencyConflict(ctx, idempotencyKey)
		}
		r.LogPoolStatsOnError(ctx, "insert embedding snapshot failed", err)
		return nil, fmt.Errorf("insert embedding snapshot: %w", err)
	}
	return embeddingSnapshot, nil
}

func (r *SnapshotRepository) SaveEmbeddingRecords(ctx context.Context, records []model.EmbeddingRecord) error {
	log.Trace("SnapshotRepository SaveEmbeddingRecords")

	if len(records) == 0 {
		return nil
	}

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		r.LogPoolStatsOnError(ctx, "begin embedding records transaction failed", err)
		return fmt.Errorf("begin embedding records transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO ` + r.Name + `.embedding_records (
		embedding_snapshot_id, dataset_id, chunk_index, source_text, embedding
	) VALUES (
		@embedding_snapshot_id, @dataset_id, @chunk_index, @source_text, @embedding
	)`

	for _, record := range records {
		if _, err := tx.Exec(ctx, query, pgx.NamedArgs{
			"embedding_snapshot_id": pgtype.UUID{Bytes: record.EmbeddingSnapshotID, Valid: true},
			"dataset_id":            pgtype.UUID{Bytes: record.DatasetID, Valid: true},
			"chunk_index":           record.ChunkIndex,
			"source_text":           record.SourceText,
			"embedding":             vectorLiteral(record.Vector),
		}); err != nil {
			return fmt.Errorf("insert embedding record: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit embedding records transaction: %w", err)
	}
	return nil
}

func (r *SnapshotRepository) MarkEmbeddingReady(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot) error {
	log.Trace("SnapshotRepository MarkEmbeddingReady")

	if embeddingSnapshot == nil {
		return domain.ErrValidationFailed.Extend("embedding snapshot is required")
	}

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin embedding snapshot ready transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := r.markEmbeddingReadyTx(ctx, tx, embeddingSnapshot); err != nil {
		return err
	}

	if r.outbox != nil {
		if err := r.outbox.EnqueueTx(ctx, tx, embeddingSnapshotReadyMessage(r.topic, embeddingSnapshot)); err != nil {
			return fmt.Errorf("enqueue embedding snapshot ready: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit embedding snapshot ready transaction: %w", err)
	}
	if r.outbox != nil {
		r.notifyOutbox()
	}
	return nil
}

func (r *SnapshotRepository) notifyOutbox() {
	log.Trace("SnapshotRepository notifyOutbox")

	if r.outboxSignal != nil {
		r.outboxSignal()
	}
}

func (r *SnapshotRepository) markEmbeddingReadyTx(ctx context.Context, tx pgx.Tx, embeddingSnapshot *model.EmbeddingSnapshot) error {
	log.Trace("SnapshotRepository markEmbeddingReadyTx")

	if _, err := tx.Exec(ctx, `UPDATE `+r.Name+`.embedding_snapshots
		SET active_for_retrieval = false
		WHERE dataset_id = @dataset_id
			AND active_for_retrieval = true
			AND embedding_snapshot_id != @embedding_snapshot_id`, pgx.NamedArgs{
		"dataset_id":            pgtype.UUID{Bytes: embeddingSnapshot.DatasetID, Valid: true},
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
	}); err != nil {
		return fmt.Errorf("deactivate previous active embedding snapshots: %w", err)
	}

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status,
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
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
		"vector_store":          embeddingSnapshot.VectorStore,
		"collection_name":       embeddingSnapshot.CollectionName,
		"embedding_dimensions":  embeddingSnapshot.EmbeddingDimensions,
		"embedding_count":       embeddingSnapshot.EmbeddingCount,
		"strategy_version":      embeddingSnapshot.StrategyVersion,
		"extractor_name":        embeddingSnapshot.ExtractorName,
		"extractor_version":     embeddingSnapshot.ExtractorVersion,
		"cleaner_name":          embeddingSnapshot.CleanerName,
		"cleaner_version":       embeddingSnapshot.CleanerVersion,
		"chunker_name":          embeddingSnapshot.ChunkerName,
		"chunker_version":       embeddingSnapshot.ChunkerVersion,
		"chunk_size":            embeddingSnapshot.ChunkSize,
		"chunk_overlap":         embeddingSnapshot.ChunkOverlap,
		"embedding_provider":    embeddingSnapshot.EmbeddingProvider,
		"embedding_model":       embeddingSnapshot.EmbeddingModel,
		"status":                model.SnapshotStatusReady.String(),
	})
	if err != nil {
		return fmt.Errorf("mark embedding snapshot ready: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotNotFound, embeddingSnapshot.EmbeddingSnapshotID)
	}
	embeddingSnapshot.ActiveForRetrieval = true
	return nil
}

func (r *SnapshotRepository) MarkEmbeddingFailed(ctx context.Context, embeddingSnapshotID uuid.UUID, reason string) error {
	log.Trace("SnapshotRepository MarkEmbeddingFailed")

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status, active_for_retrieval = false, failure_reason = @failure_reason
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

func (r *SnapshotRepository) ReadActiveEmbeddingSnapshot(ctx context.Context, datasetID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository ReadActiveEmbeddingSnapshot")

	query := `SELECT ` + embeddingSnapshotColumns() + ` FROM ` + r.Name + `.embedding_snapshots
		WHERE dataset_id = @dataset_id
			AND active_for_retrieval = true
			AND status = @status
		ORDER BY updated_at DESC
		LIMIT 1`
	embeddingSnapshot, err := scanEmbeddingSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
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
			AND vector_dims(embedding) = %d
		ORDER BY embedding::vector(%d) <=> @query_embedding::vector(%d)
		LIMIT @limit`, dimensions, dimensions, dimensions, dimensions, dimensions)

	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"embedding_snapshot_id": pgtype.UUID{Bytes: embeddingSnapshot.EmbeddingSnapshotID, Valid: true},
		"dataset_id":            pgtype.UUID{Bytes: embeddingSnapshot.DatasetID, Valid: true},
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
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read embedding search rows: %w", err)
	}
	return records, nil
}

func (r *SnapshotRepository) resolveRawSnapshotIdempotencyConflict(ctx context.Context, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository resolveRawSnapshotIdempotencyConflict")

	existing, err := r.ReadRawByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("raw snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.RawSnapshotAlreadyMaterializedError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenRawSnapshotForRetry(ctx, existing.RawSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: raw_snapshot_id=%s", domain.ErrRawSnapshotInProgress, existing.RawSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported raw snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) resolveFeatureSnapshotIdempotencyConflict(ctx context.Context, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository resolveFeatureSnapshotIdempotencyConflict")

	existing, err := r.ReadFeatureByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("feature snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.FeatureSnapshotAlreadyBuiltError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenFeatureSnapshotForRetry(ctx, existing.FeatureSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: feature_snapshot_id=%s", domain.ErrFeatureSnapshotInProgress, existing.FeatureSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported feature snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) resolveEmbeddingSnapshotIdempotencyConflict(ctx context.Context, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository resolveEmbeddingSnapshotIdempotencyConflict")

	existing, err := r.ReadEmbeddingByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("embedding snapshot idempotency conflict lookup failed: idempotency_key=%s: %w", idempotencyKey, err)
	}
	switch existing.Status {
	case model.SnapshotStatusReady:
		return nil, &domain.EmbeddingsAlreadyMaterializedError{Record: existing}
	case model.SnapshotStatusFailed:
		return r.reopenEmbeddingSnapshotForRetry(ctx, existing.EmbeddingSnapshotID)
	case model.SnapshotStatusPending:
		return nil, fmt.Errorf("%w: embedding_snapshot_id=%s", domain.ErrEmbeddingSnapshotInProgress, existing.EmbeddingSnapshotID)
	default:
		return nil, fmt.Errorf("unsupported embedding snapshot status %s", existing.Status.String())
	}
}

func (r *SnapshotRepository) reopenRawSnapshotForRetry(ctx context.Context, rawSnapshotID uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("SnapshotRepository reopenRawSnapshotForRetry")

	query := `UPDATE ` + r.Name + `.raw_snapshots
		SET status = @status, failure_reason = NULL
		WHERE raw_snapshot_id = @raw_snapshot_id
		RETURNING ` + rawSnapshotColumns()
	rawSnapshot, err := scanRawSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"raw_snapshot_id": pgtype.UUID{Bytes: rawSnapshotID, Valid: true},
		"status":          model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "reopen raw snapshot for retry failed", err)
		return nil, fmt.Errorf("reopen raw snapshot for retry: %w", err)
	}
	return rawSnapshot, nil
}

func (r *SnapshotRepository) reopenFeatureSnapshotForRetry(ctx context.Context, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("SnapshotRepository reopenFeatureSnapshotForRetry")

	query := `UPDATE ` + r.Name + `.feature_snapshots
		SET status = @status, failure_reason = NULL
		WHERE feature_snapshot_id = @feature_snapshot_id
		RETURNING ` + featureSnapshotColumns()
	featureSnapshot, err := scanFeatureSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"feature_snapshot_id": pgtype.UUID{Bytes: featureSnapshotID, Valid: true},
		"status":              model.SnapshotStatusPending.String(),
	}))
	if err != nil {
		r.LogPoolStatsOnError(ctx, "reopen feature snapshot for retry failed", err)
		return nil, fmt.Errorf("reopen feature snapshot for retry: %w", err)
	}
	return featureSnapshot, nil
}

func (r *SnapshotRepository) reopenEmbeddingSnapshotForRetry(ctx context.Context, embeddingSnapshotID uuid.UUID) (*model.EmbeddingSnapshot, error) {
	log.Trace("SnapshotRepository reopenEmbeddingSnapshotForRetry")

	query := `UPDATE ` + r.Name + `.embedding_snapshots
		SET status = @status, failure_reason = NULL
		WHERE embedding_snapshot_id = @embedding_snapshot_id
		RETURNING ` + embeddingSnapshotColumns()
	embeddingSnapshot, err := scanEmbeddingSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
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
	return `raw_snapshot_id::text, dataset_id::text, user_id::text, storage_location, content_type, file_extension,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata::text, status::text, COALESCE(failure_reason, '')`
}

func featureSnapshotColumns() string {
	return `feature_snapshot_id::text, raw_snapshot_id::text, dataset_id::text, user_id::text, storage_location,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata::text, status::text, COALESCE(failure_reason, '')`
}

func embeddingSnapshotColumns() string {
	return `embedding_snapshot_id::text, feature_snapshot_id::text, dataset_id::text, user_id::text,
		vector_store, collection_name, embedding_dimensions, embedding_count, strategy_version,
		extractor_name, extractor_version, cleaner_name, cleaner_version,
		chunker_name, chunker_version, chunk_size, chunk_overlap, embedding_provider, embedding_model,
		active_for_retrieval, status::text, COALESCE(failure_reason, '')`
}

func scanRawSnapshot(row pgx.Row) (*model.RawSnapshot, error) {
	var rawSnapshotID, datasetID, userID, statusRaw, processingProfileRaw string
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
	rawSnapshot.ProcessingProfile = processingProfile
	rawSnapshot.Status = status
	return rawSnapshot, nil
}

func scanFeatureSnapshot(row pgx.Row) (*model.FeatureSnapshot, error) {
	var featureSnapshotID, rawSnapshotID, datasetID, userID, statusRaw, processingProfileRaw string
	featureSnapshot := &model.FeatureSnapshot{}
	if err := row.Scan(
		&featureSnapshotID,
		&rawSnapshotID,
		&datasetID,
		&userID,
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
	featureSnapshot.ProcessingProfile = processingProfile
	featureSnapshot.Status = status
	return featureSnapshot, nil
}

func scanEmbeddingSnapshot(row pgx.Row) (*model.EmbeddingSnapshot, error) {
	var embeddingSnapshotID, featureSnapshotID, datasetID, userID, statusRaw string
	embeddingSnapshot := &model.EmbeddingSnapshot{}
	if err := row.Scan(
		&embeddingSnapshotID,
		&featureSnapshotID,
		&datasetID,
		&userID,
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

func rawSnapshotReadyMessage(topic string, rawSnapshot *model.RawSnapshot) msgConn.OutboundMessage {
	log.Trace("rawSnapshotReadyMessage")

	payload := mustMarshal(&featurepb.RawSnapshotReadyEvent{
		RawSnapshotId:     rawSnapshot.RawSnapshotID.String(),
		DatasetId:         rawSnapshot.DatasetID.String(),
		UserId:            rawSnapshot.UserID.String(),
		StorageLocation:   rawSnapshot.StorageLocation,
		TableNamespace:    rawSnapshot.TableNamespace,
		TableName:         rawSnapshot.TableName,
		TableFormat:       rawSnapshot.TableFormat,
		CatalogProvider:   rawSnapshot.CatalogProvider,
		SchemaVersion:     int32(rawSnapshot.SchemaVersion),
		SchemaMetadata:    rawSnapshot.SchemaMetadata,
		ProcessingProfile: rawSnapshot.ProcessingProfile.String(),
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: rawSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeRawSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "raw_snapshot_ready:" + rawSnapshot.RawSnapshotID.String(),
	}
}

func featureSnapshotReadyMessage(topic string, featureSnapshot *model.FeatureSnapshot) msgConn.OutboundMessage {
	log.Trace("featureSnapshotReadyMessage")

	payload := mustMarshal(&featurepb.FeatureSnapshotReadyEvent{
		FeatureSnapshotId: featureSnapshot.FeatureSnapshotID.String(),
		RawSnapshotId:     featureSnapshot.RawSnapshotID.String(),
		DatasetId:         featureSnapshot.DatasetID.String(),
		UserId:            featureSnapshot.UserID.String(),
		StorageLocation:   featureSnapshot.StorageLocation,
		TableNamespace:    featureSnapshot.TableNamespace,
		TableName:         featureSnapshot.TableName,
		TableFormat:       featureSnapshot.TableFormat,
		CatalogProvider:   featureSnapshot.CatalogProvider,
		SchemaVersion:     int32(featureSnapshot.SchemaVersion),
		SchemaMetadata:    featureSnapshot.SchemaMetadata,
		ProcessingProfile: featureSnapshot.ProcessingProfile.String(),
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: featureSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeFeatureSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "feature_snapshot_ready:" + featureSnapshot.FeatureSnapshotID.String(),
	}
}

func embeddingSnapshotReadyMessage(topic string, embeddingSnapshot *model.EmbeddingSnapshot) msgConn.OutboundMessage {
	log.Trace("embeddingSnapshotReadyMessage")

	payload := mustMarshal(&featurepb.EmbeddingSnapshotReadyEvent{
		EmbeddingSnapshotId: embeddingSnapshot.EmbeddingSnapshotID.String(),
		FeatureSnapshotId:   embeddingSnapshot.FeatureSnapshotID.String(),
		DatasetId:           embeddingSnapshot.DatasetID.String(),
		UserId:              embeddingSnapshot.UserID.String(),
		VectorStore:         embeddingSnapshot.VectorStore,
		CollectionName:      embeddingSnapshot.CollectionName,
		EmbeddingDimensions: int32(embeddingSnapshot.EmbeddingDimensions),
		EmbeddingCount:      embeddingSnapshot.EmbeddingCount,
		StrategyVersion:     embeddingSnapshot.StrategyVersion,
		ChunkerName:         embeddingSnapshot.ChunkerName,
		ChunkerVersion:      embeddingSnapshot.ChunkerVersion,
		ChunkSize:           int32(embeddingSnapshot.ChunkSize),
		ChunkOverlap:        int32(embeddingSnapshot.ChunkOverlap),
		EmbeddingProvider:   embeddingSnapshot.EmbeddingProvider,
		EmbeddingModel:      embeddingSnapshot.EmbeddingModel,
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: embeddingSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeEmbeddingSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "embedding_snapshot_ready:" + embeddingSnapshot.EmbeddingSnapshotID.String(),
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
