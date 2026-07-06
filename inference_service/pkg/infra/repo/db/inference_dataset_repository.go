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

type InferenceDatasetRepository struct {
	coreDB.Database
}

func NewInferenceDatasetRepository(db *coreDB.Database) *InferenceDatasetRepository {
	log.Trace("NewInferenceDatasetRepository")

	return &InferenceDatasetRepository{
		Database: *db,
	}
}

func (r *InferenceDatasetRepository) UpsertDataset(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error) {
	log.Trace("InferenceDatasetRepository UpsertDataset")

	query := `INSERT INTO ` + r.Name + `.inference_datasets (
		dataset_id, user_id, idempotency_key, dataset_version, processing_state, storage_location,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version,
		schema_metadata, raw_snapshot_id, feature_snapshot_id, embedding_snapshot_id, vector_store,
		collection_name, embedding_dimensions, embedding_count, embedding_strategy_version,
		embedding_chunker_name, embedding_chunker_version, embedding_chunk_size, embedding_chunk_overlap,
		embedding_provider, embedding_model
	) VALUES (
		@dataset_id, @user_id, @idempotency_key, @dataset_version, @processing_state::inference_dataset_processing_state_enum, @storage_location,
		@table_namespace, @table_name, @table_format::table_format_enum, @catalog_provider::catalog_provider_enum, @processing_profile::processing_profile_enum, @schema_version,
		@schema_metadata::jsonb, @raw_snapshot_id, @feature_snapshot_id, @embedding_snapshot_id, @vector_store,
		@collection_name, @embedding_dimensions, @embedding_count, @embedding_strategy_version,
		@embedding_chunker_name, @embedding_chunker_version, @embedding_chunk_size, @embedding_chunk_overlap,
		@embedding_provider, @embedding_model
	)
	ON CONFLICT (dataset_id) DO UPDATE SET
		user_id = EXCLUDED.user_id,
		idempotency_key = EXCLUDED.idempotency_key,
		dataset_version = EXCLUDED.dataset_version,
		processing_state = EXCLUDED.processing_state,
		storage_location = EXCLUDED.storage_location,
		table_namespace = EXCLUDED.table_namespace,
		table_name = EXCLUDED.table_name,
		table_format = EXCLUDED.table_format,
		catalog_provider = EXCLUDED.catalog_provider,
		processing_profile = EXCLUDED.processing_profile,
		schema_version = EXCLUDED.schema_version,
		schema_metadata = EXCLUDED.schema_metadata,
		raw_snapshot_id = EXCLUDED.raw_snapshot_id,
		feature_snapshot_id = EXCLUDED.feature_snapshot_id,
		embedding_snapshot_id = EXCLUDED.embedding_snapshot_id,
		vector_store = EXCLUDED.vector_store,
		collection_name = EXCLUDED.collection_name,
		embedding_dimensions = EXCLUDED.embedding_dimensions,
		embedding_count = EXCLUDED.embedding_count,
		embedding_strategy_version = EXCLUDED.embedding_strategy_version,
		embedding_chunker_name = EXCLUDED.embedding_chunker_name,
		embedding_chunker_version = EXCLUDED.embedding_chunker_version,
		embedding_chunk_size = EXCLUDED.embedding_chunk_size,
		embedding_chunk_overlap = EXCLUDED.embedding_chunk_overlap,
		embedding_provider = EXCLUDED.embedding_provider,
		embedding_model = EXCLUDED.embedding_model
	WHERE EXCLUDED.dataset_version >= ` + r.Name + `.inference_datasets.dataset_version
	RETURNING ` + datasetColumns()

	record, err := scanInferenceDataset(r.Pool.QueryRow(ctx, query, datasetArgs(dataset, idempotencyKey)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.ReadDataset(ctx, dataset.UserID, dataset.DatasetID)
		}
		r.LogPoolStatsOnError(ctx, "upsert inference dataset failed", err)
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("upsert inference dataset: %w", err)
	}
	return record, nil
}

func (r *InferenceDatasetRepository) ReadDataset(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.InferenceDataset, error) {
	log.Trace("InferenceDatasetRepository ReadDataset")

	query := `SELECT ` + datasetColumns() + ` FROM ` + r.Name + `.inference_datasets WHERE dataset_id = @dataset_id AND user_id = @user_id`
	record, err := scanInferenceDataset(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
		"user_id":    pgtype.UUID{Bytes: userID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrDatasetNotFound
		}
		r.LogPoolStatsOnError(ctx, "read inference dataset failed", err)
		return nil, fmt.Errorf("read inference dataset: %w", err)
	}
	return record, nil
}

func datasetColumns() string {
	log.Trace("datasetColumns")

	return `dataset_id::text, user_id::text, dataset_version, processing_state::text, storage_location,
		table_namespace, table_name, table_format::text, catalog_provider::text, processing_profile::text, schema_version,
		schema_metadata::text, COALESCE(raw_snapshot_id::text, ''), COALESCE(feature_snapshot_id::text, ''),
		COALESCE(embedding_snapshot_id::text, ''), vector_store, collection_name, embedding_dimensions,
		embedding_count, embedding_strategy_version, embedding_chunker_name, embedding_chunker_version,
		embedding_chunk_size, embedding_chunk_overlap, embedding_provider, embedding_model`
}

func datasetArgs(dataset *model.InferenceDataset, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("datasetArgs")

	return pgx.NamedArgs{
		"dataset_id":                 pgtype.UUID{Bytes: dataset.DatasetID, Valid: true},
		"user_id":                    pgtype.UUID{Bytes: dataset.UserID, Valid: true},
		"idempotency_key":            pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"dataset_version":            dataset.DatasetVersion,
		"processing_state":           dataset.ProcessingState.String(),
		"storage_location":           dataset.StorageLocation,
		"table_namespace":            dataset.TableNamespace,
		"table_name":                 dataset.TableName,
		"table_format":               dataset.TableFormat,
		"catalog_provider":           dataset.CatalogProvider,
		"processing_profile":         dataset.ProcessingProfile,
		"schema_version":             dataset.SchemaVersion,
		"schema_metadata":            jsonObjectOrDefault(dataset.SchemaMetadata),
		"raw_snapshot_id":            nullableUUID(dataset.RawSnapshotID),
		"feature_snapshot_id":        nullableUUID(dataset.FeatureSnapshotID),
		"embedding_snapshot_id":      nullableUUID(dataset.EmbeddingSnapshotID),
		"vector_store":               dataset.VectorStore,
		"collection_name":            dataset.CollectionName,
		"embedding_dimensions":       dataset.EmbeddingDimensions,
		"embedding_count":            dataset.EmbeddingCount,
		"embedding_strategy_version": dataset.EmbeddingStrategyVersion,
		"embedding_chunker_name":     dataset.EmbeddingChunkerName,
		"embedding_chunker_version":  dataset.EmbeddingChunkerVersion,
		"embedding_chunk_size":       dataset.EmbeddingChunkSize,
		"embedding_chunk_overlap":    dataset.EmbeddingChunkOverlap,
		"embedding_provider":         dataset.EmbeddingProvider,
		"embedding_model":            dataset.EmbeddingModel,
	}
}

func scanInferenceDataset(row pgx.Row) (*model.InferenceDataset, error) {
	log.Trace("scanInferenceDataset")

	var datasetID string
	var userID string
	var processingStateRaw string
	var rawSnapshotID string
	var featureSnapshotID string
	var embeddingSnapshotID string
	record := &model.InferenceDataset{}
	if err := row.Scan(
		&datasetID,
		&userID,
		&record.DatasetVersion,
		&processingStateRaw,
		&record.StorageLocation,
		&record.TableNamespace,
		&record.TableName,
		&record.TableFormat,
		&record.CatalogProvider,
		&record.ProcessingProfile,
		&record.SchemaVersion,
		&record.SchemaMetadata,
		&rawSnapshotID,
		&featureSnapshotID,
		&embeddingSnapshotID,
		&record.VectorStore,
		&record.CollectionName,
		&record.EmbeddingDimensions,
		&record.EmbeddingCount,
		&record.EmbeddingStrategyVersion,
		&record.EmbeddingChunkerName,
		&record.EmbeddingChunkerVersion,
		&record.EmbeddingChunkSize,
		&record.EmbeddingChunkOverlap,
		&record.EmbeddingProvider,
		&record.EmbeddingModel,
	); err != nil {
		return nil, err
	}
	processingState, err := model.ToDatasetProcessingState(processingStateRaw)
	if err != nil {
		return nil, err
	}
	record.DatasetID = uuid.MustParse(datasetID)
	record.UserID = uuid.MustParse(userID)
	record.ProcessingState = processingState
	record.RawSnapshotID = parseOptionalUUID(rawSnapshotID)
	record.FeatureSnapshotID = parseOptionalUUID(featureSnapshotID)
	record.EmbeddingSnapshotID = parseOptionalUUID(embeddingSnapshotID)
	return record, nil
}

func nullableUUID(value uuid.UUID) pgtype.UUID {
	log.Trace("nullableUUID")

	if value == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: value, Valid: true}
}

func parseOptionalUUID(value string) uuid.UUID {
	log.Trace("parseOptionalUUID")

	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.Nil
	}
	return uuid.MustParse(value)
}

func jsonObjectOrDefault(value string) string {
	log.Trace("jsonObjectOrDefault")

	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return value
}
