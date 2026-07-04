package db

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"errors"
	"fmt"
	datasetpb "lib/data_contracts_lib/data_registry"
	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"
	core "lib/shared_lib/transport"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type datasetDB struct {
	coreDB.Database
	outbox       msgConn.OrderedOutbox
	topic        string
	outboxSignal func()
}

type DatasetDBOption func(*datasetDB)

func WithTransactionalOutbox(outbox msgConn.OrderedOutbox, topic string) DatasetDBOption {
	log.Trace("WithTransactionalOutbox")

	return func(db *datasetDB) {
		db.outbox = outbox
		db.topic = topic
	}
}

func WithOutboxSignal(signal func()) DatasetDBOption {
	log.Trace("WithOutboxSignal")

	return func(db *datasetDB) {
		db.outboxSignal = signal
	}
}

func NewDatasetDB(db *coreDB.Database, opts ...DatasetDBOption) *datasetDB {
	log.Trace("NewDatasetDB")

	datasetDB := &datasetDB{
		Database: *db,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(datasetDB)
		}
	}
	return datasetDB
}

func (db *datasetDB) Create(ctx context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) error {
	log.Trace("DatasetDB Create")

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin dataset create transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	datasetModel := Dataset{IdempotencyKey: pgtype.UUID{Bytes: idempotencyKey, Valid: true}}
	datasetDAO := datasetModel.toDAO(dataset)

	var id, userID, origin, status, processingState string
	var sqlStatement = `INSERT INTO ` + db.Name +
		`.datasets (id, user_id, title, description, location, source_type, source_connector_id, source_query, source_database, source_collection, idempotency_key, category,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata, processing_state)
		VALUES (@id, @user_id, @title, @description, @location, @source_type::storage_type_enum, @source_connector_id, @source_query, @source_database, @source_collection, @idempotency_key, @category,
		@table_namespace, @table_name, @table_format, @catalog_provider, @processing_profile, @schema_version, @schema_metadata::jsonb, @processing_state)
		RETURNING id, user_id, origin, status, processing_state;`

	err = tx.QueryRow(ctx,
		sqlStatement,
		datasetDAO,
	).Scan(&id, &userID, &origin, &status, &processingState)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgerrcode.UniqueViolation {
				return domainErrors.ErrResourceAlreadyExists
			}
		}
		if coreDB.IsForeignKeyViolation(err) {
			return domainErrors.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return fmt.Errorf("database error. Failed to insert dataset: %w", err)
	}

	dataset.ID = uuid.MustParse(id)
	dataset.Origin, err = model.ToOriginType(origin)
	if err != nil {
		log.WithContext(ctx).Errorf("Error converting origin type: %s", origin)
		return domainErrors.ErrValidationFailed.Extend("failed to convert origin type")
	}
	dataset.Status, err = model.ToStatusType(status)
	if err != nil {
		log.WithContext(ctx).Errorf("Error converting status type: %s", status)
		return domainErrors.ErrValidationFailed.Extend("failed to convert status type")
	}
	dataset.ProcessingState, err = model.ToProcessingState(processingState)
	if err != nil {
		log.WithContext(ctx).Errorf("Error converting processing state: %s", processingState)
		return domainErrors.ErrValidationFailed.Extend("failed to convert processing state")
	}
	enqueued := false
	if db.outbox != nil {
		if err := db.outbox.EnqueueTx(ctx, tx, datasetCreatedMessage(db.topic, dataset)); err != nil {
			return err
		}
		enqueued = true
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dataset create transaction: %w", err)
	}
	if enqueued {
		db.notifyOutbox()
	}
	return nil
}

func (db *datasetDB) ReadPublished(ctx context.Context, pagination core.Pagination, filters []model.Filter) ([]*model.Dataset, int, error) {
	log.Trace("DatasetDB ReadPublished")

	whereClause := fmt.Sprintf("status = '%s' AND deleted = false", model.Published.String())
	return db.readPaginatedDatasets(ctx, whereClause, pagination, filters, uuid.Nil)
}

func (db *datasetDB) ReadPublishedByID(ctx context.Context, datasetID uuid.UUID) (*model.Dataset, error) {
	log.Trace("DatasetDB ReadPublishedByID")

	whereClause := fmt.Sprintf("id = @id AND status = '%s' AND deleted = false", model.Published.String())
	query := db.getSelectSQL(whereClause)
	row := db.Pool.QueryRow(ctx, query, pgx.NamedArgs{"id": datasetID})

	datasetModel, err := db.scanRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return datasetModel, nil
}

func (db *datasetDB) ReadPublishedByUserID(ctx context.Context, userID uuid.UUID, pagination core.Pagination, filters []model.Filter) ([]*model.Dataset, int, error) {
	log.Trace("DatasetDB ReadPublishedByUserID")

	whereClause := fmt.Sprintf("status = '%s' AND user_id = @user_id AND deleted = false", model.Published.String())
	return db.readPaginatedDatasets(ctx, whereClause, pagination, filters, userID)
}

func (db *datasetDB) Read(ctx context.Context, userID uuid.UUID, pagination core.Pagination, filters []model.Filter) ([]*model.Dataset, int, error) {
	log.Trace("DatasetDB Read")

	whereClause := "user_id = @user_id AND deleted = false"
	return db.readPaginatedDatasets(ctx, whereClause, pagination, filters, userID)
}

func (db *datasetDB) ReadByID(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (*model.Dataset, error) {
	log.Trace("DatasetDB ReadByID")

	whereClause := "id = @id AND user_id = @user_id AND deleted = false"
	query := db.getSelectSQL(whereClause)
	row := db.Pool.QueryRow(ctx, query, pgx.NamedArgs{"user_id": userID, "id": datasetID})
	datasetModel, err := db.scanRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return datasetModel, nil
}

func (db *datasetDB) Replace(ctx context.Context, dataset *model.Dataset) (*model.Dataset, error) {
	log.Trace("DatasetDB Replace")

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin dataset replace transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	datasetModel := Dataset{}
	datasetDAO := datasetModel.toDAO(dataset)

	var sqlStatement = `UPDATE ` + db.Name + `.datasets SET title = @title, 
		description = @description, origin = @origin, location = @location,
		source_type = @source_type::storage_type_enum, source_connector_id = @source_connector_id,
		source_query = @source_query, source_database = @source_database, source_collection = @source_collection,
		category = @category,
		table_namespace = @table_namespace, table_name = @table_name, table_format = @table_format,
		catalog_provider = @catalog_provider, processing_profile = @processing_profile, schema_version = @schema_version, schema_metadata = @schema_metadata::jsonb,
		dataset_version = dataset_version + 1
		WHERE id = @id AND user_id = @user_id AND status != '` + model.Blacklisted.String() + `' AND deleted = false
		RETURNING title, description, origin, location, source_type, source_connector_id, source_query, source_database, source_collection, status, category,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata::text, processing_state::text,
		dataset_version, raw_snapshot_id, feature_snapshot_id, embedding_snapshot_id, vector_store, collection_name,
		embedding_dimensions, embedding_count, embedding_strategy_version, embedding_chunker_name, embedding_chunker_version,
		embedding_chunk_size, embedding_chunk_overlap, embedding_provider, embedding_model;`
	row := tx.QueryRow(ctx, sqlStatement, datasetDAO)

	updatedDataset := DatasetDAO{
		ID:     pgtype.UUID{Bytes: dataset.ID, Valid: true},
		UserID: pgtype.UUID{Bytes: dataset.UserID, Valid: true},
	}

	switch err := row.Scan(&updatedDataset.Title,
		&updatedDataset.Description,
		&updatedDataset.Origin,
		&updatedDataset.Location,
		&updatedDataset.SourceType,
		&updatedDataset.SourceConnectorID,
		&updatedDataset.SourceQuery,
		&updatedDataset.SourceDatabase,
		&updatedDataset.SourceCollection,
		&updatedDataset.Status,
		&updatedDataset.Category,
		&updatedDataset.TableNamespace,
		&updatedDataset.TableName,
		&updatedDataset.TableFormat,
		&updatedDataset.CatalogProvider,
		&updatedDataset.ProcessingProfile,
		&updatedDataset.SchemaVersion,
		&updatedDataset.SchemaMetadata,
		&updatedDataset.ProcessingState,
		&updatedDataset.DatasetVersion,
		&updatedDataset.RawSnapshotID,
		&updatedDataset.FeatureSnapshotID,
		&updatedDataset.EmbeddingSnapshotID,
		&updatedDataset.VectorStore,
		&updatedDataset.CollectionName,
		&updatedDataset.EmbeddingDimensions,
		&updatedDataset.EmbeddingCount,
		&updatedDataset.EmbeddingStrategyVersion,
		&updatedDataset.EmbeddingChunkerName,
		&updatedDataset.EmbeddingChunkerVersion,
		&updatedDataset.EmbeddingChunkSize,
		&updatedDataset.EmbeddingChunkOverlap,
		&updatedDataset.EmbeddingProvider,
		&updatedDataset.EmbeddingModel); err {
	case pgx.ErrNoRows:
		log.WithContext(ctx).Warnf("No dataset found in database for ID: %s", dataset.ID.String())
		return nil, domainErrors.ErrResourceNotFound
	case nil:
		updated, err := fromDAO(ctx, &updatedDataset)
		if err != nil {
			return nil, err
		}
		enqueued := false
		if db.outbox != nil {
			if err := db.outbox.EnqueueTx(ctx, tx, datasetUpdatedMessage(db.topic, updated)); err != nil {
				return nil, err
			}
			enqueued = true
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit dataset replace transaction: %w", err)
		}
		if enqueued {
			db.notifyOutbox()
		}
		return updated, nil
	default:
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to replace dataset %s", dataset.ID.String())
		return nil, fmt.Errorf("database error. Failed to replace dataset: %w", err)
	}
}

func (db *datasetDB) Delete(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	log.Trace("DatasetDB Delete")

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin dataset delete transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf(`UPDATE %s.datasets SET deleted = true WHERE id = @id AND user_id = @user_id AND deleted = false;`, db.Name)
	cmdTag, err := tx.Exec(ctx, query, pgx.NamedArgs{"id": datasetID, "user_id": userID})
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to delete dataset %s", datasetID.String())
		wrappedErr := fmt.Errorf("database error. Failed to delete dataset: %w", err)
		return wrappedErr
	}
	if cmdTag.RowsAffected() == 0 {
		return domainErrors.ErrResourceNotFound
	}
	enqueued := false
	if db.outbox != nil {
		if err := db.outbox.EnqueueTx(ctx, tx, datasetDeletedMessage(db.topic, datasetID, userID)); err != nil {
			return err
		}
		enqueued = true
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dataset delete transaction: %w", err)
	}
	if enqueued {
		db.notifyOutbox()
	}
	return nil
}

func (db *datasetDB) UpdateProcessingState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, bool, error) {
	log.Trace("DatasetDB UpdateProcessingState")

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("begin dataset processing state transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	requestedRank := processingStateRankSQL("@processing_state")
	currentRank := processingStateRankSQL("processing_state")
	query := `UPDATE ` + db.Name + `.datasets
		SET processing_state = @processing_state::dataset_processing_state_enum,
			dataset_version = dataset_version + 1
		WHERE id = @id AND user_id = @user_id AND status != '` + model.Blacklisted.String() + `' AND deleted = false
			AND ` + requestedRank + ` > ` + currentRank + `
		RETURNING id, user_id, title, description, origin, location, source_type, source_connector_id, source_query, source_database, source_collection, status, category,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata::text, processing_state::text,
		dataset_version, raw_snapshot_id, feature_snapshot_id, embedding_snapshot_id, vector_store, collection_name,
		embedding_dimensions, embedding_count, embedding_strategy_version, embedding_chunker_name, embedding_chunker_version,
		embedding_chunk_size, embedding_chunk_overlap, embedding_provider, embedding_model;`

	updated, err := db.scanRow(ctx, tx.QueryRow(ctx, query, pgx.NamedArgs{
		"id":               datasetID,
		"user_id":          userID,
		"processing_state": state.String(),
	}))
	if err == nil {
		enqueued := false
		if db.outbox != nil {
			if err := db.outbox.EnqueueTx(ctx, tx, datasetUpdatedMessage(db.topic, updated)); err != nil {
				return nil, false, err
			}
			enqueued = true
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("commit dataset processing state transaction: %w", err)
		}
		if enqueued {
			db.notifyOutbox()
		}
		return updated, true, nil
	}
	if !errors.Is(err, domainErrors.ErrResourceNotFound) {
		return nil, false, err
	}
	if err := tx.Rollback(ctx); err != nil {
		return nil, false, fmt.Errorf("rollback unchanged dataset processing state transaction: %w", err)
	}
	current, err := db.ReadByID(ctx, datasetID, userID)
	if err != nil {
		return nil, false, err
	}
	if current.Status == model.Blacklisted {
		return nil, false, domainErrors.ErrResourceNotFound
	}
	return current, false, nil
}

func (db *datasetDB) RecordMaterialization(ctx context.Context, materialized *model.Dataset, state model.ProcessingState) (*model.Dataset, error) {
	log.Trace("DatasetDB RecordMaterialization")

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		db.LogPoolStatsOnError(ctx, "begin dataset materialization transaction failed", err)
		return nil, fmt.Errorf("begin dataset materialization transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	updated, changed, err := db.recordMaterializationTx(ctx, tx, materialized, state)
	if err != nil {
		return nil, err
	}
	enqueued := false
	if changed && db.outbox != nil {
		if err := db.outbox.EnqueueTx(ctx, tx, datasetUpdatedMessage(db.topic, updated)); err != nil {
			return nil, fmt.Errorf("enqueue dataset updated: %w", err)
		}
		enqueued = true
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit dataset materialization transaction: %w", err)
	}
	if enqueued {
		db.notifyOutbox()
	}
	return updated, nil
}

func (db *datasetDB) notifyOutbox() {
	log.Trace("DatasetDB notifyOutbox")

	if db.outboxSignal != nil {
		db.outboxSignal()
	}
}

func (db *datasetDB) recordMaterializationTx(ctx context.Context, tx pgx.Tx, materialized *model.Dataset, state model.ProcessingState) (*model.Dataset, bool, error) {
	log.Trace("DatasetDB recordMaterializationTx")

	datasetDAO := (&Dataset{}).toDAO(materialized)
	applyMaterializationOptionalFields(datasetDAO, materialized)
	datasetDAO["processing_state"] = pgtype.Text{String: state.String(), Valid: true}

	requestedRank := processingStateRankSQL("@processing_state")
	currentRank := processingStateRankSQL("d.processing_state")
	query := `UPDATE ` + db.Name + `.datasets d
		SET location = COALESCE(NULLIF(@location, ''), d.location),
			table_namespace = COALESCE(NULLIF(@table_namespace, ''), d.table_namespace),
			table_name = COALESCE(NULLIF(@table_name, ''), d.table_name),
			table_format = COALESCE(NULLIF(@table_format, '')::table_format_enum, d.table_format),
			catalog_provider = COALESCE(NULLIF(@catalog_provider, '')::catalog_provider_enum, d.catalog_provider),
			processing_profile = COALESCE(NULLIF(@processing_profile, '')::processing_profile_enum, d.processing_profile),
			schema_version = CASE WHEN @schema_version > 0 THEN @schema_version ELSE d.schema_version END,
			schema_metadata = COALESCE(NULLIF(@schema_metadata, '')::jsonb, d.schema_metadata),
			processing_state = CASE
				WHEN ` + requestedRank + ` > ` + currentRank + ` THEN @processing_state::dataset_processing_state_enum
				ELSE d.processing_state
			END,
			dataset_version = d.dataset_version + 1,
			raw_snapshot_id = COALESCE(@raw_snapshot_id, d.raw_snapshot_id),
			feature_snapshot_id = COALESCE(@feature_snapshot_id, d.feature_snapshot_id),
			embedding_snapshot_id = COALESCE(@embedding_snapshot_id, d.embedding_snapshot_id),
			vector_store = COALESCE(NULLIF(@vector_store, ''), d.vector_store),
			collection_name = COALESCE(NULLIF(@collection_name, ''), d.collection_name),
			embedding_dimensions = CASE WHEN @embedding_dimensions > 0 THEN @embedding_dimensions ELSE d.embedding_dimensions END,
			embedding_count = CASE WHEN @embedding_count > 0 THEN @embedding_count ELSE d.embedding_count END,
			embedding_strategy_version = COALESCE(NULLIF(@embedding_strategy_version, ''), d.embedding_strategy_version),
			embedding_chunker_name = COALESCE(NULLIF(@embedding_chunker_name, ''), d.embedding_chunker_name),
			embedding_chunker_version = COALESCE(NULLIF(@embedding_chunker_version, ''), d.embedding_chunker_version),
			embedding_chunk_size = CASE WHEN @embedding_chunk_size > 0 THEN @embedding_chunk_size ELSE d.embedding_chunk_size END,
			embedding_chunk_overlap = CASE WHEN @embedding_chunk_overlap > 0 THEN @embedding_chunk_overlap ELSE d.embedding_chunk_overlap END,
			embedding_provider = COALESCE(NULLIF(@embedding_provider, ''), d.embedding_provider),
			embedding_model = COALESCE(NULLIF(@embedding_model, ''), d.embedding_model)
		WHERE d.id = @id
			AND d.user_id = @user_id
			AND d.status != '` + model.Blacklisted.String() + `'
			AND d.deleted = false
			AND (
				` + requestedRank + ` > ` + currentRank + `
				OR d.location IS DISTINCT FROM COALESCE(NULLIF(@location, ''), d.location)
				OR d.table_namespace IS DISTINCT FROM COALESCE(NULLIF(@table_namespace, ''), d.table_namespace)
				OR d.table_name IS DISTINCT FROM COALESCE(NULLIF(@table_name, ''), d.table_name)
				OR d.table_format IS DISTINCT FROM COALESCE(NULLIF(@table_format, '')::table_format_enum, d.table_format)
				OR d.catalog_provider IS DISTINCT FROM COALESCE(NULLIF(@catalog_provider, '')::catalog_provider_enum, d.catalog_provider)
				OR d.processing_profile IS DISTINCT FROM COALESCE(NULLIF(@processing_profile, '')::processing_profile_enum, d.processing_profile)
				OR d.schema_version IS DISTINCT FROM CASE WHEN @schema_version > 0 THEN @schema_version ELSE d.schema_version END
				OR d.schema_metadata IS DISTINCT FROM COALESCE(NULLIF(@schema_metadata, '')::jsonb, d.schema_metadata)
				OR d.raw_snapshot_id IS DISTINCT FROM COALESCE(@raw_snapshot_id, d.raw_snapshot_id)
				OR d.feature_snapshot_id IS DISTINCT FROM COALESCE(@feature_snapshot_id, d.feature_snapshot_id)
				OR d.embedding_snapshot_id IS DISTINCT FROM COALESCE(@embedding_snapshot_id, d.embedding_snapshot_id)
				OR d.vector_store IS DISTINCT FROM COALESCE(NULLIF(@vector_store, ''), d.vector_store)
				OR d.collection_name IS DISTINCT FROM COALESCE(NULLIF(@collection_name, ''), d.collection_name)
				OR d.embedding_dimensions IS DISTINCT FROM CASE WHEN @embedding_dimensions > 0 THEN @embedding_dimensions ELSE d.embedding_dimensions END
				OR d.embedding_count IS DISTINCT FROM CASE WHEN @embedding_count > 0 THEN @embedding_count ELSE d.embedding_count END
				OR d.embedding_strategy_version IS DISTINCT FROM COALESCE(NULLIF(@embedding_strategy_version, ''), d.embedding_strategy_version)
				OR d.embedding_chunker_name IS DISTINCT FROM COALESCE(NULLIF(@embedding_chunker_name, ''), d.embedding_chunker_name)
				OR d.embedding_chunker_version IS DISTINCT FROM COALESCE(NULLIF(@embedding_chunker_version, ''), d.embedding_chunker_version)
				OR d.embedding_chunk_size IS DISTINCT FROM CASE WHEN @embedding_chunk_size > 0 THEN @embedding_chunk_size ELSE d.embedding_chunk_size END
				OR d.embedding_chunk_overlap IS DISTINCT FROM CASE WHEN @embedding_chunk_overlap > 0 THEN @embedding_chunk_overlap ELSE d.embedding_chunk_overlap END
				OR d.embedding_provider IS DISTINCT FROM COALESCE(NULLIF(@embedding_provider, ''), d.embedding_provider)
				OR d.embedding_model IS DISTINCT FROM COALESCE(NULLIF(@embedding_model, ''), d.embedding_model)
			)
		RETURNING d.id, d.user_id, d.title, d.description, d.origin, d.location, d.source_type, d.source_connector_id, d.source_query, d.source_database, d.source_collection, d.status, d.category,
			d.table_namespace, d.table_name, d.table_format, d.catalog_provider, d.processing_profile, d.schema_version, d.schema_metadata::text, d.processing_state::text,
			d.dataset_version, d.raw_snapshot_id, d.feature_snapshot_id, d.embedding_snapshot_id, d.vector_store, d.collection_name,
			d.embedding_dimensions, d.embedding_count, d.embedding_strategy_version, d.embedding_chunker_name, d.embedding_chunker_version,
			d.embedding_chunk_size, d.embedding_chunk_overlap, d.embedding_provider, d.embedding_model`

	updated, err := db.scanRow(ctx, tx.QueryRow(ctx, query, datasetDAO))
	if err == nil {
		return updated, true, nil
	}
	if !errors.Is(err, domainErrors.ErrResourceNotFound) {
		return nil, false, err
	}
	current, err := db.readMaterializationDatasetTx(ctx, tx, materialized.ID, materialized.UserID)
	if err != nil {
		return nil, false, err
	}
	return current, false, nil
}

func applyMaterializationOptionalFields(datasetDAO pgx.NamedArgs, materialized *model.Dataset) {
	log.Trace("applyMaterializationOptionalFields")

	if hasTableMaterializationMetadata(materialized) {
		return
	}
	datasetDAO["table_format"] = pgtype.Text{String: "", Valid: true}
	datasetDAO["catalog_provider"] = pgtype.Text{String: "", Valid: true}
	datasetDAO["processing_profile"] = pgtype.Text{String: "", Valid: true}
}

func hasTableMaterializationMetadata(dataset *model.Dataset) bool {
	log.Trace("hasTableMaterializationMetadata")

	if dataset == nil {
		return false
	}
	return strings.TrimSpace(dataset.Location) != "" ||
		strings.TrimSpace(dataset.TableNamespace) != "" ||
		strings.TrimSpace(dataset.TableName) != "" ||
		dataset.RawSnapshotID != uuid.Nil
}

func (db *datasetDB) readMaterializationDatasetTx(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, userID uuid.UUID) (*model.Dataset, error) {
	log.Trace("DatasetDB readMaterializationDatasetTx")

	whereClause := "id = @id AND user_id = @user_id AND status != '" + model.Blacklisted.String() + "' AND deleted = false"
	query := db.getSelectSQL(whereClause)
	return db.scanRow(ctx, tx.QueryRow(ctx, query, pgx.NamedArgs{"user_id": userID, "id": datasetID}))
}

func (db *datasetDB) UpdatePublishedState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	log.Trace("DatasetDB UpdatePublishedState")

	sqlStatement := fmt.Sprintf(`UPDATE %s.datasets SET status = '%s', published_at = LOCALTIMESTAMP 
	WHERE id = @id AND user_id = @user_id AND status = '%s' AND deleted = false;`, db.Name, model.Published.String(), model.Draft.String())
	cmdTag, err := db.Pool.Exec(ctx, sqlStatement, pgx.NamedArgs{"id": datasetID, "user_id": userID})
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to update dataset published status for dataset ID: %s", datasetID.String())
		wrappedErr := fmt.Errorf("database error. Failed to update dataset published status: %w", err)
		return wrappedErr
	}
	if cmdTag.RowsAffected() == 0 {
		return domainErrors.ErrResourceNotFound
	}
	return nil
}

func (db *datasetDB) readPaginatedDatasets(ctx context.Context, clause string, pagination core.Pagination, filters []model.Filter, userID uuid.UUID) ([]*model.Dataset, int, error) {
	log.Trace("DatasetDB readPaginatedDatasets")

	var args pgx.NamedArgs = make(map[string]any)
	if userID != uuid.Nil {
		args["user_id"] = userID
	}
	filter, err := getSQLFilterAndFillArguments(filters, clause, args)
	if err != nil {
		wrappedErr := fmt.Errorf("failed to get SQL filter and fill arguments: %w", err)
		log.WithContext(ctx).Error(wrappedErr)
		return nil, 0, wrappedErr
	}

	selectCount := db.getCountSQL(filter)
	var count int
	if err := db.Pool.QueryRow(ctx, selectCount, args).Scan(&count); err != nil {
		wrappedErr := fmt.Errorf("database error. Failed to execute query: %w", err)
		log.WithContext(ctx).Error(wrappedErr)
		return nil, 0, wrappedErr
	}

	if count == 0 {
		log.WithContext(ctx).Warn("No datasets found")
		return nil, 0, domainErrors.ErrResourceNotFound
	}

	if !pagination.IsValidForCount(count) {
		log.WithContext(ctx).Warnf("No datasets found in database on page: %d with size:%d", pagination.Page, pagination.Limit)
		return nil, count, nil
	}

	dataSelect := db.getPaginatedSelectSQL(filter, pagination)
	rows, err := db.Pool.Query(ctx, dataSelect, args)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to read datasets")
		wrappedErr := fmt.Errorf("database error. Failed read datasets: %w", err)
		return nil, 0, wrappedErr
	}

	defer rows.Close()

	datasets, err := db.scanRows(ctx, rows)
	if err != nil {
		return nil, 0, err
	}
	return datasets, count, nil
}

func getSQLFilterAndFillArguments(filters []model.Filter, extraClause string, args pgx.NamedArgs) (string, error) {
	log.Trace("db GetSQLFilterAndFillArguments")
	var sqlFilters []string

	for _, filter := range filters {
		var field string

		switch filter.GetType() {
		case model.FilterByCategory:
			field = "category"

		default:
			return "", domainErrors.ErrValidationFailed.Extend("filter type not supported")
		}

		sqlfilter := filter.GetFilterAndFillArguments(field, args)
		sqlFilters = append(sqlFilters, sqlfilter)
	}

	if len(sqlFilters) > 0 {
		filtersSQL := "AND " + strings.Join(sqlFilters, " AND ")
		return fmt.Sprintf("%s %s", extraClause, filtersSQL), nil
	}
	return extraClause, nil
}

func (db *datasetDB) getCountSQL(filter string) string {
	return fmt.Sprintf(`SELECT COUNT(id) FROM %s.datasets WHERE %s;`, db.Name, filter)
}

func (db *datasetDB) getPaginatedSelectSQL(filter string, pagination core.Pagination) string {
	withPagination := fmt.Sprintf("%s LIMIT %d OFFSET %d", filter, pagination.Limit, pagination.GetOffset())
	return db.getSelectSQL(withPagination)
}

func (db *datasetDB) getSelectSQL(filter string) string {
	return fmt.Sprintf(`SELECT id, user_id, title, description, origin, location, source_type, source_connector_id, source_query, source_database, source_collection, status, category,
	table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata::text, processing_state::text,
	dataset_version, raw_snapshot_id, feature_snapshot_id, embedding_snapshot_id, vector_store, collection_name,
	embedding_dimensions, embedding_count, embedding_strategy_version, embedding_chunker_name, embedding_chunker_version,
	embedding_chunk_size, embedding_chunk_overlap, embedding_provider, embedding_model
	FROM %s.datasets WHERE %s;`, db.Name, filter)
}

func (db *datasetDB) scanRows(ctx context.Context, rows pgx.Rows) ([]*model.Dataset, error) {
	log.Trace("DatasetDB scanRows")

	var datasets []*model.Dataset
	for rows.Next() {
		var dataset DatasetDAO
		err := rows.Scan(
			&dataset.ID,
			&dataset.UserID,
			&dataset.Title,
			&dataset.Description,
			&dataset.Origin,
			&dataset.Location,
			&dataset.SourceType,
			&dataset.SourceConnectorID,
			&dataset.SourceQuery,
			&dataset.SourceDatabase,
			&dataset.SourceCollection,
			&dataset.Status,
			&dataset.Category,
			&dataset.TableNamespace,
			&dataset.TableName,
			&dataset.TableFormat,
			&dataset.CatalogProvider,
			&dataset.ProcessingProfile,
			&dataset.SchemaVersion,
			&dataset.SchemaMetadata,
			&dataset.ProcessingState,
			&dataset.DatasetVersion,
			&dataset.RawSnapshotID,
			&dataset.FeatureSnapshotID,
			&dataset.EmbeddingSnapshotID,
			&dataset.VectorStore,
			&dataset.CollectionName,
			&dataset.EmbeddingDimensions,
			&dataset.EmbeddingCount,
			&dataset.EmbeddingStrategyVersion,
			&dataset.EmbeddingChunkerName,
			&dataset.EmbeddingChunkerVersion,
			&dataset.EmbeddingChunkSize,
			&dataset.EmbeddingChunkOverlap,
			&dataset.EmbeddingProvider,
			&dataset.EmbeddingModel,
		)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("Failed to read datasets")
			wrappedErr := fmt.Errorf("failed to read datasets: %w", err)
			return nil, wrappedErr
		}
		datasetModel, err := fromDAO(ctx, &dataset)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, datasetModel)
	}

	return datasets, nil
}

func (db *datasetDB) scanRow(ctx context.Context, row pgx.Row) (*model.Dataset, error) {
	log.Trace("DatasetDB scanRow")

	var dataset DatasetDAO
	err := row.Scan(&dataset.ID, &dataset.UserID, &dataset.Title, &dataset.Description, &dataset.Origin,
		&dataset.Location, &dataset.SourceType, &dataset.SourceConnectorID, &dataset.SourceQuery, &dataset.SourceDatabase, &dataset.SourceCollection,
		&dataset.Status, &dataset.Category, &dataset.TableNamespace, &dataset.TableName,
		&dataset.TableFormat, &dataset.CatalogProvider, &dataset.ProcessingProfile, &dataset.SchemaVersion, &dataset.SchemaMetadata, &dataset.ProcessingState,
		&dataset.DatasetVersion, &dataset.RawSnapshotID, &dataset.FeatureSnapshotID, &dataset.EmbeddingSnapshotID,
		&dataset.VectorStore, &dataset.CollectionName, &dataset.EmbeddingDimensions, &dataset.EmbeddingCount,
		&dataset.EmbeddingStrategyVersion, &dataset.EmbeddingChunkerName, &dataset.EmbeddingChunkerVersion,
		&dataset.EmbeddingChunkSize, &dataset.EmbeddingChunkOverlap, &dataset.EmbeddingProvider, &dataset.EmbeddingModel)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domainErrors.ErrResourceNotFound
		}
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to read corrupt dataset.")
		wrappedErr := fmt.Errorf("database error. Failed read dataset: %w", err)
		return nil, wrappedErr
	}

	datasetModel, err := fromDAO(ctx, &dataset)
	if err != nil {
		return nil, err
	}
	return datasetModel, nil
}

func processingStateRankSQL(expression string) string {
	log.Trace("processingStateRankSQL")

	return `(CASE ` + expression + `::text
		WHEN 'PENDING' THEN 0
		WHEN 'RAW_MATERIALIZED' THEN 1
		WHEN 'FEATURE_MATERIALIZED' THEN 2
		WHEN 'EMBEDDINGS_MATERIALIZED' THEN 3
		WHEN 'FAILED' THEN 4
		ELSE -1
	END)`
}

func datasetUpdatedMessage(topic string, dataset *model.Dataset) msgConn.OutboundMessage {
	log.Trace("datasetUpdatedMessage")

	payload := mustMarshalDataset(&datasetpb.DatasetUpdatedEvent{
		DatasetId:                dataset.ID.String(),
		UserId:                   dataset.UserID.String(),
		DatasetVersion:           int32(dataset.DatasetVersion),
		ProcessingState:          dataset.ProcessingState.String(),
		StorageLocation:          dataset.Location,
		TableNamespace:           dataset.TableNamespace,
		TableName:                dataset.TableName,
		TableFormat:              dataset.TableFormat.String(),
		CatalogProvider:          dataset.CatalogProvider.String(),
		ProcessingProfile:        dataset.ProcessingProfile.String(),
		SchemaVersion:            int32(dataset.SchemaVersion),
		SchemaMetadata:           dataset.SchemaMetadata,
		RawSnapshotId:            uuidToString(dataset.RawSnapshotID),
		FeatureSnapshotId:        uuidToString(dataset.FeatureSnapshotID),
		EmbeddingSnapshotId:      uuidToString(dataset.EmbeddingSnapshotID),
		VectorStore:              dataset.VectorStore,
		CollectionName:           dataset.CollectionName,
		EmbeddingDimensions:      int32(dataset.EmbeddingDimensions),
		EmbeddingCount:           dataset.EmbeddingCount,
		EmbeddingStrategyVersion: dataset.EmbeddingStrategyVersion,
		EmbeddingChunkerName:     dataset.EmbeddingChunkerName,
		EmbeddingChunkerVersion:  dataset.EmbeddingChunkerVersion,
		EmbeddingChunkSize:       int32(dataset.EmbeddingChunkSize),
		EmbeddingChunkOverlap:    int32(dataset.EmbeddingChunkOverlap),
		EmbeddingProvider:        dataset.EmbeddingProvider,
		EmbeddingModel:           dataset.EmbeddingModel,
		SourceType:               datasetSourceType(dataset),
		SourceConnectorId:        uuidToString(dataset.SourceConnectorID),
		SourceQuery:              dataset.SourceQuery,
		SourceDatabase:           dataset.SourceDatabase,
		SourceCollection:         dataset.SourceCollection,
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: dataset.ID,
			MsgType:     msgConn.MsgTypeDatasetUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("dataset_updated:%s:%d", dataset.ID, dataset.DatasetVersion),
	}
}

func datasetCreatedMessage(topic string, dataset *model.Dataset) msgConn.OutboundMessage {
	log.Trace("datasetCreatedMessage")

	payload := mustMarshalDataset(&datasetpb.DatasetCreatedEvent{
		DatasetId:         dataset.ID.String(),
		UserId:            dataset.UserID.String(),
		DatasetVersion:    int32(dataset.DatasetVersion),
		ProcessingState:   dataset.ProcessingState.String(),
		StorageLocation:   dataset.Location,
		TableNamespace:    dataset.TableNamespace,
		TableName:         dataset.TableName,
		TableFormat:       dataset.TableFormat.String(),
		CatalogProvider:   dataset.CatalogProvider.String(),
		ProcessingProfile: dataset.ProcessingProfile.String(),
		SchemaVersion:     int32(dataset.SchemaVersion),
		SchemaMetadata:    dataset.SchemaMetadata,
		SourceType:        datasetSourceType(dataset),
		SourceConnectorId: uuidToString(dataset.SourceConnectorID),
		SourceQuery:       dataset.SourceQuery,
		SourceDatabase:    dataset.SourceDatabase,
		SourceCollection:  dataset.SourceCollection,
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: dataset.ID,
			MsgType:     msgConn.MsgTypeDatasetCreated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("dataset_created:%s:%d", dataset.ID, dataset.DatasetVersion),
	}
}

func datasetDeletedMessage(topic string, datasetID uuid.UUID, userID uuid.UUID) msgConn.OutboundMessage {
	log.Trace("datasetDeletedMessage")

	payload := mustMarshalDataset(&datasetpb.DatasetDeletedEvent{
		DatasetId: datasetID.String(),
		UserId:    userID.String(),
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: datasetID,
			MsgType:     msgConn.MsgTypeDatasetDeleted,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("dataset_deleted:%s", datasetID),
	}
}

func uuidToString(id uuid.UUID) string {
	log.Trace("uuidToString")

	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func datasetSourceType(dataset *model.Dataset) string {
	log.Trace("datasetSourceType")

	if dataset == nil || dataset.SourceConnectorID == uuid.Nil {
		return ""
	}
	return dataset.SourceType.String()
}

func mustMarshalDataset(payload proto.Message) []byte {
	log.Trace("mustMarshalDataset")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}
