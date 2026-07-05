package db

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"errors"
	"fmt"
	coreDB "lib/shared_lib/db"
	core "lib/shared_lib/transport"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type datasetDB struct {
	coreDB.Database
}

func NewDatasetDB(db *coreDB.Database) *datasetDB {
	log.Trace("NewDatasetDB")

	return &datasetDB{
		Database: *db,
	}
}

func (db *datasetDB) Create(ctx context.Context, tx pgx.Tx, dataset *model.Dataset, idempotencyKey uuid.UUID) error {
	log.Trace("DatasetDB Create")

	datasetModel := Dataset{IdempotencyKey: pgtype.UUID{Bytes: idempotencyKey, Valid: true}}
	datasetDAO := datasetModel.toDAO(dataset)

	var id, userID, origin, status, processingState string
	var sqlStatement = `INSERT INTO ` + db.Name +
		`.datasets (id, user_id, title, description, location, source_type, source_connector_id, source_query, source_database, source_collection, idempotency_key, category,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata, processing_state)
		VALUES (@id, @user_id, @title, @description, @location, @source_type::storage_type_enum, @source_connector_id, @source_query, @source_database, @source_collection, @idempotency_key, @category,
		@table_namespace, @table_name, @table_format, @catalog_provider, @processing_profile, @schema_version, @schema_metadata::jsonb, @processing_state)
		RETURNING id, user_id, origin, status, processing_state;`

	err := tx.QueryRow(ctx,
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
		if coreDB.IsForeignKeyViolation(err) || coreDB.IsRowLevelSecurityViolation(err) {
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
	return nil
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
	datasetModel, err := fromDatasetRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return datasetModel, nil
}

func (db *datasetDB) Replace(ctx context.Context, tx pgx.Tx, dataset *model.Dataset) (*model.Dataset, error) {
	log.Trace("DatasetDB Replace")

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
		RETURNING id, user_id, title, description, origin, location, source_type, source_connector_id, source_query, source_database, source_collection, status, category,
		table_namespace, table_name, table_format, catalog_provider, processing_profile, schema_version, schema_metadata::text, processing_state::text,
		dataset_version, raw_snapshot_id, feature_snapshot_id, embedding_snapshot_id, vector_store, collection_name,
		embedding_dimensions, embedding_count, embedding_strategy_version, embedding_chunker_name, embedding_chunker_version,
		embedding_chunk_size, embedding_chunk_overlap, embedding_provider, embedding_model;`
	row := tx.QueryRow(ctx, sqlStatement, datasetDAO)

	updated, err := fromDatasetRow(ctx, row)
	switch {
	case errors.Is(err, domainErrors.ErrResourceNotFound):
		log.WithContext(ctx).Warnf("No dataset found in database for ID: %s", dataset.ID.String())
		return nil, domainErrors.ErrResourceNotFound
	case err == nil:
		return updated, nil
	default:
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to replace dataset %s", dataset.ID.String())
		return nil, fmt.Errorf("database error. Failed to replace dataset: %w", err)
	}
}

func (db *datasetDB) Delete(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, userID uuid.UUID) error {
	log.Trace("DatasetDB Delete")

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
	return nil
}

func (db *datasetDB) UpdateProcessingState(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, bool, error) {
	log.Trace("DatasetDB UpdateProcessingState")

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

	updated, err := fromDatasetRow(ctx, tx.QueryRow(ctx, query, pgx.NamedArgs{
		"id":               datasetID,
		"user_id":          userID,
		"processing_state": state.String(),
	}))
	if err == nil {
		return updated, true, nil
	}
	if !errors.Is(err, domainErrors.ErrResourceNotFound) {
		return nil, false, err
	}
	current, err := db.readMaterializationDatasetTx(ctx, tx, datasetID, userID)
	if err != nil {
		return nil, false, err
	}
	if current.Status == model.Blacklisted {
		return nil, false, domainErrors.ErrResourceNotFound
	}
	return current, false, nil
}

func (db *datasetDB) RecordMaterialization(ctx context.Context, tx pgx.Tx, materialized *model.Dataset, state model.ProcessingState) (*model.Dataset, bool, error) {
	log.Trace("DatasetDB RecordMaterialization")

	updated, changed, err := db.recordMaterializationTx(ctx, tx, materialized, state)
	if err != nil {
		return nil, false, err
	}
	return updated, changed, nil
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

	updated, err := fromDatasetRow(ctx, tx.QueryRow(ctx, query, datasetDAO))
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
	return fromDatasetRow(ctx, tx.QueryRow(ctx, query, pgx.NamedArgs{"user_id": userID, "id": datasetID}))
}

func (db *datasetDB) UpdatePublishedState(ctx context.Context, tx pgx.Tx, datasetID uuid.UUID, userID uuid.UUID) error {
	log.Trace("DatasetDB UpdatePublishedState")

	sqlStatement := fmt.Sprintf(`UPDATE %s.datasets SET status = '%s', published_at = LOCALTIMESTAMP 
	WHERE id = @id AND user_id = @user_id AND status = '%s' AND deleted = false;`, db.Name, model.Published.String(), model.Draft.String())
	cmdTag, err := tx.Exec(ctx, sqlStatement, pgx.NamedArgs{"id": datasetID, "user_id": userID})
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
		datasetModel, err := fromDatasetRow(ctx, rows)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, datasetModel)
	}
	if err := rows.Err(); err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to iterate datasets")
		return nil, fmt.Errorf("database error. Failed to iterate datasets: %w", err)
	}

	return datasets, nil
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
