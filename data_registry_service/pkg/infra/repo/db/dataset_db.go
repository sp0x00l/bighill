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
		*db,
	}
}

func (db *datasetDB) Create(ctx context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) error {
	log.Trace("DatasetDB Create")

	datasetModel := Dataset{IdempotencyKey: pgtype.UUID{Bytes: idempotencyKey, Valid: true}}
	datasetDAO := datasetModel.toDAO(dataset)

	var id, userID, origin, status, processingState string
	var sqlStatement = `INSERT INTO ` + db.Name +
		`.datasets (id, user_id, title, description, location, idempotency_key, category,
		table_namespace, table_name, table_format, catalog_provider, schema_version, schema_metadata, processing_state)
		VALUES (@id, @user_id, @title, @description, @location, @idempotency_key, @category,
		@table_namespace, @table_name, @table_format, @catalog_provider, @schema_version, @schema_metadata::jsonb, @processing_state)
		RETURNING id, user_id, origin, status, processing_state;`

	err := db.Pool.QueryRow(ctx,
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

	datasetModel := Dataset{}
	datasetDAO := datasetModel.toDAO(dataset)

	var sqlStatement = `UPDATE ` + db.Name + `.datasets SET title = @title, 
		description = @description, origin = @origin, location = @location, category = @category,
		table_namespace = @table_namespace, table_name = @table_name, table_format = @table_format,
		catalog_provider = @catalog_provider, schema_version = @schema_version, schema_metadata = @schema_metadata::jsonb
		WHERE id = @id AND user_id = @user_id AND status != '` + model.Blacklisted.String() + `' AND deleted = false
		RETURNING title, description, origin, location, status, category,
		table_namespace, table_name, table_format, catalog_provider, schema_version, schema_metadata::text, processing_state::text;`
	row := db.Pool.QueryRow(ctx, sqlStatement, datasetDAO)

	updatedDataset := DatasetDAO{
		ID:     pgtype.UUID{Bytes: dataset.ID, Valid: true},
		UserID: pgtype.UUID{Bytes: dataset.UserID, Valid: true},
	}

	switch err := row.Scan(&updatedDataset.Title,
		&updatedDataset.Description,
		&updatedDataset.Origin,
		&updatedDataset.Location,
		&updatedDataset.Status,
		&updatedDataset.Category,
		&updatedDataset.TableNamespace,
		&updatedDataset.TableName,
		&updatedDataset.TableFormat,
		&updatedDataset.CatalogProvider,
		&updatedDataset.SchemaVersion,
		&updatedDataset.SchemaMetadata,
		&updatedDataset.ProcessingState); err {
	case pgx.ErrNoRows:
		log.WithContext(ctx).Warnf("No dataset found in database for ID: %s", dataset.ID.String())
		return nil, domainErrors.ErrResourceNotFound
	case nil:
		return fromDAO(ctx, &updatedDataset)
	default:
		log.WithContext(ctx).WithError(err).Errorf("database error. Failed to replace dataset %s", dataset.ID.String())
		return nil, fmt.Errorf("database error. Failed to replace dataset: %w", err)
	}
}

func (db *datasetDB) Delete(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	log.Trace("DatasetDB Delete")

	query := fmt.Sprintf(`UPDATE %s.datasets SET deleted = true WHERE id = @id AND user_id = @user_id AND deleted = false;`, db.Name)
	cmdTag, err := db.Pool.Exec(ctx, query, pgx.NamedArgs{"id": datasetID, "user_id": userID})
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

func (db *datasetDB) UpdateProcessingState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, error) {
	log.Trace("DatasetDB UpdateProcessingState")

	query := `UPDATE ` + db.Name + `.datasets
		SET processing_state = @processing_state
		WHERE id = @id AND user_id = @user_id AND status != '` + model.Blacklisted.String() + `' AND deleted = false
		RETURNING id, user_id, title, description, origin, location, status, category,
		table_namespace, table_name, table_format, catalog_provider, schema_version, schema_metadata::text, processing_state::text;`

	return db.scanRow(ctx, db.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"id":               datasetID,
		"user_id":          userID,
		"processing_state": state.String(),
	}))
}

func (db *datasetDB) UpdateMaterializationMetadata(ctx context.Context, dataset *model.Dataset) (*model.Dataset, error) {
	log.Trace("DatasetDB UpdateMaterializationMetadata")

	datasetModel := Dataset{}
	datasetDAO := datasetModel.toDAO(dataset)
	query := `UPDATE ` + db.Name + `.datasets
		SET location = @location,
			table_namespace = @table_namespace,
			table_name = @table_name,
			table_format = @table_format,
			catalog_provider = @catalog_provider,
			schema_version = @schema_version,
			schema_metadata = @schema_metadata::jsonb,
			processing_state = @processing_state
		WHERE id = @id AND user_id = @user_id AND status != '` + model.Blacklisted.String() + `' AND deleted = false
		RETURNING id, user_id, title, description, origin, location, status, category,
		table_namespace, table_name, table_format, catalog_provider, schema_version, schema_metadata::text, processing_state::text;`

	return db.scanRow(ctx, db.Pool.QueryRow(ctx, query, datasetDAO))
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
	return fmt.Sprintf(`SELECT id, user_id, title, description, origin, location, status, category,
	table_namespace, table_name, table_format, catalog_provider, schema_version, schema_metadata::text, processing_state::text
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
			&dataset.Status,
			&dataset.Category,
			&dataset.TableNamespace,
			&dataset.TableName,
			&dataset.TableFormat,
			&dataset.CatalogProvider,
			&dataset.SchemaVersion,
			&dataset.SchemaMetadata,
			&dataset.ProcessingState,
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
		&dataset.Location, &dataset.Status, &dataset.Category, &dataset.TableNamespace, &dataset.TableName,
		&dataset.TableFormat, &dataset.CatalogProvider, &dataset.SchemaVersion, &dataset.SchemaMetadata, &dataset.ProcessingState)
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
