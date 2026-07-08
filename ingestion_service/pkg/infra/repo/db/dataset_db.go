package db

import (
	"context"
	"errors"
	"fmt"

	"ingestion_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"

	"ingestion_service/pkg/domain"
	coreDb "lib/shared_lib/db"
)

type DatasetDB struct {
	coreDb.Database
}

func NewDatasetDB(db *coreDb.Database) *DatasetDB {
	return &DatasetDB{
		*db,
	}
}

// Upsert stores the latest registry-owned dataset metadata projection.
func (db *DatasetDB) Upsert(ctx context.Context, dataset *model.Dataset) error {
	log.Trace("DatasetDB Upsert")

	var sqlStatement = `INSERT INTO ` + db.Name + `.datasets (
		dataset_id, user_id, org_id, storage_location, table_namespace, table_name, table_format,
		catalog_provider, processing_profile, schema_version, schema_metadata, blacklisted
	) VALUES (
		@dataset_id, @user_id, @org_id, @storage_location, @table_namespace, @table_name, @table_format::table_format_enum,
		@catalog_provider::catalog_provider_enum, @processing_profile::processing_profile_enum, @schema_version, @schema_metadata::jsonb, false
	)
	ON CONFLICT (dataset_id) DO UPDATE SET
		user_id = EXCLUDED.user_id,
		org_id = EXCLUDED.org_id,
		storage_location = EXCLUDED.storage_location,
		table_namespace = EXCLUDED.table_namespace,
		table_name = EXCLUDED.table_name,
		table_format = EXCLUDED.table_format,
		catalog_provider = EXCLUDED.catalog_provider,
		processing_profile = EXCLUDED.processing_profile,
		schema_version = EXCLUDED.schema_version,
		schema_metadata = EXCLUDED.schema_metadata;`
	_, err := db.Pool.Exec(ctx, sqlStatement, ToDAO(dataset))
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to upsert dataset")
		if coreDb.IsForeignKeyViolation(err) {
			return domain.ErrDependencyNotReady.Extend("tenant projection is not ready")
		}
		return fmt.Errorf("database error. Failed to upsert dataset: %w", err)
	}
	return nil
}

// BlacklistDataset sets a dataset as blacklisted if it exists
func (db *DatasetDB) BlacklistDataset(ctx context.Context, datasetID, userID uuid.UUID) error {
	log.Trace("DatasetDB BlacklistDataset")

	dto := IDsToDAO(ctx, datasetID, userID)
	var sqlStatement = `UPDATE ` + db.Name + `.datasets SET blacklisted = true WHERE dataset_id = @dataset_id AND org_id = @org_id;`
	cmdTag, err := db.Pool.Exec(ctx, sqlStatement, dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to set dataset as blacklisted")
		return fmt.Errorf("database error. Failed to set dataset as blacklisted: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return domain.ErrResourceNotFound
	}
	return nil
}

// DeleteDataset removes a dataset from the database if it exists
func (db *DatasetDB) DeleteDataset(ctx context.Context, datasetID, userID uuid.UUID) error {
	log.Trace("DatasetDB DeleteDataset")

	dto := IDsToDAO(ctx, datasetID, userID)

	var sqlStatement = `DELETE FROM ` + db.Name + `.datasets WHERE dataset_id = @dataset_id AND org_id = @org_id;`
	cmdTag, err := db.Pool.Exec(ctx, sqlStatement, dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to delete dataset")
		return fmt.Errorf("database error. Failed to delete dataset: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return domain.ErrResourceNotFound
	}
	return nil
}

func (db *DatasetDB) ReadForUpload(ctx context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error) {
	log.Trace("DatasetDB ReadForUpload")

	query := `SELECT dataset_id::text, user_id::text, org_id::text, storage_location, table_namespace, table_name,
		table_format::text, catalog_provider::text, processing_profile::text, schema_version, schema_metadata::text
		FROM ` + db.Name + `.datasets
		WHERE dataset_id = @dataset_id AND org_id = @org_id AND blacklisted = false`
	dataset, err := scanDataset(db.Pool.QueryRow(ctx, query, IDsToDAO(ctx, datasetID, userID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrResourceNotFound
		}
		log.WithContext(ctx).WithError(err).Error("database error. Failed to read dataset for upload")
		return nil, fmt.Errorf("database error. Failed to read dataset for upload: %w", err)
	}
	return dataset, nil
}

func ToDAO(dataset *model.Dataset) pgx.NamedArgs {
	return pgx.NamedArgs{
		"dataset_id":         pgtype.UUID{Bytes: dataset.DatasetID, Valid: true},
		"user_id":            pgtype.UUID{Bytes: dataset.UserID, Valid: true},
		"org_id":             pgtype.UUID{Bytes: dataset.OrgID, Valid: dataset.OrgID != uuid.Nil},
		"storage_location":   dataset.StorageLocation,
		"table_namespace":    dataset.TableNamespace,
		"table_name":         dataset.TableName,
		"table_format":       dataset.TableFormat,
		"catalog_provider":   dataset.CatalogProvider,
		"processing_profile": dataset.ProcessingProfile,
		"schema_version":     dataset.SchemaVersion,
		"schema_metadata":    dataset.SchemaMetadata,
	}
}

func IDsToDAO(ctx context.Context, datasetID, userID uuid.UUID) pgx.NamedArgs {
	return pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
		"user_id":    pgtype.UUID{Bytes: userID, Valid: true},
		"org_id":     pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
	}
}

func scanDataset(row pgx.Row) (*model.Dataset, error) {
	log.Trace("scanDataset")

	var datasetID, userID, orgID string
	dataset := &model.Dataset{}
	if err := row.Scan(
		&datasetID,
		&userID,
		&orgID,
		&dataset.StorageLocation,
		&dataset.TableNamespace,
		&dataset.TableName,
		&dataset.TableFormat,
		&dataset.CatalogProvider,
		&dataset.ProcessingProfile,
		&dataset.SchemaVersion,
		&dataset.SchemaMetadata,
	); err != nil {
		return nil, err
	}
	dataset.DatasetID = uuid.MustParse(datasetID)
	dataset.UserID = uuid.MustParse(userID)
	dataset.OrgID = uuid.MustParse(orgID)
	return dataset, nil
}

func orgIDFromContext(ctx context.Context) uuid.UUID {
	log.Trace("orgIDFromContext")

	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return uuid.Nil
	}
	return orgID
}
