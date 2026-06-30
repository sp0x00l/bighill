package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"

	"data_ingestion_service/pkg/domain"
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

// Save adds a dataset to the database if it does not already exist
func (db *DatasetDB) Save(ctx context.Context, datasetID, userID uuid.UUID) error {
	log.Trace("DatasetDB Save")

	dto := ToDAO(datasetID, userID)

	var sqlStatement = `INSERT INTO ` + db.Name + `.datasets (dataset_id, user_id) VALUES (@dataset_id, @user_id);`
	_, err := db.Pool.Exec(ctx, sqlStatement, dto)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgerrcode.UniqueViolation {
				return domain.ErrResourceAlreadyExists
			}
		}
		log.WithContext(ctx).WithError(err).Error("database error. Failed to add dataset")
		return fmt.Errorf("database error. Failed to add dataset: %w", err)
	}

	return nil
}

// BlacklistDataset sets a dataset as blacklisted if it exists
func (db *DatasetDB) BlacklistDataset(ctx context.Context, datasetID, userID uuid.UUID) error {
	log.Trace("DatasetDB BlacklistDataset")

	dto := ToDAO(datasetID, userID)
	var sqlStatement = `UPDATE ` + db.Name + `.datasets SET blacklisted = true WHERE dataset_id = @dataset_id AND user_id = @user_id;`
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

	dto := ToDAO(datasetID, userID)

	var sqlStatement = `DELETE FROM ` + db.Name + `.datasets WHERE dataset_id = @dataset_id AND user_id = @user_id;`
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

// IsValid checks if a dataset exists in the database and is not published or deleted
func (db *DatasetDB) IsValid(ctx context.Context, datasetID, userID uuid.UUID) (bool, error) {
	log.Trace("DatasetDB IsValid")

	dto := ToDAO(datasetID, userID)

	var sqlStatement = `SELECT EXISTS(SELECT 1 FROM ` + db.Name + `.datasets WHERE dataset_id = @dataset_id AND user_id = @user_id AND blacklisted = false);`

	var exists bool
	err := db.Pool.QueryRow(ctx, sqlStatement, dto).Scan(&exists)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to check if dataset exists")
		return false, fmt.Errorf("database error. Failed to check if dataset exists: %w", err)
	}

	return exists, nil
}

func ToDAO(datasetID, userID uuid.UUID) pgx.NamedArgs {
	return pgx.NamedArgs{
		"dataset_id": pgtype.UUID{Bytes: datasetID, Valid: true},
		"user_id":    pgtype.UUID{Bytes: userID, Valid: true},
	}
}
