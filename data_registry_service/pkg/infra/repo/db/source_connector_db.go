package db

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"errors"
	"fmt"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type sourceConnectorDB struct {
	coreDB.Database
}

func NewSourceConnectorDB(db *coreDB.Database) *sourceConnectorDB {
	log.Trace("NewSourceConnectorDB")

	return &sourceConnectorDB{
		*db,
	}
}

func (db *sourceConnectorDB) Create(ctx context.Context, sourceConnector *model.SourceConnector, idempotencyKey uuid.UUID) error {
	log.Trace("SourceConnectorDB Create")

	sourceConnDAO, err := toSourceConnDAO(ctx, sourceConnector, idempotencyKey)
	if err != nil {
		return err
	}

	var sqlStatement = `INSERT INTO ` + db.Name + `.connectors (id, user_id, catalog_id, storage_type, config, idempotency_key)
	VALUES (@id,  @user_id, @catalog_id, @storage_type, @config, @idempotency_key);`

	_, err = db.Pool.Exec(ctx, sqlStatement, sourceConnDAO)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to insert connector")
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgerrcode.UniqueViolation {
				return domainErrors.ErrResourceAlreadyExists
			}
		}
		if coreDB.IsForeignKeyViolation(err) || coreDB.IsRowLevelSecurityViolation(err) {
			return domainErrors.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return fmt.Errorf("database error. Failed to insert connector: %w", err)
	}

	return nil
}

func (db *sourceConnectorDB) ReadByUserID(ctx context.Context, userID uuid.UUID) ([]model.SourceConnector, error) {
	log.Trace("SourceConnectorDB ReadByUserID")

	sqlStatement := `SELECT id, user_id, storage_type, config FROM ` + db.Name + `.connectors 
	WHERE user_id = @user_id AND deleted = false;`

	rows, err := db.Pool.Query(ctx, sqlStatement, pgx.NamedArgs{"user_id": userID})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to query connector")
		return nil, fmt.Errorf("database error. Failed to query connector: %w", err)
	}

	defer rows.Close()
	connectors, err := db.scanRows(ctx, rows)
	if err != nil {
		return nil, err
	}

	if len(connectors) == 0 {
		log.WithContext(ctx).Warn(fmt.Sprintf("No connectors found in database for userID: %s", userID.String()))
		return nil, domainErrors.ErrResourceNotFound
	}
	return connectors, nil
}

func (db *sourceConnectorDB) ReadByID(ctx context.Context, connectorID, userID uuid.UUID) (*model.SourceConnector, error) {
	log.Trace("SourceConnectorDB ReadByID")

	sqlStatement := `SELECT id, user_id, catalog_id, storage_type, config FROM ` + db.Name + `.connectors 
	WHERE id = @id AND user_id = @user_id AND deleted = false;`

	row := db.Pool.QueryRow(ctx, sqlStatement, pgx.NamedArgs{"id": connectorID, "user_id": userID})
	var sourceConnDAO SourceConnectorDAO
	err := row.Scan(&sourceConnDAO.ID, &sourceConnDAO.UserID, &sourceConnDAO.CatalogID, &sourceConnDAO.StorageType, &sourceConnDAO.Config)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domainErrors.ErrResourceNotFound
		}
		log.WithContext(ctx).WithError(err).Error("database error. Failed to scan connector")
		return nil, fmt.Errorf("database error. Failed to scan connector: %w", err)
	}

	var sourceConnector model.SourceConnector
	err = fromSourceConnDAO(ctx, &sourceConnector, sourceConnDAO)
	return &sourceConnector, err
}

func (db *sourceConnectorDB) ReadCatalogID(ctx context.Context, connectorID, userID uuid.UUID) (uuid.UUID, error) {
	log.Trace("SourceConnectorDB ReadCatalogID")

	sqlStatement := `SELECT catalog_id FROM ` + db.Name + `.connectors 
	WHERE id = @id AND user_id = @user_id AND deleted = false;`

	var daoID pgtype.UUID

	row := db.Pool.QueryRow(ctx, sqlStatement, pgx.NamedArgs{"id": connectorID, "user_id": userID})
	err := row.Scan(&daoID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, domainErrors.ErrResourceNotFound
		}
		log.WithContext(ctx).WithError(err).Error("database error. Failed to scan connector")
		return uuid.Nil, fmt.Errorf("database error. Failed to scan connector: %w", err)
	}

	catalogID := daoID.Bytes

	return catalogID, nil
}

func (db *sourceConnectorDB) Delete(ctx context.Context, connectorID, userID uuid.UUID) error {
	log.Trace("SourceConnectorDB Delete")

	sqlStatement := `UPDATE ` + db.Name + `.connectors SET deleted = true 
	WHERE id = @id AND user_id = @user_id AND deleted = false;`

	commandTag, err := db.Pool.Exec(ctx, sqlStatement, pgx.NamedArgs{"id": connectorID, "user_id": userID})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to delete connector")
		return fmt.Errorf("database error. Failed to delete connector: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		log.WithContext(ctx).Warn(fmt.Sprintf("No connector found in database for id %v and userID %v", connectorID, userID))
		return domainErrors.ErrResourceNotFound
	}

	return nil
}

func (db *sourceConnectorDB) Replace(ctx context.Context, sourceConnector *model.SourceConnector) error {
	log.Trace("SourceConnectorDB Replace")

	sourceConnDAO, err := toSourceConnDAO(ctx, sourceConnector, uuid.Nil)
	if err != nil {
		return err
	}

	sqlStatement := `UPDATE ` + db.Name + `.connectors SET user_id = @user_id, catalog_id = @catalog_id, storage_type = @storage_type, config = @config
	WHERE id = @id AND user_id = @user_id AND deleted = false;`

	commandTag, err := db.Pool.Exec(ctx, sqlStatement, sourceConnDAO)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to update connector")
		return fmt.Errorf("database error. Failed to update connector: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		log.WithContext(ctx).Warn(fmt.Sprintf("No connector found in database for id %v and userID %v", sourceConnector.ID, sourceConnector.UserID))
		return domainErrors.ErrResourceNotFound
	}

	return nil
}

func (db *sourceConnectorDB) scanRows(ctx context.Context, rows pgx.Rows) ([]model.SourceConnector, error) {
	log.Trace("sourceConnectorDB scanRows")

	sourceConnectors := make([]model.SourceConnector, 0)
	for rows.Next() {
		var sourceConnDAO SourceConnectorDAO
		err := rows.Scan(&sourceConnDAO.ID, &sourceConnDAO.UserID, &sourceConnDAO.StorageType, &sourceConnDAO.Config)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("database error. Failed to scan connector")
			return []model.SourceConnector{}, fmt.Errorf("database error. Failed to scan connector: %w", err)
		}

		var sourceConnector model.SourceConnector
		err = fromSourceConnDAO(ctx, &sourceConnector, sourceConnDAO)
		if err != nil {
			return []model.SourceConnector{}, err
		}
		sourceConnectors = append(sourceConnectors, sourceConnector)
	}
	if err := rows.Err(); err != nil {
		log.WithContext(ctx).WithError(err).Error("database error. Failed to iterate connectors")
		return []model.SourceConnector{}, fmt.Errorf("database error. Failed to iterate connectors: %w", err)
	}

	return sourceConnectors, nil
}
