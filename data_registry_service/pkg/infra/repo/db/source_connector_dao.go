package db

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type SourceConnectorDAO struct {
	ID          pgtype.UUID
	UserID      pgtype.UUID
	OrgID       pgtype.UUID
	CatalogID   pgtype.UUID
	StorageType pgtype.Text
	Config      []byte
}

func toSourceConnDAO(ctx context.Context, sourceConnector *model.SourceConnector, idempotencyKey uuid.UUID) (pgx.NamedArgs, error) {
	log.Trace("SourceConnectorDAO toSourceConnDAO")

	serializedCfg, err := json.Marshal(sourceConnector.Config)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to serialize config")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to serialize source connector config")
	}

	dao := pgx.NamedArgs{
		"id":           pgtype.UUID{Bytes: sourceConnector.ID, Valid: true},
		"user_id":      pgtype.UUID{Bytes: sourceConnector.UserID, Valid: true},
		"org_id":       pgtype.UUID{Bytes: sourceConnector.OrgID, Valid: sourceConnector.OrgID != uuid.Nil},
		"catalog_id":   pgtype.UUID{Bytes: sourceConnector.CatalogID, Valid: true},
		"storage_type": pgtype.Text{String: sourceConnector.Config.GetStorageType().String(), Valid: true},
		"config":       serializedCfg,
	}

	if idempotencyKey != uuid.Nil {
		dao["idempotency_key"] = pgtype.UUID{Bytes: idempotencyKey, Valid: true}
	}

	return dao, nil
}

func fromSourceConnDAO(ctx context.Context, sourceConnector *model.SourceConnector, dao SourceConnectorDAO) error {
	log.Trace("SourceConnectorDAO fromSourceConnDAO")

	storageType, err := model.ToStorageType(dao.StorageType.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert storage type")
		return domainErrors.ErrValidationFailed.Extend("invalid storage type")
	}

	sourceConnector.ID = dao.ID.Bytes
	sourceConnector.UserID = dao.UserID.Bytes
	sourceConnector.OrgID = dao.OrgID.Bytes
	// CatalogID is only needed for catalog integration operations.
	if dao.CatalogID.Valid {
		sourceConnector.CatalogID = dao.CatalogID.Bytes
	}

	// must unmarshal the actual storage type, can't use generics because
	// the attributes of generic struct have default types (e.g. all numbers are float64)
	switch storageType {
	case model.S3:
		s3cfg := model.AwsS3StorageConnCfg{}
		err = json.Unmarshal(dao.Config, &s3cfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal S3 connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal S3 connector config")
		}
		sourceConnector.Config = &s3cfg
	case model.AzureStorage:
		azureCfg := model.AzureStorageConnCfg{}
		err = json.Unmarshal(dao.Config, &azureCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal Azure connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal Azure connector config")
		}
		sourceConnector.Config = &azureCfg
	case model.GoogleCloudStorage:
		gcsCfg := model.GoogleCloudStorageConnCfg{}
		err = json.Unmarshal(dao.Config, &gcsCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal Google Cloud Storage connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal Google Cloud Storage connector config")
		}
		sourceConnector.Config = &gcsCfg
	case model.Postgres:
		postgresCfg := model.PostgresDBConnCfg{}
		err = json.Unmarshal(dao.Config, &postgresCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal Postgres connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal Postgres connector config")
		}
		sourceConnector.Config = &postgresCfg
	case model.MySQL:
		mysqlCfg := model.MysqlDBConnCfg{}
		err = json.Unmarshal(dao.Config, &mysqlCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal MySQL connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal MySQL connector config")
		}
		sourceConnector.Config = &mysqlCfg
	case model.Oracle:
		oracleCfg := model.OracleDBConnCfg{}
		err = json.Unmarshal(dao.Config, &oracleCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal Oracle connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal Oracle connector config")
		}
		sourceConnector.Config = &oracleCfg
	case model.MongoDB:
		mongoCfg := model.MongoDBConnCfg{}
		err = json.Unmarshal(dao.Config, &mongoCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal MongoDB connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal MongoDB connector config")
		}
		sourceConnector.Config = &mongoCfg
	case model.ClickHouse:
		clickHouseCfg := model.ClickHouseConnCfg{}
		err = json.Unmarshal(dao.Config, &clickHouseCfg)
		if err != nil {
			log.WithContext(ctx).WithError(err).Errorf("failed to unmarshal ClickHouse connector config")
			return domainErrors.ErrValidationFailed.Extend("failed to unmarshal ClickHouse connector config")
		}
		sourceConnector.Config = &clickHouseCfg
	default:
		return domainErrors.ErrValidationFailed.Extend("invalid storage type")
	}

	return nil
}
