package adapter

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	log "github.com/sirupsen/logrus"
)

type (
	SecretizeFunc             func(string) string
	ToDTOFunc                 func(context.Context, model.ConnectorConfig, SecretizeFunc, serializers.SerializeFunc) ([]byte, error)
	FromDTOFunc               func(context.Context, []byte, serializers.DeserializeFunc) (model.ConnectorConfig, error)
	ConnCfgToDTOFactoryFunc   func(context.Context, model.StorageType) ToDTOFunc
	ConnCfgFromDTOFactoryFunc func(context.Context, model.StorageType) (FromDTOFunc, error)
)

func GetConnCfgToDTOFunc(ctx context.Context, storageType model.StorageType) ToDTOFunc {
	log.Trace("adapter GetConnCfgToDTOFunc")

	switch storageType {
	case model.S3:
		return NewAwsS3ConnCfgDTOAdapter().ToDTO
	case model.AzureStorage:
		return NewAzureStorageConnCfgDTOAdapter().ToDTO
	case model.GoogleCloudStorage:
		return NewGoogleCloudStorageConnCfgDTOAdapter().ToDTO
	case model.MongoDB:
		return NewMongoDBConnCfgDTOAdapter().ToDTO
	case model.MySQL:
		return NewMySqlDBConnCfgDTOAdapter().ToDTO
	case model.ClickHouse:
		return NewClickHouseConnCfgDTOAdapter().ToDTO
	case model.Oracle:
		return NewOracleDBConnCfgDTOAdapter().ToDTO
	case model.Postgres:
		return NewPostgresDBConnCfgDTOAdapter().ToDTO
	default:
		log.WithContext(ctx).Error("unknown source connector configuration type")
		return nil
	}
}

func GetConnCfgFromDTOFunc(ctx context.Context, storageType model.StorageType) (FromDTOFunc, error) {
	log.Trace("adapter GetConnCfgFromDTOFunc")

	switch storageType {
	case model.S3:
		return NewAwsS3ConnCfgDTOAdapter().FromDTO, nil
	case model.AzureStorage:
		return NewAzureStorageConnCfgDTOAdapter().FromDTO, nil
	case model.GoogleCloudStorage:
		return NewGoogleCloudStorageConnCfgDTOAdapter().FromDTO, nil
	case model.MongoDB:
		return NewMongoDBConnCfgDTOAdapter().FromDTO, nil
	case model.MySQL:
		return NewMySqlDBConnCfgDTOAdapter().FromDTO, nil
	case model.ClickHouse:
		return NewClickHouseConnCfgDTOAdapter().FromDTO, nil
	case model.Oracle:
		return NewOracleDBConnCfgDTOAdapter().FromDTO, nil
	case model.Postgres:
		return NewPostgresDBConnCfgDTOAdapter().FromDTO, nil
	default:
		log.WithContext(ctx).Error("unknown source connector configuration type")
		return nil, domainErrors.ErrValidationFailed.Extend("unknown source connector configuration type")
	}
}

func secretizeParam(s string, secretizer SecretizeFunc) string {
	log.Trace("adapter secretizeParam")
	if secretizer != nil {
		return secretizer(s)
	}
	return s
}
