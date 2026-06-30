package adapter

import (
	"context"
	serializers "data_registry_service/pkg/common/serializer"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

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
		return ToAwsS3ConnCfgDTO
	case model.AzureStorage:
		return ToAzureStorageConnCfgDTO
	case model.GoogleCloudStorage:
		return ToGoogleCloudStorageConnCfgDTO
	case model.MongoDB:
		return ToMongoDBConnCfgDTO
	case model.MySQL:
		return ToMySqlDBConnCfgDTO
	case model.ClickHouse:
		return ToClickHouseConnCfgDTO
	case model.Oracle:
		return ToOracleDBConnCfgDTO
	case model.Postgres:
		return ToPostgresDBConnCfgDTO
	default:
		log.WithContext(ctx).Error("unknown source connector configuration type")
		return nil
	}
}

func GetConnCfgFromDTOFunc(ctx context.Context, storageType model.StorageType) (FromDTOFunc, error) {
	log.Trace("adapter GetConnCfgFromDTOFunc")

	switch storageType {
	case model.S3:
		return FromAwsS3ConnCfgDTO, nil
	case model.AzureStorage:
		return FromAzureStorageConnCfgDTO, nil
	case model.GoogleCloudStorage:
		return FromGoogleCloudStorageConnCfgDTO, nil
	case model.MongoDB:
		return FromMongoDBConnCfgDTO, nil
	case model.MySQL:
		return FromMySqlDBConnCfgDTO, nil
	case model.ClickHouse:
		return FromClickHouseConnCfgDTO, nil
	case model.Oracle:
		return FromOracleDBConnCfgDTO, nil
	case model.Postgres:
		return FromPostgresDBConnCfgDTO, nil
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
