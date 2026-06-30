package adapter

import (
	"context"
	serializers "data_registry_service/pkg/common/serializer"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"fmt"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type AzureStorageConnCfgDTO struct {
	AccountName       string `json:"accountName"               validate:"required,min=3,max=24"`
	AccessKey         string `json:"accessKey,omitempty"       validate:"omitempty,max=20000"` // https://learn.microsoft.com/en-us/purview/sit-defn-azure-storage-account-access-key
	ClientSecret      string `json:"clientSecret,omitempty"    validate:"omitempty,max=40"`
	RootPath          string `json:"rootPath,omitempty"        validate:"omitempty,max=1024"`
	AccountKind       string `json:"accountKind"               validate:"required"`
	DefaultCtasFormat string `json:"defaultCtasFormat,omitempty"`
	CredentialsType   string `json:"credentialsType"           validate:"required"`
	ClientID          string `json:"clientId,omitempty"`
	TokenEndpoint     string `json:"tokenEndpoint,omitempty"`
}

func ToAzureStorageConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToAzureStorageConnCfgDTO")

	connCfg, ok := conn.(*model.AzureStorageConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to Azure Storage connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not Azure Storage")
	}

	dto := AzureStorageConnCfgDTO{
		AccountName:       connCfg.AccountName,
		AccountKind:       connCfg.AccountKind.String(),
		CredentialsType:   connCfg.CredentialsType.String(),
		RootPath:          connCfg.RootPath,
		DefaultCtasFormat: connCfg.DefaultCtasFormat.String(),
	}

	if connCfg.CredentialsType == model.AccessKey {
		dto.AccessKey = secretizeParam(connCfg.AccessKey, secretizer)
	} else {
		dto.ClientID = connCfg.ClientID
		dto.TokenEndpoint = connCfg.TokenEndpoint
		dto.ClientSecret = secretizeParam(connCfg.ClientSecret, secretizer)
	}

	bytes, err := serializer(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to serialize Azure Storage source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromAzureStorageConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromAzureStorageConnCfgDTO")

	var dto AzureStorageConnCfgDTO
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize Azure Storage source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := validator.New().Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate Azure Storage source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	accountKind, err := model.ToAzureVersion(dto.AccountKind)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert Azure Storage source connector configuration accountKind")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	credentialsType, err := model.ToCredentialsType(dto.CredentialsType)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert Azure Storage source connector configuration credentialsType")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	azureStorageConnCfg := &model.AzureStorageConnCfg{
		AccountName:     dto.AccountName,
		AccountKind:     accountKind,
		CredentialsType: credentialsType,
		RootPath:        dto.RootPath,
	}

	if len(dto.DefaultCtasFormat) > 0 {
		ctasFormat, err := model.ToCtasFormat(dto.DefaultCtasFormat)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("failed to convert Azure Storage source connector configuration ctasFormat")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}

		azureStorageConnCfg.DefaultCtasFormat = ctasFormat
	}

	if credentialsType == model.AccessKey {
		if dto.AccessKey == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("accessKey is required")).Error("failed to validate Azure Storage source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("AccessKey is required for Azure Storage access key credentials type")
		}

		azureStorageConnCfg.AccessKey = dto.AccessKey
	} else {
		if dto.ClientID == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("clientID is required")).Error("failed to validate Azure Storage source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("ClientID is required for Azure Storage active directory credentials type")
		}

		if dto.ClientSecret == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("clientSecret is required")).Error("failed to validate Azure Storage source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("ClientSecret is required for Azure Storage active directory credentials type")
		}

		if dto.TokenEndpoint == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("tokenEndpoint is required")).Error("failed to validate Azure Storage source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("TokenEndpoint is required for Azure Storage active directory credentials type")
		}

		azureStorageConnCfg.ClientID = dto.ClientID
		azureStorageConnCfg.ClientSecret = dto.ClientSecret
		azureStorageConnCfg.TokenEndpoint = dto.TokenEndpoint
	}

	return azureStorageConnCfg, nil
}
