package adapter

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type GoogleCloudStorageConnCfgDTO struct {
	ProjectID         string `json:"projectId"           validate:"required,min=6,max=30"`
	AuthMode          string `json:"authMode"            validate:"required"`
	RootPath          string `json:"rootPath"            validate:"required,max=1024"`
	PrivateKeyId      string `json:"privateKeyId"        validate:"required,min=40,max=40"`
	PrivateKey        string `json:"privateKey"          validate:"required"`
	ClientEmail       string `json:"clientEmail"         validate:"required"`
	ClientId          string `json:"clientId"            validate:"required"`
	DefaultCtasFormat string `json:"defaultCtasFormat,omitempty"`
}

func ToGoogleCloudStorageConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToGoogleCloudStorageConnCfgDTO")

	connCfg, ok := conn.(*model.GoogleCloudStorageConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to Google Cloud Storage connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not Google Cloud Storage")
	}

	dto := &GoogleCloudStorageConnCfgDTO{
		ProjectID:         connCfg.ProjectID,
		AuthMode:          connCfg.AuthMode.String(),
		RootPath:          connCfg.RootPath,
		PrivateKeyId:      connCfg.PrivateKeyId,
		PrivateKey:        secretizeParam(connCfg.PrivateKey, secretizer),
		ClientEmail:       connCfg.ClientEmail,
		ClientId:          connCfg.ClientId,
		DefaultCtasFormat: connCfg.DefaultCtasFormat.String(),
	}

	bytes, err := serializer(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to serialize Google Cloud Storage source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromGoogleCloudStorageConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromGoogleCloudStorageConnCfgDTO")

	var dto GoogleCloudStorageConnCfgDTO
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize Google Cloud Storage source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := validator.New().Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate Google Cloud Storage source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	authMode, err := model.ToAuthMode(dto.AuthMode)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert Google Cloud Storage source connector configuration authMode")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	googleCloudStorageConnCfg := &model.GoogleCloudStorageConnCfg{
		ProjectID:    dto.ProjectID,
		AuthMode:     authMode,
		RootPath:     dto.RootPath,
		PrivateKeyId: dto.PrivateKeyId,
		PrivateKey:   dto.PrivateKey,
		ClientEmail:  dto.ClientEmail,
		ClientId:     dto.ClientId,
	}

	if len(dto.DefaultCtasFormat) > 0 {
		ctasFormat, err := model.ToCtasFormat(dto.DefaultCtasFormat)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("failed to convert Google Cloud Storage source connector configuration ctasFormat")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}

		googleCloudStorageConnCfg.DefaultCtasFormat = ctasFormat
	}

	return googleCloudStorageConnCfg, nil
}
