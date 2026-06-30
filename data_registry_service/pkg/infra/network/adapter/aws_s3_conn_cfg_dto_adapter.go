package adapter

import (
	"context"
	serializers "data_registry_service/pkg/common/serializer"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type AwsS3StorageConnCfgDTO struct {
	AccessKey          string   `json:"accessKey"                validate:"required,max=20"`
	AccessSecret       string   `json:"accessSecret"             validate:"required,max=40"`
	AssumedRoleARN     string   `json:"assumedRoleARN,omitempty" validate:"omitempty,min=20,max=2048"`
	RootPath           string   `json:"rootPath,omitempty"       validate:"omitempty,max=1024"`
	Secure             *bool    `json:"secure,omitempty"`
	DefaultCtasFormat  string   `json:"defaultCtasFormat,omitempty"`
	WhitelistedBuckets []string `json:"whitelistedBuckets,omitempty"`
}

func ToAwsS3ConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToAwsS3ConnCfgDTO")

	connCfg, ok := conn.(*model.AwsS3StorageConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to AWS S3 connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not AWS S3")
	}

	dto := AwsS3StorageConnCfgDTO{
		AccessKey:          connCfg.AccessKey,
		AccessSecret:       secretizeParam(connCfg.AccessSecret, secretizer),
		AssumedRoleARN:     connCfg.AssumedRoleARN,
		RootPath:           connCfg.RootPath,
		DefaultCtasFormat:  connCfg.DefaultCtasFormat.String(),
		WhitelistedBuckets: connCfg.WhitelistedBuckets,
	}

	if connCfg.Secure {
		dto.Secure = &connCfg.Secure
	}

	bytes, err := serializer(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to serialize AWS S3 source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromAwsS3ConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromAwsS3ConnCfgDTO")

	var dto AwsS3StorageConnCfgDTO
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize AWS S3 source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := validator.New().Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate AWS S3 source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	s3Conn := &model.AwsS3StorageConnCfg{
		AccessKey:          dto.AccessKey,
		AccessSecret:       dto.AccessSecret,
		AssumedRoleARN:     dto.AssumedRoleARN,
		RootPath:           dto.RootPath,
		WhitelistedBuckets: dto.WhitelistedBuckets,
	}

	if len(dto.DefaultCtasFormat) > 0 {
		ctasFormat, err := model.ToCtasFormat(dto.DefaultCtasFormat)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("failed to convert AWS S3 source connector configuration ctasFormat")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}

		s3Conn.DefaultCtasFormat = ctasFormat
	}

	if dto.Secure != nil {
		s3Conn.Secure = *dto.Secure
	}
	return s3Conn, nil
}
