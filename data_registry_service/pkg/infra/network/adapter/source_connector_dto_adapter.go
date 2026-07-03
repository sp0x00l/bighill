package adapter

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"encoding/json"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type RestSourceConnDTO struct {
	ID     string          `json:"id,omitempty"`
	Config json.RawMessage `json:"config"                 validate:"required"`
}

type RestSourceConnDTOAdapter struct {
	cfgToDTOFuncFactory   ConnCfgToDTOFactoryFunc
	cfgFromDTOFuncFactory ConnCfgFromDTOFactoryFunc
	encoder               *serializers.Encoder
	validator             *validator.Validate
}

func NewRestSourceConnDTOAdapter(configToDTOFactory ConnCfgToDTOFactoryFunc, configFromDTOFactory ConnCfgFromDTOFactoryFunc, encoder *serializers.Encoder) *RestSourceConnDTOAdapter {
	log.Trace("NewRestSourceConnDTOAdapter")

	return &RestSourceConnDTOAdapter{
		cfgToDTOFuncFactory:   configToDTOFactory,
		cfgFromDTOFuncFactory: configFromDTOFactory,
		encoder:               encoder,
		validator:             validator.New(),
	}
}

func (a *RestSourceConnDTOAdapter) ToDTO(ctx context.Context, conn *model.SourceConnector) ([]byte, error) {
	log.Trace("RestSourceConnDTOAdapter ToDTO")

	cfgToDTOFunc := a.cfgToDTOFuncFactory(ctx, conn.Config.GetStorageType())
	if cfgToDTOFunc == nil {
		log.WithContext(ctx).Error("source connector config DTO function is not configured")
		return nil, domainErrors.ErrValidationFailed.Extend("unknown source connector configuration type")
	}
	cfgBytes, err := cfgToDTOFunc(ctx, conn.Config, nil, a.encoder.Serialize)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Source connector config encoding failed")
		return nil, err
	}

	dto := &RestSourceConnDTO{
		ID:     conn.ID.String(),
		Config: cfgBytes,
	}

	bytes, err := a.encoder.EncodeDataToString(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Source connector encoding failed")
		return nil, err
	}

	return []byte(bytes), nil
}

func (a *RestSourceConnDTOAdapter) FromDTO(ctx context.Context, storageType model.StorageType, body []byte) (*model.SourceConnector, error) {
	log.Trace("RestSourceConnDTOAdapter FromDTO")

	var dto RestSourceConnDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("Source connector decoding failed")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to validate source connector DTO")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	cfgFromDTOFunc, err := a.cfgFromDTOFuncFactory(ctx, storageType)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to get source connector config from DTO function")
		return nil, err
	}

	cfg, err := cfgFromDTOFunc(ctx, dto.Config, a.encoder.Deserialize)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Source connector config decoding failed")
		return nil, err
	}

	connector := &model.SourceConnector{
		Config: cfg,
	}

	if dto.ID != "" {
		connector.ID, err = uuid.Parse(dto.ID)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("Source connector ID is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	return connector, nil
}
