package adapter

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"fmt"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type MongoDBHostListDTO struct {
	Hostname string `json:"hostname"    validate:"required"`
	Port     int    `json:"port"        validate:"required"`
}

type MongoDBConnCfgDTO struct {
	HostList           []MongoDBHostListDTO `json:"hostList"              validate:"required,min=1,dive"`
	AuthenticationType string               `json:"authenticationType"    validate:"required"`
	Username           string               `json:"username,omitempty"`
	Password           string               `json:"password,omitempty"`
	AuthDatabase       string               `json:"authDatabase"          validate:"required"`
}

type mongoDBConnCfgDTOAdapter struct {
	validator *validator.Validate
}

func NewMongoDBConnCfgDTOAdapter() *mongoDBConnCfgDTOAdapter {
	log.Trace("NewMongoDBConnCfgDTOAdapter")

	return &mongoDBConnCfgDTOAdapter{validator: validator.New()}
}

func ToMongoDBConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	return NewMongoDBConnCfgDTOAdapter().ToDTO(ctx, conn, secretizer, serializer)
}

func (a *mongoDBConnCfgDTOAdapter) ToDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToMongoDBConnCfgDTO")

	connCfg, ok := conn.(*model.MongoDBConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to Mongo DB connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not Mongo DB")
	}

	dto := &MongoDBConnCfgDTO{
		AuthDatabase:       connCfg.AuthDatabase,
		AuthenticationType: connCfg.AuthenticationType.String(),
		HostList:           make([]MongoDBHostListDTO, len(connCfg.HostList)),
	}

	for i, host := range connCfg.HostList {
		dto.HostList[i] = MongoDBHostListDTO{
			Hostname: host.Hostname,
			Port:     host.Port,
		}
	}

	if connCfg.AuthenticationType == model.Master {
		dto.Username = connCfg.Username
		dto.Password = secretizeParam(connCfg.Password, secretizer)
	}

	bytes, err := serializer(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to serialize Mongo DB source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromMongoDBConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	return NewMongoDBConnCfgDTOAdapter().FromDTO(ctx, cfgBytes, deserializer)
}

func (a *mongoDBConnCfgDTOAdapter) FromDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromMongoDBConnCfgDTO")

	var dto MongoDBConnCfgDTO
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize Mongo DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to validate Mongo DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	authType, err := model.ToAuthenticationType(dto.AuthenticationType)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert Mongo DB source connector authenticationType")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	mongoDBConnCfg := &model.MongoDBConnCfg{
		AuthDatabase:       dto.AuthDatabase,
		HostList:           make([]model.Host, len(dto.HostList)),
		AuthenticationType: authType,
	}

	for i, host := range dto.HostList {
		mongoDBConnCfg.HostList[i] = model.Host{
			Hostname: host.Hostname,
			Port:     host.Port,
		}
	}

	if authType == model.Master {
		if dto.Username == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("username is required")).Error("failed to validate Mongo DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires an username")
		}

		if dto.Password == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("password is required")).Error("failed to validate Mongo DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires a password")
		}
		mongoDBConnCfg.Username = dto.Username
		mongoDBConnCfg.Password = dto.Password
	}

	return mongoDBConnCfg, nil
}
