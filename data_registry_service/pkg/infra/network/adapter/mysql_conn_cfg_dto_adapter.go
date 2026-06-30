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

type MysqlDBConnCfg struct {
	Hostname           string `json:"hostname"                validate:"required"`
	Port               int    `json:"port"                    validate:"required"`
	DatabaseName       string `json:"databaseName"            validate:"required"`
	AuthenticationType string `json:"authenticationType"      validate:"required"`
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
}

func ToMySqlDBConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToMySqlDBConnCfgDTO")

	connCfg, ok := conn.(*model.MysqlDBConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to MySQL DB connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not MySQL DB")
	}

	dto := &MysqlDBConnCfg{
		Hostname:           connCfg.Hostname,
		Port:               connCfg.Port,
		DatabaseName:       connCfg.DatabaseName,
		AuthenticationType: connCfg.AuthenticationType.String(),
	}

	if connCfg.AuthenticationType == model.Master {
		dto.Username = connCfg.Username
		dto.Password = secretizeParam(connCfg.Password, secretizer)
	}

	bytes, err := serializer(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to serialize MySQL DB source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromMySqlDBConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromMySqlDBConnCfgDTO")

	var dto MysqlDBConnCfg
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize MySQL DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := validator.New().Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate MySQL DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	authType, err := model.ToAuthenticationType(dto.AuthenticationType)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert MySQL DB source connector authenticationType")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	mySqlDBConnCfg := &model.MysqlDBConnCfg{
		Hostname:           dto.Hostname,
		Port:               dto.Port,
		DatabaseName:       dto.DatabaseName,
		AuthenticationType: authType,
	}
	if authType == model.Master {
		if dto.Username == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("username is required")).Error("failed to validate MySQL DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires username")
		}

		if dto.Password == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("password is required")).Error("failed to validate MySQL DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires password")
		}

		mySqlDBConnCfg.Password = dto.Password
		mySqlDBConnCfg.Username = dto.Username
	}

	return mySqlDBConnCfg, nil
}
