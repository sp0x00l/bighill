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

type PostgresDBConnCfgDTO struct {
	Hostname           string `json:"hostname"                validate:"required"`
	Port               int    `json:"port"                    validate:"required"`
	DatabaseName       string `json:"databaseName"            validate:"required"`
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
	SecretResourceUrl  string `json:"secretResourceUrl,omitempty"`
	AuthenticationType string `json:"authenticationType"      validate:"required"`
}

type postgresDBConnCfgDTOAdapter struct {
	validator *validator.Validate
}

func NewPostgresDBConnCfgDTOAdapter() *postgresDBConnCfgDTOAdapter {
	log.Trace("NewPostgresDBConnCfgDTOAdapter")

	return &postgresDBConnCfgDTOAdapter{validator: validator.New()}
}

func ToPostgresDBConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	return NewPostgresDBConnCfgDTOAdapter().ToDTO(ctx, conn, secretizer, serializer)
}

func (a *postgresDBConnCfgDTOAdapter) ToDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToPostgresDBConnCfgDTO")

	connCfg, ok := conn.(*model.PostgresDBConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to Postgres DB connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not Postgres DB")
	}

	dto := &PostgresDBConnCfgDTO{
		Hostname:           connCfg.Hostname,
		Port:               connCfg.Port,
		DatabaseName:       connCfg.DatabaseName,
		AuthenticationType: connCfg.AuthenticationType.String(),
	}

	if connCfg.AuthenticationType == model.Master {
		dto.Username = connCfg.Username
		if connCfg.Password != "" {
			dto.Password = secretizeParam(connCfg.Password, secretizer)
		} else {
			dto.SecretResourceUrl = connCfg.SecretResourceUrl
		}
	}
	bytes, err := serializer(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to serialize Postgres DB source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromPostgresDBConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	return NewPostgresDBConnCfgDTOAdapter().FromDTO(ctx, cfgBytes, deserializer)
}

func (a *postgresDBConnCfgDTOAdapter) FromDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromPostgresDBConnCfgDTO")

	var dto PostgresDBConnCfgDTO
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize Postgres DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate Postgres DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	authType, err := model.ToAuthenticationType(dto.AuthenticationType)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("failed to convert Postgres DB source connector authenticationType")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	postgresDBConnCfg := &model.PostgresDBConnCfg{
		Hostname:           dto.Hostname,
		Port:               dto.Port,
		DatabaseName:       dto.DatabaseName,
		AuthenticationType: authType,
	}

	if authType == model.Master {
		if dto.Username == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("username is required")).Error("failed to validate Postgres DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires username")
		}
		postgresDBConnCfg.Username = dto.Username

		if dto.Password == "" && dto.SecretResourceUrl == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("password or secretResourceUrl is required")).Error("failed to validate Postgres DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires password or secretResourceUrl")
		}
		if dto.Password != "" {
			postgresDBConnCfg.Password = dto.Password
		} else {
			postgresDBConnCfg.SecretResourceUrl = dto.SecretResourceUrl
		}
	}
	return postgresDBConnCfg, nil
}
