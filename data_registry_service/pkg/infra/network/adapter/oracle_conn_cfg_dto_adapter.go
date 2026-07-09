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

type OracleDBConnCfgDTO struct {
	Hostname           string `json:"hostname"                validate:"required"`
	Port               int    `json:"port"                    validate:"required"`
	Instance           string `json:"instance"                validate:"required"`
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
	SecretResourceUrl  string `json:"secretResourceUrl,omitempty"`
	AuthenticationType string `json:"authenticationType"      validate:"required"`
}

type oracleDBConnCfgDTOAdapter struct {
	validator *validator.Validate
}

func NewOracleDBConnCfgDTOAdapter() *oracleDBConnCfgDTOAdapter {
	log.Trace("NewOracleDBConnCfgDTOAdapter")

	return &oracleDBConnCfgDTOAdapter{validator: validator.New()}
}

func ToOracleDBConnCfgDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	return NewOracleDBConnCfgDTOAdapter().ToDTO(ctx, conn, secretizer, serializer)
}

func (a *oracleDBConnCfgDTOAdapter) ToDTO(ctx context.Context, conn model.ConnectorConfig, secretizer SecretizeFunc, serializer serializers.SerializeFunc) ([]byte, error) {
	log.Trace("adapter ToOracleDBConnCfgDTO")

	connCfg, ok := conn.(*model.OracleDBConnCfg)
	if !ok {
		log.WithContext(ctx).Error("failed to cast connector configuration to Oracle DB connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend("connector configuration is not Oracle DB")
	}

	dto := &OracleDBConnCfgDTO{
		Hostname:           connCfg.Hostname,
		Port:               connCfg.Port,
		Instance:           connCfg.Instance,
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
		log.WithContext(ctx).WithError(err).Error("failed to serialize Oracle DB source connector configuration")
		return nil, err
	}

	return bytes, nil
}

func FromOracleDBConnCfgDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	return NewOracleDBConnCfgDTOAdapter().FromDTO(ctx, cfgBytes, deserializer)
}

func (a *oracleDBConnCfgDTOAdapter) FromDTO(ctx context.Context, cfgBytes []byte, deserializer serializers.DeserializeFunc) (model.ConnectorConfig, error) {
	log.Trace("adapter FromOracleDBConnCfgDTO")

	var dto OracleDBConnCfgDTO
	if err := deserializer(cfgBytes, &dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to deserialize Oracle DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate Oracle DB source connector configuration")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	authType, err := model.ToAuthenticationType(dto.AuthenticationType)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to convert Oracle DB source connector authenticationType")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	oracleDBConnCfg := &model.OracleDBConnCfg{
		Hostname:           dto.Hostname,
		Port:               dto.Port,
		Instance:           dto.Instance,
		AuthenticationType: authType,
	}
	if authType == model.Master {
		if dto.Username == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("username is required")).Error("failed to validate Oracle DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires username")
		}
		oracleDBConnCfg.Username = dto.Username

		if dto.Password == "" && dto.SecretResourceUrl == "" {
			log.WithContext(ctx).WithError(fmt.Errorf("password or secretResourceUrl is required")).Error("failed to validate Oracle DB source connector configuration")
			return nil, domainErrors.ErrValidationFailed.Extend("Master authentication requires password or secretResourceUrl")
		}
		if dto.Password != "" {
			oracleDBConnCfg.Password = dto.Password
		} else {
			oracleDBConnCfg.SecretResourceUrl = dto.SecretResourceUrl
		}
	}

	return oracleDBConnCfg, nil
}
