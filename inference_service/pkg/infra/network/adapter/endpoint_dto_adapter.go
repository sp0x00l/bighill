package adapter

import (
	"context"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type EndpointDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (model.EndpointPublication, error)
	FromDatasetBindingDTO(ctx context.Context, body []byte) (model.EndpointDatasetBinding, error)
	FromMergeConfigurationDTO(ctx context.Context, body []byte) (model.EndpointMergeConfiguration, error)
	ToDTOs(ctx context.Context, endpoints []*model.PublishedEndpoint) ([]byte, error)
	ToDetailDTOs(ctx context.Context, endpoints []*model.PublishedEndpoint) ([]byte, error)
}

type endpointDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type PublishedEndpointDTO struct {
	EndpointID    string `json:"endpoint_id"`
	Mode          string `json:"mode"`
	MergeStrategy string `json:"merge_strategy"`
	DisplayName   string `json:"display_name"`
	Status        string `json:"status"`
}

type PublishedEndpointDetailDTO struct {
	EndpointID      string   `json:"endpoint_id"`
	ModelID         string   `json:"model_id"`
	ServingModelID  string   `json:"serving_model_id,omitempty"`
	Mode            string   `json:"mode"`
	AgentSpecID     string   `json:"agent_spec_id,omitempty"`
	AgentSpecHash   string   `json:"agent_spec_hash,omitempty"`
	DatasetIDs      []string `json:"dataset_ids"`
	MergeStrategy   string   `json:"merge_strategy"`
	DisplayName     string   `json:"display_name"`
	Status          string   `json:"status"`
	CreatedByUserID string   `json:"created_by_user_id"`
}

type EndpointPublicationDTO struct {
	ModelID       string   `json:"model_id"        validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	DatasetIDs    []string `json:"dataset_ids"     validate:"required,min=1,dive,required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	Mode          string   `json:"mode"            validate:"omitempty,oneof=rag agent"`
	AgentSpecHash string   `json:"agent_spec_hash" validate:"omitempty,len=64,hexadecimal"`
	MergeStrategy string   `json:"merge_strategy"  validate:"omitempty,oneof=reranker score_normalized"`
	DisplayName   string   `json:"display_name"    validate:"omitempty,max=200"`
}

type EndpointDatasetBindingDTO struct {
	DatasetIDs []string `json:"dataset_ids" validate:"required,min=1,dive,required,uuid,ne=00000000-0000-0000-0000-000000000000"`
}

type EndpointMergeConfigurationDTO struct {
	MergeStrategy string `json:"merge_strategy" validate:"required,oneof=reranker score_normalized"`
}

func NewEndpointDTOAdapter(encoder *serializers.Encoder) *endpointDTOAdapter {
	log.Trace("NewEndpointDTOAdapter")

	return &endpointDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *endpointDTOAdapter) FromDTO(ctx context.Context, body []byte) (model.EndpointPublication, error) {
	log.Trace("EndpointDTOAdapter FromDTO")

	var dto EndpointPublicationDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.EndpointPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("EndpointPublicationDTO validation failed")
		return model.EndpointPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	strategy, err := optionalRAGMergeStrategy(dto.MergeStrategy)
	if err != nil {
		return model.EndpointPublication{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	modelID, err := parseRequiredUUID(dto.ModelID, "endpoint model_id is invalid")
	if err != nil {
		return model.EndpointPublication{}, err
	}
	datasetIDs, err := parseRequiredUUIDs(dto.DatasetIDs, "endpoint dataset_id is invalid")
	if err != nil {
		return model.EndpointPublication{}, err
	}
	mode := model.AgentEndpointModeRAG
	if strings.TrimSpace(dto.Mode) != "" {
		parsedMode, err := model.ToAgentEndpointMode(dto.Mode)
		if err != nil {
			return model.EndpointPublication{}, domain.ErrValidationFailed.Extend(err.Error())
		}
		mode = parsedMode
	}
	return model.EndpointPublication{
		ModelID:       modelID,
		DatasetIDs:    datasetIDs,
		Mode:          mode,
		AgentSpecHash: strings.TrimSpace(dto.AgentSpecHash),
		MergeStrategy: strategy,
		DisplayName:   dto.DisplayName,
	}, nil
}

func (a *endpointDTOAdapter) FromDatasetBindingDTO(ctx context.Context, body []byte) (model.EndpointDatasetBinding, error) {
	log.Trace("EndpointDTOAdapter FromDatasetBindingDTO")

	var dto EndpointDatasetBindingDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.EndpointDatasetBinding{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("EndpointDatasetBindingDTO validation failed")
		return model.EndpointDatasetBinding{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	datasetIDs, err := parseRequiredUUIDs(dto.DatasetIDs, "endpoint dataset_id is invalid")
	if err != nil {
		return model.EndpointDatasetBinding{}, err
	}
	return model.EndpointDatasetBinding{DatasetIDs: datasetIDs}, nil
}

func (a *endpointDTOAdapter) FromMergeConfigurationDTO(ctx context.Context, body []byte) (model.EndpointMergeConfiguration, error) {
	log.Trace("EndpointDTOAdapter FromMergeConfigurationDTO")

	var dto EndpointMergeConfigurationDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.EndpointMergeConfiguration{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("EndpointMergeConfigurationDTO validation failed")
		return model.EndpointMergeConfiguration{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	strategy, err := model.ToRAGMergeStrategy(dto.MergeStrategy)
	if err != nil {
		return model.EndpointMergeConfiguration{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	return model.EndpointMergeConfiguration{MergeStrategy: strategy}, nil
}

func (a *endpointDTOAdapter) ToDTOs(ctx context.Context, endpoints []*model.PublishedEndpoint) ([]byte, error) {
	log.Trace("EndpointDTOAdapter ToDTOs")

	dtos := make([]PublishedEndpointDTO, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		dtos = append(dtos, PublishedEndpointDTO{
			EndpointID:    endpoint.EndpointID.String(),
			Mode:          endpoint.Mode.String(),
			MergeStrategy: endpoint.MergeStrategy.String(),
			DisplayName:   endpoint.DisplayName,
			Status:        string(endpoint.Status),
		})
	}
	encoded, err := a.encoder.EncodeDataToString(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("PublishedEndpointDTOs encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *endpointDTOAdapter) ToDetailDTOs(ctx context.Context, endpoints []*model.PublishedEndpoint) ([]byte, error) {
	log.Trace("EndpointDTOAdapter ToDetailDTOs")

	dtos := make([]PublishedEndpointDetailDTO, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		dtos = append(dtos, PublishedEndpointDetailDTO{
			EndpointID:      endpoint.EndpointID.String(),
			ModelID:         endpoint.ModelID.String(),
			ServingModelID:  optionalUUIDString(endpoint.ServingModelID),
			Mode:            endpoint.Mode.String(),
			AgentSpecID:     optionalUUIDString(endpoint.AgentSpecID),
			AgentSpecHash:   endpoint.AgentSpecHash,
			DatasetIDs:      uuidStrings(endpoint.DatasetIDs),
			MergeStrategy:   endpoint.MergeStrategy.String(),
			DisplayName:     endpoint.DisplayName,
			Status:          string(endpoint.Status),
			CreatedByUserID: endpoint.CreatedByUserID.String(),
		})
	}
	encoded, err := a.encoder.EncodeDataToString(dtos)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("PublishedEndpointDetailDTOs encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}
