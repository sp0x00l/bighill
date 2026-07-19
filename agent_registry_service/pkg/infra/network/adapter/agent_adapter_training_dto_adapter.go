package adapter

import (
	"context"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type AgentAdapterTrainingDTOAdapter interface {
	ToStartAgentAdapterTrainingRunDTO(ctx context.Context, request model.AgentAdapterTrainingRequest) ([]byte, error)
	FromStartAgentAdapterTrainingRunResponseDTO(ctx context.Context, body []byte) (uuid.UUID, error)
}

type agentAdapterTrainingDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type StartAgentAdapterTrainingRunDTO struct {
	DatasetID          string `json:"dataset_id"           validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	DatasetURI         string `json:"dataset_uri"          validate:"required"`
	DatasetContentHash string `json:"dataset_content_hash" validate:"required"`
	SourceModelID      string `json:"source_model_id"      validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	AgentLineage       string `json:"agent_lineage"        validate:"required"`
	TrainingProfile    string `json:"training_profile,omitempty"`
}

type StartAgentAdapterTrainingRunResponseDTO struct {
	TrainingRunID string `json:"training_run_id" validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	StatusURL     string `json:"status_url,omitempty"`
}

func NewAgentAdapterTrainingDTOAdapter(encoder *serializers.Encoder) *agentAdapterTrainingDTOAdapter {
	log.Trace("NewAgentAdapterTrainingDTOAdapter")

	return &agentAdapterTrainingDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *agentAdapterTrainingDTOAdapter) ToStartAgentAdapterTrainingRunDTO(ctx context.Context, request model.AgentAdapterTrainingRequest) ([]byte, error) {
	log.Trace("AgentAdapterTrainingDTOAdapter ToStartAgentAdapterTrainingRunDTO")

	dto := StartAgentAdapterTrainingRunDTO{
		DatasetID:          request.DatasetID.String(),
		DatasetURI:         request.DatasetURI,
		DatasetContentHash: request.ContentHash,
		SourceModelID:      request.SourceModelID.String(),
		AgentLineage:       request.AgentLineage,
		TrainingProfile:    request.TrainingProfile,
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("StartAgentAdapterTrainingRunDTO validation failed")
		return nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	raw, err := a.encoder.Serialize(dto)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("StartAgentAdapterTrainingRunDTO encoding failed")
		return nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	return raw, nil
}

func (a *agentAdapterTrainingDTOAdapter) FromStartAgentAdapterTrainingRunResponseDTO(ctx context.Context, body []byte) (uuid.UUID, error) {
	log.Trace("AgentAdapterTrainingDTOAdapter FromStartAgentAdapterTrainingRunResponseDTO")

	var dto StartAgentAdapterTrainingRunResponseDTO
	if err := a.encoder.Deserialize(body, &dto); err != nil {
		return uuid.Nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("StartAgentAdapterTrainingRunResponseDTO validation failed")
		return uuid.Nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	trainingRunID, err := uuid.Parse(dto.TrainingRunID)
	if err != nil || trainingRunID == uuid.Nil {
		return uuid.Nil, domain.ErrAgentTrainingFailed.Extend("training service returned invalid training_run_id")
	}
	return trainingRunID, nil
}
