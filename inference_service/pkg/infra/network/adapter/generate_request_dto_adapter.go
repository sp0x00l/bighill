package adapter

import (
	"context"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const defaultTopK = 5

type GenerationDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (model.GenerateRequest, error)
	FromAgentEvalRunDTO(ctx context.Context, body []byte) (model.GenerateRequest, error)
	ToDTO(ctx context.Context, response *model.GenerateResponse) ([]byte, error)
}

type generationDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type GenerateRequestDTO struct {
	QueryText       string            `json:"query_text"       validate:"required,max=4000"`
	TopK            *int              `json:"top_k"            validate:"omitempty,min=1,max=100"`
	MetadataFilters map[string]string `json:"metadata_filters" validate:"omitempty,dive,keys,max=128,endkeys,max=512"`
}

type AgentEvalRunRequestDTO struct {
	QueryText       string            `json:"query_text"        validate:"required,max=4000"`
	TopK            *int              `json:"top_k"             validate:"omitempty,min=1,max=100"`
	MetadataFilters map[string]string `json:"metadata_filters"  validate:"omitempty,dive,keys,max=128,endkeys,max=512"`
	ServingModelID  string            `json:"serving_model_id"  validate:"omitempty,uuid,ne=00000000-0000-0000-0000-000000000000"`
}

func NewGenerationDTOAdapter(encoder *serializers.Encoder) *generationDTOAdapter {
	log.Trace("NewGenerationDTOAdapter")

	return &generationDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *generationDTOAdapter) FromDTO(ctx context.Context, body []byte) (model.GenerateRequest, error) {
	log.Trace("GenerationDTOAdapter FromDTO")

	var dto GenerateRequestDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.GenerateRequest{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("GenerateRequestDTO validation failed")
		return model.GenerateRequest{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	topK := dto.TopK
	if topK == nil {
		topK = ptr(defaultTopK)
	}
	return model.GenerateRequest{
		QueryText:       dto.QueryText,
		TopK:            *topK,
		MetadataFilters: dto.MetadataFilters,
	}, nil
}

func (a *generationDTOAdapter) FromAgentEvalRunDTO(ctx context.Context, body []byte) (model.GenerateRequest, error) {
	log.Trace("GenerationDTOAdapter FromAgentEvalRunDTO")

	var dto AgentEvalRunRequestDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return model.GenerateRequest{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("AgentEvalRunRequestDTO validation failed")
		return model.GenerateRequest{}, domain.ErrValidationFailed.Extend(err.Error())
	}
	topK := dto.TopK
	if topK == nil {
		topK = ptr(defaultTopK)
	}
	servingModelID := uuid.Nil
	if dto.ServingModelID != "" {
		parsed, err := uuid.Parse(dto.ServingModelID)
		if err != nil {
			return model.GenerateRequest{}, domain.ErrValidationFailed.Extend("serving_model_id is invalid")
		}
		servingModelID = parsed
	}
	return model.GenerateRequest{
		QueryText:       dto.QueryText,
		TopK:            *topK,
		MetadataFilters: dto.MetadataFilters,
		ServingModelID:  servingModelID,
	}, nil
}
