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

type GenerateRequestDTO struct {
	QueryText       string            `json:"query_text"       validate:"required,max=4000"`
	TopK            *int              `json:"top_k"            validate:"omitempty,min=1,max=100"`
	MetadataFilters map[string]string `json:"metadata_filters" validate:"omitempty,dive,keys,max=128,endkeys,max=512"`
}

type RetrievedContextDTO struct {
	ChunkIndex  int     `json:"chunk_index"`
	SourceText  string  `json:"source_text"`
	Similarity  float64 `json:"similarity"`
	RerankScore float64 `json:"rerank_score,omitempty"`
}

type GenerateResponseDTO struct {
	RequestID          string                `json:"request_id"`
	QueryText          string                `json:"query_text"`
	Answer             string                `json:"answer"`
	GenerationProtocol string                `json:"generation_protocol"`
	GenerationModel    string                `json:"generation_model"`
	Contexts           []RetrievedContextDTO `json:"contexts"`
}

type FeedbackRequestDTO struct {
	RequestID       string `json:"request_id"       validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	Accepted        bool   `json:"accepted"`
	Rating          int    `json:"rating"           validate:"min=-1,max=1"`
	Comment         string `json:"comment"          validate:"max=2000"`
	PreferredAnswer string `json:"preferred_answer" validate:"max=8000"`
}

type FeedbackResponseDTO struct {
	FeedbackID string `json:"feedback_id"`
	RequestID  string `json:"request_id"`
}

type PublishedEndpointDTO struct {
	EndpointID  string `json:"endpoint_id"`
	DisplayName string `json:"display_name"`
	Status      string `json:"status"`
}

type InferenceDTOAdapter interface {
	FromGenerateDTO(ctx context.Context, body []byte) (model.GenerateRequest, error)
	ToGenerateDTO(ctx context.Context, response *model.GenerateResponse) ([]byte, error)
	FromFeedbackDTO(ctx context.Context, body []byte) (*model.InferenceFeedback, error)
	ToFeedbackDTO(ctx context.Context, feedback *model.InferenceFeedback) ([]byte, error)
	ToEndpointDTOs(ctx context.Context, endpoints []*model.PublishedEndpoint) ([]byte, error)
}

type inferenceDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

func NewInferenceDTOAdapter(encoder *serializers.Encoder) *inferenceDTOAdapter {
	log.Trace("NewInferenceDTOAdapter")

	return &inferenceDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *inferenceDTOAdapter) FromGenerateDTO(ctx context.Context, body []byte) (model.GenerateRequest, error) {
	log.Trace("InferenceDTOAdapter FromGenerateDTO")

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

func (a *inferenceDTOAdapter) ToGenerateDTO(ctx context.Context, response *model.GenerateResponse) ([]byte, error) {
	log.Trace("InferenceDTOAdapter ToGenerateDTO")

	contexts := make([]RetrievedContextDTO, 0, len(response.Contexts))
	for _, item := range response.Contexts {
		contexts = append(contexts, RetrievedContextDTO{
			ChunkIndex:  item.ChunkIndex,
			SourceText:  item.SourceText,
			Similarity:  item.Similarity,
			RerankScore: item.RerankScore,
		})
	}
	encoded, err := a.encoder.EncodeDataToString(GenerateResponseDTO{
		RequestID:          response.RequestID.String(),
		QueryText:          response.QueryText,
		Answer:             response.Answer,
		GenerationProtocol: response.GenerationProtocol,
		GenerationModel:    response.GenerationModel,
		Contexts:           contexts,
	})
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *inferenceDTOAdapter) FromFeedbackDTO(ctx context.Context, body []byte) (*model.InferenceFeedback, error) {
	log.Trace("InferenceDTOAdapter FromFeedbackDTO")

	var dto FeedbackRequestDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return nil, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("FeedbackRequestDTO validation failed")
		return nil, domain.ErrValidationFailed.Extend(err.Error())
	}
	return &model.InferenceFeedback{
		RequestID:       mustParseUUID(dto.RequestID),
		Accepted:        dto.Accepted,
		Rating:          dto.Rating,
		Comment:         dto.Comment,
		PreferredAnswer: dto.PreferredAnswer,
	}, nil
}

func (a *inferenceDTOAdapter) ToFeedbackDTO(ctx context.Context, feedback *model.InferenceFeedback) ([]byte, error) {
	log.Trace("InferenceDTOAdapter ToFeedbackDTO")

	encoded, err := a.encoder.EncodeDataToString(FeedbackResponseDTO{
		FeedbackID: feedback.FeedbackID.String(),
		RequestID:  feedback.RequestID.String(),
	})
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *inferenceDTOAdapter) ToEndpointDTOs(ctx context.Context, endpoints []*model.PublishedEndpoint) ([]byte, error) {
	log.Trace("InferenceDTOAdapter ToEndpointDTOs")

	dtos := make([]PublishedEndpointDTO, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		dtos = append(dtos, PublishedEndpointDTO{
			EndpointID:  endpoint.EndpointID.String(),
			DisplayName: endpoint.DisplayName,
			Status:      string(endpoint.Status),
		})
	}
	encoded, err := a.encoder.EncodeDataToString(dtos)
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}

func mustParseUUID(value string) uuid.UUID {
	log.Trace("mustParseUUID")

	return uuid.MustParse(value)
}

func ptr(value int) *int {
	log.Trace("ptr")

	return &value
}
