package adapter

import (
	"context"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

const defaultTopK = 5

type GenerationDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (model.GenerateRequest, error)
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

type RetrievedContextDTO struct {
	DatasetID   string  `json:"dataset_id"`
	ChunkIndex  int     `json:"chunk_index"`
	SourceText  string  `json:"source_text"`
	Similarity  float64 `json:"similarity"`
	RerankScore float64 `json:"rerank_score,omitempty"`
}

type GenerateResponseDTO struct {
	RequestID          string                `json:"request_id"`
	AgentRunID         string                `json:"agent_run_id,omitempty"`
	DatasetID          string                `json:"dataset_id"`
	DatasetIDs         []string              `json:"dataset_ids"`
	QueryText          string                `json:"query_text"`
	Answer             string                `json:"answer"`
	GenerationProtocol string                `json:"generation_protocol"`
	GenerationModel    string                `json:"generation_model"`
	RAGMergeStrategy   string                `json:"rag_merge_strategy"`
	Contexts           []RetrievedContextDTO `json:"contexts"`
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

func (a *generationDTOAdapter) ToDTO(ctx context.Context, response *model.GenerateResponse) ([]byte, error) {
	log.Trace("GenerationDTOAdapter ToDTO")

	contexts := make([]RetrievedContextDTO, 0, len(response.Contexts))
	for _, item := range response.Contexts {
		contexts = append(contexts, RetrievedContextDTO{
			DatasetID:   item.DatasetID.String(),
			ChunkIndex:  item.ChunkIndex,
			SourceText:  item.SourceText,
			Similarity:  item.Similarity,
			RerankScore: item.RerankScore,
		})
	}
	encoded, err := a.encoder.EncodeDataToString(GenerateResponseDTO{
		RequestID:          response.RequestID.String(),
		AgentRunID:         optionalUUIDString(response.AgentRunID),
		DatasetID:          response.DatasetID.String(),
		DatasetIDs:         uuidStrings(response.DatasetIDs),
		QueryText:          response.QueryText,
		Answer:             response.Answer,
		GenerationProtocol: response.GenerationProtocol,
		GenerationModel:    response.GenerationModel,
		RAGMergeStrategy:   response.RAGMergeStrategy.String(),
		Contexts:           contexts,
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("GenerateResponseDTO encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}
