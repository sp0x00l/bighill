package adapter

import (
	"context"

	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

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
	Status             string                `json:"status,omitempty"`
	AgentRunHref       string                `json:"agent_run_href,omitempty"`
	DatasetID          string                `json:"dataset_id"`
	DatasetIDs         []string              `json:"dataset_ids"`
	QueryText          string                `json:"query_text"`
	Answer             string                `json:"answer"`
	GenerationProtocol string                `json:"generation_protocol"`
	GenerationModel    string                `json:"generation_model"`
	RAGMergeStrategy   string                `json:"rag_merge_strategy"`
	Contexts           []RetrievedContextDTO `json:"contexts"`
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
		Status:             generateResponseStatus(response),
		AgentRunHref:       agentRunHref(response.AgentRunID),
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

func generateResponseStatus(response *model.GenerateResponse) string {
	log.Trace("generateResponseStatus")

	if response != nil && response.Accepted {
		return "RUNNING"
	}
	return ""
}

func agentRunHref(runID uuid.UUID) string {
	log.Trace("agentRunHref")

	if runID == uuid.Nil {
		return ""
	}
	return "/v1/inference/agent-runs/" + runID.String()
}
