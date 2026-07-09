package model

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type GenerateRequest struct {
	RequestID       uuid.UUID
	UserID          uuid.UUID
	OrgID           uuid.UUID
	DatasetID       uuid.UUID
	ModelID         uuid.UUID
	QueryText       string
	TopK            int
	MetadataFilters map[string]string
}

type GenerateResponse struct {
	RequestID             uuid.UUID
	OrgID                 uuid.UUID
	DatasetID             uuid.UUID
	DatasetIDs            []uuid.UUID
	ModelID               uuid.UUID
	QueryText             string
	Answer                string
	Contexts              []RetrievedContext
	PromptStrategyVersion string
	GenerationProtocol    string
	GenerationModel       string
	RAGMergeStrategy      RAGMergeStrategy
}

type QueryTransformRequest struct {
	RequestID       uuid.UUID
	UserID          uuid.UUID
	OrgID           uuid.UUID
	DatasetID       uuid.UUID
	ModelID         uuid.UUID
	Model           *InferenceModel
	QueryText       string
	MetadataFilters map[string]string
}

type QueryTransformResult struct {
	QueryText       string
	MetadataFilters map[string]string
}

type RetrievedContext struct {
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	ChunkIndex          int
	SourceText          string
	Distance            float64
	Similarity          float64
	RerankScore         float64
}

type GenerationRequest struct {
	RequestID             uuid.UUID
	Dataset               *InferenceDataset
	Model                 *InferenceModel
	Query                 string
	Prompt                string
	PromptStrategyVersion string
	Contexts              []RetrievedContext
}

type PromptStrategy struct {
	Version          string
	SystemPrompt     string
	MaxContextTokens int
	MaxContextChunks int
}

type ContextPackRequest struct {
	Query    string
	Contexts []RetrievedContext
}

type PromptBuildRequest struct {
	Query    string
	Dataset  *InferenceDataset
	Model    *InferenceModel
	Contexts []RetrievedContext
}

type PromptPackage struct {
	Prompt   string
	Strategy PromptStrategy
	Contexts []RetrievedContext
}

type InferenceRequestStatus int

const (
	InferenceRequestStatusCompleted InferenceRequestStatus = iota
	InferenceRequestStatusFailed
)

func (s InferenceRequestStatus) String() string {
	return [...]string{"COMPLETED", "FAILED"}[s]
}

func ToInferenceRequestStatus(value string) (InferenceRequestStatus, error) {
	switch strings.TrimSpace(value) {
	case "COMPLETED":
		return InferenceRequestStatusCompleted, nil
	case "FAILED":
		return InferenceRequestStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid inference request status %q", value)
	}
}

type InferenceRequest struct {
	RequestID             uuid.UUID
	UserID                uuid.UUID
	OrgID                 uuid.UUID
	DatasetID             uuid.UUID
	ModelID               uuid.UUID
	EmbeddingSnapshotID   uuid.UUID
	QueryText             string
	TopK                  int
	MetadataFilters       string
	RetrievedContextIDs   string
	RetrievedContexts     string
	PromptText            string
	AnswerText            string
	PromptStrategyVersion string
	GenerationProtocol    string
	GenerationModel       string
	LatencyMs             int64
	Status                InferenceRequestStatus
	ErrorMessage          string
}
