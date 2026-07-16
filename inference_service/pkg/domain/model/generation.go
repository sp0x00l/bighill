package model

import (
	"encoding/json"
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
	AgentRunID            uuid.UUID
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
	Messages              []ChatMessage
	Tools                 []ToolSpec
	ToolChoice            string
	Options               GenerationOptions
}

type ChatMessageRole string

const (
	ChatMessageRoleSystem    ChatMessageRole = "system"
	ChatMessageRoleUser      ChatMessageRole = "user"
	ChatMessageRoleAssistant ChatMessageRole = "assistant"
	ChatMessageRoleTool      ChatMessageRole = "tool"
)

type ChatMessage struct {
	Role       ChatMessageRole `json:"role"`
	Content    string          `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	Name       string          `json:"name,omitempty"`
	ToolCalls  []ToolCall      `json:"tool_calls,omitempty"`
}

type ToolSpec struct {
	Name                  string          `json:"name"`
	Description           string          `json:"description"`
	Parameters            json.RawMessage `json:"parameters"`
	ImplementationVersion string          `json:"implementation_version,omitempty"`
	Locality              string          `json:"locality,omitempty"`
}

type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type TokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type GenerationOptions struct {
	Temperature     float64 `json:"temperature"`
	TopP            float64 `json:"top_p"`
	Seed            int64   `json:"seed,omitempty"`
	MaxOutputTokens int     `json:"max_output_tokens"`
}

type GenerationFinishReason string

const (
	GenerationFinishReasonStop      GenerationFinishReason = "stop"
	GenerationFinishReasonToolCalls GenerationFinishReason = "tool_calls"
	GenerationFinishReasonLength    GenerationFinishReason = "length"
)

type GenerationResult struct {
	Content      string                 `json:"content"`
	ToolCalls    []ToolCall             `json:"tool_calls,omitempty"`
	FinishReason GenerationFinishReason `json:"finish_reason"`
	Usage        TokenUsage             `json:"usage"`
	Options      GenerationOptions      `json:"options"`
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
