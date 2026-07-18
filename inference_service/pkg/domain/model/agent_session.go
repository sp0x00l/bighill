package model

import "github.com/google/uuid"

type AgentSession struct {
	RunID             uuid.UUID
	OrgID             uuid.UUID
	UserID            uuid.UUID
	Endpoint          *PublishedEndpoint
	Spec              *AgentSpec
	Model             *InferenceModel
	Datasets          []*InferenceDataset
	DataSnapshotSet   []DatasetSnapshotRef
	Messages          []ChatMessage
	ResolvedToolSpecs []ToolSpec
	ToolsetHash       string
	DecodingOptions   GenerationOptions
	TotalTokens       int
}
