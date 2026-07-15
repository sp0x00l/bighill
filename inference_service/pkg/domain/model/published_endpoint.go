package model

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type PublishedEndpointStatus string

const (
	PublishedEndpointStatusReady    PublishedEndpointStatus = "ready"
	PublishedEndpointStatusDisabled PublishedEndpointStatus = "disabled"
)

type RAGMergeStrategy string

const (
	RAGMergeStrategyReranker        RAGMergeStrategy = "reranker"
	RAGMergeStrategyScoreNormalized RAGMergeStrategy = "score_normalized"
)

type PublishedEndpoint struct {
	EndpointID      uuid.UUID
	OrgID           uuid.UUID
	ModelID         uuid.UUID
	Mode            AgentEndpointMode
	AgentSpecID     uuid.UUID
	AgentSpecHash   string
	DatasetIDs      []uuid.UUID
	MergeStrategy   RAGMergeStrategy
	Status          PublishedEndpointStatus
	DisplayName     string
	CreatedByUserID uuid.UUID
}

func (e PublishedEndpoint) IsReady() bool {
	return e.Status == PublishedEndpointStatusReady
}

type EndpointPublication struct {
	UserID        uuid.UUID
	OrgID         uuid.UUID
	ModelID       uuid.UUID
	Mode          AgentEndpointMode
	AgentSpecID   uuid.UUID
	AgentSpecHash string
	DatasetIDs    []uuid.UUID
	MergeStrategy RAGMergeStrategy
	DisplayName   string
}

type EndpointDatasetBinding struct {
	UserID     uuid.UUID
	OrgID      uuid.UUID
	EndpointID uuid.UUID
	DatasetIDs []uuid.UUID
}

type EndpointMergeConfiguration struct {
	UserID        uuid.UUID
	OrgID         uuid.UUID
	EndpointID    uuid.UUID
	MergeStrategy RAGMergeStrategy
}

func (s RAGMergeStrategy) String() string {
	return string(s)
}

func ToRAGMergeStrategy(value string) (RAGMergeStrategy, error) {
	switch RAGMergeStrategy(strings.TrimSpace(value)) {
	case RAGMergeStrategyReranker:
		return RAGMergeStrategyReranker, nil
	case RAGMergeStrategyScoreNormalized:
		return RAGMergeStrategyScoreNormalized, nil
	default:
		return "", fmt.Errorf("invalid rag merge strategy %q", value)
	}
}
