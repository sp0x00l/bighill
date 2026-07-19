package model

import (
	"strings"

	"github.com/google/uuid"
)

const (
	DefaultGraphExtractionModel         = ""
	DefaultGraphExtractionPromptVersion = "graph_extraction_prompt_v1"
	DefaultGraphExtractionSchemaVersion = "graph_extraction_v1"
)

type GraphExtractionStrategy struct {
	ExtractionModel         string
	ExtractionPromptVersion string
	ExtractionSchemaVersion string
}

func ApplyGraphExtractionStrategyDefaults(strategy GraphExtractionStrategy) GraphExtractionStrategy {
	strategy.ExtractionModel = strings.TrimSpace(strategy.ExtractionModel)
	if strategy.ExtractionModel == "" {
		strategy.ExtractionModel = DefaultGraphExtractionModel
	}
	strategy.ExtractionPromptVersion = strings.TrimSpace(strategy.ExtractionPromptVersion)
	if strategy.ExtractionPromptVersion == "" {
		strategy.ExtractionPromptVersion = DefaultGraphExtractionPromptVersion
	}
	strategy.ExtractionSchemaVersion = strings.TrimSpace(strategy.ExtractionSchemaVersion)
	if strategy.ExtractionSchemaVersion == "" {
		strategy.ExtractionSchemaVersion = DefaultGraphExtractionSchemaVersion
	}
	return strategy
}

type GraphSnapshot struct {
	GraphSnapshotID         uuid.UUID
	FeatureSnapshotID       uuid.UUID
	EmbeddingSnapshotID     uuid.UUID
	DatasetID               uuid.UUID
	UserID                  uuid.UUID
	OrgID                   uuid.UUID
	MaterializationEventSeq int64
	IdempotencyKey          uuid.UUID
	ProvenanceHash          string
	ExtractionModel         string
	ExtractionPromptVersion string
	ExtractionSchemaVersion string
	ChunkCount              int64
	ChunksProcessed         int64
	EntityCount             int64
	EdgeCount               int64
	ActiveForRetrieval      bool
	Status                  SnapshotStatus
	FailureReason           string
}

type GraphChunk struct {
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	OrgID               uuid.UUID
	ChunkIndex          int
	SourceText          string
}

type GraphNode struct {
	GraphNodeID     uuid.UUID
	GraphSnapshotID uuid.UUID
	DatasetID       uuid.UUID
	UserID          uuid.UUID
	OrgID           uuid.UUID
	EntityKey       string
	Name            string
	Type            string
	Description     string
	MentionCount    int
}

type GraphEdge struct {
	GraphEdgeID     uuid.UUID
	GraphSnapshotID uuid.UUID
	DatasetID       uuid.UUID
	UserID          uuid.UUID
	OrgID           uuid.UUID
	SourceNodeID    uuid.UUID
	TargetNodeID    uuid.UUID
	SourceEntityKey string
	TargetEntityKey string
	RelationType    string
	Description     string
	Weight          float64
}

type GraphNodeChunk struct {
	GraphNodeChunkID    uuid.UUID
	GraphSnapshotID     uuid.UUID
	GraphNodeID         uuid.UUID
	EntityKey           string
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	OrgID               uuid.UUID
	ChunkIndex          int
	SourceText          string
}

type GraphMaterialization struct {
	Snapshot   *GraphSnapshot
	Nodes      []GraphNode
	Edges      []GraphEdge
	NodeChunks []GraphNodeChunk
}

type GraphExtractionEntity struct {
	ID          string
	Name        string
	Type        string
	Description string
	ChunkIndex  int
}

type GraphExtractionRelation struct {
	Source      string
	Target      string
	Type        string
	Description string
	Weight      float64
}

type GraphExtraction struct {
	Entities  []GraphExtractionEntity
	Relations []GraphExtractionRelation
}

type GraphSearchResult struct {
	GraphSnapshot   *GraphSnapshot
	Contexts        []GraphRetrievedContext
	MatchedEntities []GraphMatchedEntity
	Paths           []GraphPath
}

type GraphRetrievedContext struct {
	GraphNodeChunkID    uuid.UUID
	GraphNodeID         uuid.UUID
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	ChunkIndex          int
	SourceText          string
	Score               float64
	OrgID               uuid.UUID
}

type GraphMatchedEntity struct {
	GraphNodeID uuid.UUID
	Name        string
	Type        string
	Description string
	Score       float64
}

type GraphPath struct {
	GraphNodeIDs  []uuid.UUID
	RelationTypes []string
	Score         float64
}
