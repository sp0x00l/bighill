package model

import (
	"strings"

	"github.com/google/uuid"
)

const (
	DefaultGraphExtractionModel         = ""
	DefaultGraphExtractionPromptVersion = "graph_extraction_prompt_v1"
	DefaultGraphExtractionSchemaVersion = "graph_extraction_v1"

	GraphSearchModeLocal  GraphSearchMode = "local"
	GraphSearchModeGlobal GraphSearchMode = "global"

	GraphCommunityAlgorithmConnectedComponents = "connected_components_v1"
	GraphCommunityAlgorithmLeidenV2            = "leiden_v2"
	GraphCommunityReportExtractiveV1           = "graph_community_report_extractive_v1"
	GraphCommunityReportModelServingV2         = "graph_community_report_model_serving_v2"
)

type GraphSearchMode string

func ParseGraphSearchMode(value string) GraphSearchMode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(GraphSearchModeLocal):
		return GraphSearchModeLocal
	case string(GraphSearchModeGlobal):
		return GraphSearchModeGlobal
	default:
		return GraphSearchMode(strings.ToLower(strings.TrimSpace(value)))
	}
}

func (m GraphSearchMode) String() string {
	if m == "" {
		return string(GraphSearchModeLocal)
	}
	return string(m)
}

func (m GraphSearchMode) IsValid() bool {
	switch m {
	case "", GraphSearchModeLocal, GraphSearchModeGlobal:
		return true
	default:
		return false
	}
}

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

type GraphNodeAlias struct {
	GraphNodeAliasID   uuid.UUID
	GraphSnapshotID    uuid.UUID
	GraphNodeID        uuid.UUID
	CanonicalEntityKey string
	SourceEntityKey    string
	Alias              string
	Type               string
	DatasetID          uuid.UUID
	UserID             uuid.UUID
	OrgID              uuid.UUID
}

type GraphNodeEmbedding struct {
	GraphNodeEmbeddingID uuid.UUID
	GraphSnapshotID      uuid.UUID
	GraphNodeID          uuid.UUID
	EntityKey            string
	EmbeddingSnapshotID  uuid.UUID
	DatasetID            uuid.UUID
	UserID               uuid.UUID
	OrgID                uuid.UUID
	EmbeddingText        string
	Vector               []float32
}

type GraphMaterialization struct {
	Snapshot         *GraphSnapshot
	Nodes            []GraphNode
	Edges            []GraphEdge
	NodeChunks       []GraphNodeChunk
	NodeAliases      []GraphNodeAlias
	NodeEmbeddings   []GraphNodeEmbedding
	Communities      []GraphCommunity
	CommunityMembers []GraphCommunityMember
	CommunityReports []GraphCommunityReport
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
	GraphSnapshot    *GraphSnapshot
	Mode             GraphSearchMode
	Contexts         []GraphRetrievedContext
	MatchedEntities  []GraphMatchedEntity
	Paths            []GraphPath
	CommunityReports []GraphCommunityReportMatch
}

type GraphSearchSeed struct {
	QueryText           string
	QueryVector         []float32
	EmbeddingDimensions int
	Mode                GraphSearchMode
}

type GraphCommunity struct {
	GraphCommunityID uuid.UUID
	GraphSnapshotID  uuid.UUID
	DatasetID        uuid.UUID
	UserID           uuid.UUID
	OrgID            uuid.UUID
	CommunityKey     string
	Algorithm        string
	Level            int
	Title            string
	Summary          string
	Rank             float64
	EntityCount      int
	EdgeCount        int
}

type GraphCommunityMember struct {
	GraphCommunityMemberID uuid.UUID
	GraphCommunityID       uuid.UUID
	GraphSnapshotID        uuid.UUID
	GraphNodeID            uuid.UUID
	EntityKey              string
	CommunityKey           string
	DatasetID              uuid.UUID
	UserID                 uuid.UUID
	OrgID                  uuid.UUID
}

type GraphCommunityReport struct {
	GraphCommunityReportID uuid.UUID
	GraphCommunityID       uuid.UUID
	GraphSnapshotID        uuid.UUID
	EmbeddingSnapshotID    uuid.UUID
	DatasetID              uuid.UUID
	UserID                 uuid.UUID
	OrgID                  uuid.UUID
	CommunityKey           string
	Level                  int
	Title                  string
	Summary                string
	ReportText             string
	Rank                   float64
	ReportVersion          string
	EmbeddingText          string
	Vector                 []float32
}

type GraphCommunityReportMatch struct {
	GraphCommunityReportID uuid.UUID
	GraphCommunityID       uuid.UUID
	GraphSnapshotID        uuid.UUID
	DatasetID              uuid.UUID
	OrgID                  uuid.UUID
	CommunityKey           string
	Level                  int
	Title                  string
	Summary                string
	ReportText             string
	Rank                   float64
	Score                  float64
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
