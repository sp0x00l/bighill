package model

import "github.com/google/uuid"

type Model struct {
	ModelID            uuid.UUID
	UserID             uuid.UUID
	OrgID              uuid.UUID
	TrainingRunID      uuid.UUID
	DatasetID          uuid.UUID
	ModelKind          ModelKind
	Source             ModelSource
	SourceURI          string
	SourceMetadata     string
	Name               string
	ModelVersion       int
	BaseModel          string
	ArtifactLocation   string
	ArtifactFormat     string
	ArtifactChecksum   string
	ArtifactSizeBytes  int64
	AdapterURI         string
	AdapterRank        int
	ServingTarget      string
	ServingModel       string
	ServingProtocol    ServingProtocol
	ServingLoadStatus  ModelLoadStatus
	MetricsMetadata    string
	PromotionReportURI string
	PromotionDeltas    string
	PromotionDecision  string
	PromotionReason    string
	Status             ModelStatus
	FailureReason      string
}
