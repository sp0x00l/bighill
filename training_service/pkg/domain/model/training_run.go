package model

import (
	"fmt"

	"github.com/google/uuid"
)

type TrainingRunStatus int

const (
	TrainingRunStatusRequested TrainingRunStatus = iota
	TrainingRunStatusPreparingData
	TrainingRunStatusTraining
	TrainingRunStatusEvaluating
	TrainingRunStatusCompleted
	TrainingRunStatusFailed
)

func (s TrainingRunStatus) String() string {
	if s < TrainingRunStatusRequested || s > TrainingRunStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"REQUESTED", "PREPARING_DATA", "TRAINING", "EVALUATING", "COMPLETED", "FAILED"}[s]
}

func ToTrainingRunStatus(value string) (TrainingRunStatus, error) {
	switch value {
	case "REQUESTED":
		return TrainingRunStatusRequested, nil
	case "PREPARING_DATA":
		return TrainingRunStatusPreparingData, nil
	case "TRAINING":
		return TrainingRunStatusTraining, nil
	case "EVALUATING":
		return TrainingRunStatusEvaluating, nil
	case "COMPLETED":
		return TrainingRunStatusCompleted, nil
	case "FAILED":
		return TrainingRunStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid training run status %q", value)
	}
}

type TrainingRunRequest struct {
	TrainingRunID          string
	UserID                 string
	OrgID                  string
	DatasetID              string
	DatasetVersion         string
	FeatureSnapshotID      string
	DatasetURI             string
	PreferenceDatasetID    string
	PreferenceDatasetURI   string
	SourceModelID          string
	SourceArtifactURI      string
	SourceModelKind        string
	SourceArtifactChecksum string
	ParentModelID          string
	ParentModelVersion     string
	ParentAdapterURI       string
	ModelName              string
	LineageName            string
	ModelVersion           string
	BaseModel              string
	EvaluationProfile      string
	TrainingProfile        TrainingProfile
}

type TrainingProfile struct {
	Name                      string
	Trainer                   string
	Adapter                   string
	Quantization              string
	PreferenceDatasetURI      string
	DPOBeta                   float64
	SequenceLength            int
	SamplePacking             bool
	LearningRate              float64
	Epochs                    float64
	MicroBatchSize            int
	GradientAccumulationSteps int
	LoRAR                     int
	LoRAAlpha                 int
}

type StartTrainingRunCommand struct {
	IdempotencyKey    uuid.UUID
	DatasetID         uuid.UUID
	SourceModelID     uuid.UUID
	TrainingProfile   string
	EvaluationProfile string
}

type StartDPOTrainingRunCommand struct {
	IdempotencyKey      uuid.UUID
	PreferenceDatasetID uuid.UUID
	TrainingProfile     string
	EvaluationProfile   string
}

type StartAgentAdapterTrainingRunCommand struct {
	IdempotencyKey     uuid.UUID
	DatasetID          uuid.UUID
	DatasetURI         string
	DatasetContentHash string
	SourceModelID      uuid.UUID
	AgentLineage       string
	TrainingProfile    string
	EvaluationProfile  string
}

type TrainingRunStartResult struct {
	TrainingRunID string
	StatusURL     string
}

type TrainingRunStatusResult struct {
	TrainingRunID string
	Status        string
}

type MaterializedDatasetRef struct {
	DatasetID         string
	UserID            string
	OrgID             string
	DatasetVersion    string
	FeatureSnapshotID string
	DatasetURI        string
	TableName         string
	TableFormat       string
	ProcessingState   string
}

type SourceModelRef struct {
	ModelID           string
	UserID            string
	OrgID             string
	ModelKind         string
	Name              string
	LineageName       string
	ModelVersion      int
	BaseModel         string
	ArtifactLocation  string
	ArtifactChecksum  string
	AdapterURI        string
	ServingLoadStatus string
	Status            string
}

type PreferenceDatasetRef struct {
	PreferenceDatasetID    string
	UserID                 string
	OrgID                  string
	DatasetID              string
	DatasetIDs             []string
	ModelID                string
	ParentModelKind        string
	ParentArtifactURI      string
	ParentArtifactChecksum string
	ParentAdapterURI       string
	ParentBaseModel        string
	ParentModelName        string
	ParentLineageName      string
	ParentModelVersion     int
	OutputURI              string
	EvaluationOutputURI    string
	ExampleCount           int
	IntegrityKey           string
}

type ObjectInfo struct {
	Location  string
	SizeBytes int64
	Checksum  string
}

type PreparedTrainingDataset struct {
	TrainingRunID     string
	OrgID             string
	FeatureSnapshotID string
	DatasetURI        string
}

type TrainedModelArtifact struct {
	TrainingRunID     string
	ModelURI          string
	ModelName         string
	ModelVersion      string
	BaseModel         string
	ArtifactFormat    string
	ArtifactChecksum  string
	ArtifactSizeBytes int64
	AdapterURI        string
	AdapterRank       int
	ServingTarget     string
	ServingModel      string
	ServingLoadStatus string
	RecipeHash        string
}

type EvaluationReport struct {
	TrainingRunID        string
	ReportURI            string
	Passed               bool
	Metrics              map[string]float64
	Thresholds           map[string]float64
	EvaluatorName        string
	EvaluatorVersion     string
	MetricSuite          string
	EvalDatasetURI       string
	EvalDatasetMode      string
	JudgeProvider        string
	JudgeModel           string
	JudgeTemplateVersion string
	DeepchecksPassed     bool
	DeepchecksReportURI  string
	EvidentlyPassed      bool
	EvidentlyReportURI   string
	ScoreRowsURI         string
	FailureReason        string
}

type TrainingJobSpec struct {
	TrainingRunID          string
	OrgID                  string
	DatasetURI             string
	PreferenceDatasetID    string
	ModelName              string
	ModelVersion           string
	BaseModel              string
	SourceModelID          string
	SourceArtifactURI      string
	SourceModelKind        string
	SourceArtifactChecksum string
	ParentModelID          string
	ParentModelVersion     string
	ParentAdapterURI       string
	TrainingProfile        TrainingProfile
	ModelURI               string
	AdapterURI             string
	AdapterRank            int
	ServingTarget          string
	ServingModel           string
	ServingLoadStatus      string
	ArtifactFormat         string
	ArtifactManifestURI    string
	ArtifactBucketRegion   string
	AxolotlCommand         string
	RecipeYAML             string
	RecipeHash             string
	SubmissionID           string
}

type EvaluationJobSpec struct {
	TrainingRunID        string
	OrgID                string
	ModelURI             string
	EvaluationProfile    string
	ReportURI            string
	ReportManifestURI    string
	ArtifactBucketRegion string
	SubmissionID         string
}

type PromotionReportJobSpec struct {
	UserID                   string
	OrgID                    string
	ModelID                  string
	TrainingRunID            string
	CandidateReportURI       string
	CandidateMetricsMetadata string
	ChampionModelID          string
	ChampionReportURI        string
	ChampionMetricsMetadata  string
	PromotionProfile         string
	ReportURI                string
	ReportManifestURI        string
	ArtifactBucketRegion     string
	SubmissionID             string
}

type PromotionReport struct {
	UserID              string
	OrgID               string
	ModelID             string
	TrainingRunID       string
	PromotionReportURI  string
	DeepchecksPassed    bool
	DeepchecksReportURI string
	EvidentlyPassed     bool
	EvidentlyReportURI  string
	Deltas              map[string]float64
	FailureReason       string
}

type TrainingRunResult struct {
	TrainingRunID     string
	UserID            string
	OrgID             string
	DatasetID         string
	DatasetVersion    string
	FeatureSnapshotID string
	SourceModelID     string
	ModelID           string
	ModelURI          string
	ModelName         string
	LineageName       string
	ModelVersion      string
	BaseModel         string
	ArtifactFormat    string
	ArtifactChecksum  string
	ArtifactSizeBytes int64
	AdapterURI        string
	AdapterRank       int
	ServingTarget     string
	ServingModel      string
	ServingLoadStatus string
	MetricsMetadata   string
	ReportURI         string
	FailureReason     string
	Status            TrainingRunStatus
}
