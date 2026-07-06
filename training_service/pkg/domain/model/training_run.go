package model

import "fmt"

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
	IdempotencyKey    string
	DatasetID         string
	SourceModelID     string
	TrainingProfile   string
	EvaluationProfile string
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
	ModelKind         string
	Name              string
	ModelVersion      int
	BaseModel         string
	ArtifactLocation  string
	ArtifactChecksum  string
	AdapterURI        string
	ServingLoadStatus string
	Status            string
}

type PreparedTrainingDataset struct {
	TrainingRunID     string
	FeatureSnapshotID string
	DatasetURI        string
}

type TrainedModelArtifact struct {
	TrainingRunID     string `json:"training_run_id"`
	ModelURI          string `json:"model_uri"`
	ModelName         string `json:"model_name"`
	ModelVersion      string `json:"model_version"`
	BaseModel         string `json:"base_model"`
	ArtifactFormat    string `json:"artifact_format"`
	ArtifactChecksum  string `json:"artifact_checksum"`
	ArtifactSizeBytes int64  `json:"artifact_size_bytes"`
	AdapterURI        string `json:"adapter_uri"`
	ServingTarget     string `json:"serving_target"`
	ServingModel      string `json:"serving_model"`
	ServingLoadStatus string `json:"serving_load_status"`
	RecipeHash        string `json:"recipe_hash"`
}

type EvaluationReport struct {
	TrainingRunID        string             `json:"training_run_id"`
	ReportURI            string             `json:"report_uri"`
	Passed               bool               `json:"passed"`
	Metrics              map[string]float64 `json:"metrics,omitempty"`
	Thresholds           map[string]float64 `json:"thresholds,omitempty"`
	EvaluatorName        string             `json:"evaluator_name,omitempty"`
	EvaluatorVersion     string             `json:"evaluator_version,omitempty"`
	MetricSuite          string             `json:"metric_suite,omitempty"`
	EvalDatasetURI       string             `json:"eval_dataset_uri,omitempty"`
	EvalDatasetMode      string             `json:"eval_dataset_mode,omitempty"`
	JudgeProvider        string             `json:"judge_provider,omitempty"`
	JudgeModel           string             `json:"judge_model,omitempty"`
	JudgeTemplateVersion string             `json:"judge_template_version,omitempty"`
	DeepchecksPassed     bool               `json:"deepchecks_passed,omitempty"`
	DeepchecksReportURI  string             `json:"deepchecks_report_uri,omitempty"`
	EvidentlyPassed      bool               `json:"evidently_passed,omitempty"`
	EvidentlyReportURI   string             `json:"evidently_report_uri,omitempty"`
	ScoreRowsURI         string             `json:"score_rows_uri,omitempty"`
	FailureReason        string             `json:"failure_reason,omitempty"`
}

type TrainingJobSpec struct {
	TrainingRunID          string
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
	ModelURI             string
	EvaluationProfile    string
	ReportURI            string
	ReportManifestURI    string
	ArtifactBucketRegion string
	SubmissionID         string
}

type PromotionReportJobSpec struct {
	UserID                   string
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
	UserID              string             `json:"user_id"`
	ModelID             string             `json:"model_id"`
	TrainingRunID       string             `json:"training_run_id"`
	PromotionReportURI  string             `json:"promotion_report_uri"`
	DeepchecksPassed    bool               `json:"deepchecks_passed,omitempty"`
	DeepchecksReportURI string             `json:"deepchecks_report_uri,omitempty"`
	EvidentlyPassed     bool               `json:"evidently_passed,omitempty"`
	EvidentlyReportURI  string             `json:"evidently_report_uri,omitempty"`
	Deltas              map[string]float64 `json:"deltas,omitempty"`
	FailureReason       string             `json:"failure_reason,omitempty"`
}

type TrainingRunResult struct {
	TrainingRunID     string
	UserID            string
	DatasetID         string
	DatasetVersion    string
	FeatureSnapshotID string
	SourceModelID     string
	ModelID           string
	ModelURI          string
	ModelName         string
	ModelVersion      string
	BaseModel         string
	ArtifactFormat    string
	ArtifactChecksum  string
	ArtifactSizeBytes int64
	AdapterURI        string
	ServingTarget     string
	ServingModel      string
	ServingLoadStatus string
	MetricsMetadata   string
	ReportURI         string
	FailureReason     string
	Status            TrainingRunStatus
}
