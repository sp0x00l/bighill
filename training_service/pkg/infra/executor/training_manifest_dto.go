package executor

import (
	"context"

	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type trainedModelArtifactDTO struct {
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

type evaluationReportDTO struct {
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

type promotionReportDTO struct {
	UserID              string             `json:"user_id"`
	OrgID               string             `json:"org_id"`
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

type trainingManifestDTOAdapter struct{}

func newTrainingManifestDTOAdapter() *trainingManifestDTOAdapter {
	log.Trace("newTrainingManifestDTOAdapter")

	return &trainingManifestDTOAdapter{}
}

func (a *trainingManifestDTOAdapter) ToTrainedModelArtifact(_ context.Context, dto trainedModelArtifactDTO) *model.TrainedModelArtifact {
	log.Trace("trainingManifestDTOAdapter ToTrainedModelArtifact")

	return &model.TrainedModelArtifact{
		TrainingRunID:     dto.TrainingRunID,
		ModelURI:          dto.ModelURI,
		ModelName:         dto.ModelName,
		ModelVersion:      dto.ModelVersion,
		BaseModel:         dto.BaseModel,
		ArtifactFormat:    dto.ArtifactFormat,
		ArtifactChecksum:  dto.ArtifactChecksum,
		ArtifactSizeBytes: dto.ArtifactSizeBytes,
		AdapterURI:        dto.AdapterURI,
		ServingTarget:     dto.ServingTarget,
		ServingModel:      dto.ServingModel,
		ServingLoadStatus: dto.ServingLoadStatus,
		RecipeHash:        dto.RecipeHash,
	}
}

func (a *trainingManifestDTOAdapter) ToEvaluationReport(_ context.Context, dto evaluationReportDTO) *model.EvaluationReport {
	log.Trace("trainingManifestDTOAdapter ToEvaluationReport")

	return &model.EvaluationReport{
		TrainingRunID:        dto.TrainingRunID,
		ReportURI:            dto.ReportURI,
		Passed:               dto.Passed,
		Metrics:              dto.Metrics,
		Thresholds:           dto.Thresholds,
		EvaluatorName:        dto.EvaluatorName,
		EvaluatorVersion:     dto.EvaluatorVersion,
		MetricSuite:          dto.MetricSuite,
		EvalDatasetURI:       dto.EvalDatasetURI,
		EvalDatasetMode:      dto.EvalDatasetMode,
		JudgeProvider:        dto.JudgeProvider,
		JudgeModel:           dto.JudgeModel,
		JudgeTemplateVersion: dto.JudgeTemplateVersion,
		DeepchecksPassed:     dto.DeepchecksPassed,
		DeepchecksReportURI:  dto.DeepchecksReportURI,
		EvidentlyPassed:      dto.EvidentlyPassed,
		EvidentlyReportURI:   dto.EvidentlyReportURI,
		ScoreRowsURI:         dto.ScoreRowsURI,
		FailureReason:        dto.FailureReason,
	}
}

func (a *trainingManifestDTOAdapter) ToPromotionReport(_ context.Context, dto promotionReportDTO) *model.PromotionReport {
	log.Trace("trainingManifestDTOAdapter ToPromotionReport")

	return &model.PromotionReport{
		UserID:              dto.UserID,
		OrgID:               dto.OrgID,
		ModelID:             dto.ModelID,
		TrainingRunID:       dto.TrainingRunID,
		PromotionReportURI:  dto.PromotionReportURI,
		DeepchecksPassed:    dto.DeepchecksPassed,
		DeepchecksReportURI: dto.DeepchecksReportURI,
		EvidentlyPassed:     dto.EvidentlyPassed,
		EvidentlyReportURI:  dto.EvidentlyReportURI,
		Deltas:              dto.Deltas,
		FailureReason:       dto.FailureReason,
	}
}
