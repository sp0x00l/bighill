package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TrainModelWorkflowName                 = "training.train_model"
	PrepareTrainingDatasetActivity         = "training.prepare_training_dataset"
	RunTrainingJobActivity                 = "training.run_training_job"
	EvaluateTrainedModelActivity           = "training.evaluate_trained_model"
	PublishModelTrainingCompletedActivity  = "training.publish_model_training_completed"
	PublishModelTrainingFailedActivity     = "training.publish_model_training_failed"
	DefaultTrainingWorkflowTaskQueue       = "training-service"
	DefaultRunTrainingActivityTimeout      = 24 * time.Hour
	DefaultEvaluateTrainingActivityTimeout = 24 * time.Hour
	DefaultTrainingActivityHeartbeat       = 2 * time.Minute
)

func TrainModelWorkflow(ctx workflow.Context, request model.TrainingRunRequest) (*model.TrainingRunResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("TrainModelWorkflow started", "training_run_id", request.TrainingRunID, "dataset_id", request.DatasetID)

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var prepared model.PreparedTrainingDataset
	if err := workflow.ExecuteActivity(ctx, PrepareTrainingDatasetActivity, request).Get(ctx, &prepared); err != nil {
		return nil, publishActivityFailure(ctx, request, "prepare training dataset", err)
	}

	var artifact model.TrainedModelArtifact
	runTrainingCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "run-training:" + request.TrainingRunID,
		StartToCloseTimeout:    DefaultRunTrainingActivityTimeout,
		ScheduleToCloseTimeout: DefaultRunTrainingActivityTimeout,
		HeartbeatTimeout:       DefaultTrainingActivityHeartbeat,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})
	if err := workflow.ExecuteActivity(runTrainingCtx, RunTrainingJobActivity, prepared, request).Get(ctx, &artifact); err != nil {
		return nil, publishActivityFailure(ctx, request, "run training job", err)
	}

	var report model.EvaluationReport
	evaluateCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "evaluate:" + request.TrainingRunID,
		StartToCloseTimeout:    DefaultEvaluateTrainingActivityTimeout,
		ScheduleToCloseTimeout: DefaultEvaluateTrainingActivityTimeout,
		HeartbeatTimeout:       DefaultTrainingActivityHeartbeat,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})
	if err := workflow.ExecuteActivity(evaluateCtx, EvaluateTrainedModelActivity, artifact, request).Get(ctx, &report); err != nil {
		return nil, publishActivityFailure(ctx, request, "evaluate trained model", err)
	}

	result := &model.TrainingRunResult{
		TrainingRunID:     request.TrainingRunID,
		UserID:            request.UserID,
		OrgID:             request.OrgID,
		DatasetID:         request.DatasetID,
		DatasetVersion:    request.DatasetVersion,
		FeatureSnapshotID: request.FeatureSnapshotID,
		ModelID:           request.TrainingRunID,
		ModelURI:          artifact.ModelURI,
		ModelName:         artifact.ModelName,
		LineageName:       request.LineageName,
		ModelVersion:      artifact.ModelVersion,
		BaseModel:         artifact.BaseModel,
		ArtifactFormat:    artifact.ArtifactFormat,
		ArtifactChecksum:  artifact.ArtifactChecksum,
		ArtifactSizeBytes: artifact.ArtifactSizeBytes,
		AdapterURI:        artifact.AdapterURI,
		AdapterRank:       artifact.AdapterRank,
		ServingTarget:     artifact.ServingTarget,
		ServingModel:      artifact.ServingModel,
		ServingLoadStatus: artifact.ServingLoadStatus,
		MetricsMetadata:   evaluationMetricsMetadata(report),
		ReportURI:         report.ReportURI,
		Status:            model.TrainingRunStatusCompleted,
	}

	if !report.Passed {
		result.Status = model.TrainingRunStatusFailed
		result.FailureReason = report.FailureReason
		if err := workflow.ExecuteActivity(ctx, PublishModelTrainingFailedActivity, *result).Get(ctx, nil); err != nil {
			return nil, err
		}
		logger.Info("TrainModelWorkflow failed evaluation", "training_run_id", request.TrainingRunID, "report_uri", result.ReportURI)
		return result, nil
	}

	if err := workflow.ExecuteActivity(ctx, PublishModelTrainingCompletedActivity, *result).Get(ctx, nil); err != nil {
		return nil, err
	}

	logger.Info("TrainModelWorkflow completed", "training_run_id", request.TrainingRunID, "model_uri", result.ModelURI)
	return result, nil
}

func publishActivityFailure(ctx workflow.Context, request model.TrainingRunRequest, stage string, cause error) error {
	failed := failedTrainingRunResult(request, fmt.Sprintf("%s failed: %v", stage, cause))
	if err := workflow.ExecuteActivity(ctx, PublishModelTrainingFailedActivity, *failed).Get(ctx, nil); err != nil {
		return errors.Join(cause, fmt.Errorf("publish model training failed fact: %w", err))
	}
	return cause
}

func failedTrainingRunResult(request model.TrainingRunRequest, reason string) *model.TrainingRunResult {
	return &model.TrainingRunResult{
		TrainingRunID:     request.TrainingRunID,
		UserID:            request.UserID,
		OrgID:             request.OrgID,
		DatasetID:         request.DatasetID,
		DatasetVersion:    request.DatasetVersion,
		FeatureSnapshotID: request.FeatureSnapshotID,
		SourceModelID:     request.SourceModelID,
		ModelID:           request.TrainingRunID,
		ModelName:         request.ModelName,
		LineageName:       request.LineageName,
		ModelVersion:      request.ModelVersion,
		BaseModel:         request.BaseModel,
		FailureReason:     reason,
		Status:            model.TrainingRunStatusFailed,
	}
}

func evaluationMetricsMetadata(report model.EvaluationReport) string {
	raw, err := json.Marshal(struct {
		Passed               bool               `json:"passed"`
		Metrics              map[string]float64 `json:"metrics,omitempty"`
		Thresholds           map[string]float64 `json:"thresholds,omitempty"`
		ReportURI            string             `json:"report_uri,omitempty"`
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
	}{
		Passed:               report.Passed,
		Metrics:              report.Metrics,
		Thresholds:           report.Thresholds,
		ReportURI:            report.ReportURI,
		EvaluatorName:        report.EvaluatorName,
		EvaluatorVersion:     report.EvaluatorVersion,
		MetricSuite:          report.MetricSuite,
		EvalDatasetURI:       report.EvalDatasetURI,
		EvalDatasetMode:      report.EvalDatasetMode,
		JudgeProvider:        report.JudgeProvider,
		JudgeModel:           report.JudgeModel,
		JudgeTemplateVersion: report.JudgeTemplateVersion,
		DeepchecksPassed:     report.DeepchecksPassed,
		DeepchecksReportURI:  report.DeepchecksReportURI,
		EvidentlyPassed:      report.EvidentlyPassed,
		EvidentlyReportURI:   report.EvidentlyReportURI,
		ScoreRowsURI:         report.ScoreRowsURI,
	})
	if err != nil {
		log.Fatalf("marshal evaluation metrics metadata: %v", err)
	}
	return string(raw)
}
