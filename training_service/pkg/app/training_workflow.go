package app

import (
	"encoding/json"
	"fmt"
	"time"

	"training_service/pkg/domain/model"

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
		return nil, err
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
		return nil, err
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
		return nil, err
	}

	result := &model.TrainingRunResult{
		TrainingRunID:     request.TrainingRunID,
		UserID:            request.UserID,
		DatasetID:         request.DatasetID,
		DatasetVersion:    request.DatasetVersion,
		FeatureSnapshotID: request.FeatureSnapshotID,
		ModelID:           request.TrainingRunID,
		ModelURI:          artifact.ModelURI,
		ModelName:         artifact.ModelName,
		ModelVersion:      artifact.ModelVersion,
		BaseModel:         artifact.BaseModel,
		ArtifactFormat:    artifact.ArtifactFormat,
		ArtifactChecksum:  artifact.ArtifactChecksum,
		ArtifactSizeBytes: artifact.ArtifactSizeBytes,
		AdapterURI:        artifact.AdapterURI,
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
	})
	if err != nil {
		panic(fmt.Sprintf("marshal evaluation metrics metadata: %v", err))
	}
	return string(raw)
}
