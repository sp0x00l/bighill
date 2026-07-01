package app

import (
	"fmt"
	"time"

	"training_service/pkg/domain/model"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TrainModelWorkflowName                = "training.train_model"
	PrepareTrainingDatasetActivity        = "training.prepare_training_dataset"
	RunTrainingJobActivity                = "training.run_training_job"
	EvaluateTrainedModelActivity          = "training.evaluate_trained_model"
	PublishModelTrainingCompletedActivity = "training.publish_model_training_completed"
	PublishModelTrainingFailedActivity    = "training.publish_model_training_failed"
	DefaultTrainingWorkflowTaskQueue      = "training-service"
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
	if err := workflow.ExecuteActivity(ctx, RunTrainingJobActivity, prepared, request).Get(ctx, &artifact); err != nil {
		return nil, err
	}

	var report model.EvaluationReport
	if err := workflow.ExecuteActivity(ctx, EvaluateTrainedModelActivity, artifact, request).Get(ctx, &report); err != nil {
		return nil, err
	}

	result := &model.TrainingRunResult{
		TrainingRunID:     request.TrainingRunID,
		DatasetID:         request.DatasetID,
		DatasetVersion:    request.DatasetVersion,
		FeatureSnapshotID: request.FeatureSnapshotID,
		ModelURI:          artifact.ModelURI,
		ModelName:         artifact.ModelName,
		ModelVersion:      artifact.ModelVersion,
		BaseModel:         artifact.BaseModel,
		ArtifactFormat:    artifact.ArtifactFormat,
		ArtifactChecksum:  artifact.ArtifactChecksum,
		ArtifactSizeBytes: artifact.ArtifactSizeBytes,
		MetricsMetadata:   fmt.Sprintf(`{"passed":%t}`, report.Passed),
		ReportURI:         report.ReportURI,
		Status:            model.TrainingRunStatusCompleted,
	}

	if !report.Passed {
		result.Status = model.TrainingRunStatusFailed
		result.FailureReason = "model evaluation failed"
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
