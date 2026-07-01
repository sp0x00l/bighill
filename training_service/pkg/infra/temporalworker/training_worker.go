package temporalworker

import (
	"training_service/pkg/app"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func NewTrainingWorker(temporalClient client.Client, taskQueue string, activities *TrainingActivities) worker.Worker {
	log.Trace("NewTrainingWorker")

	trainingWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	trainingWorker.RegisterWorkflowWithOptions(app.TrainModelWorkflow, workflowRegisterOptions(app.TrainModelWorkflowName))
	trainingWorker.RegisterActivityWithOptions(activities.PrepareTrainingDataset, activityRegisterOptions(app.PrepareTrainingDatasetActivity))
	trainingWorker.RegisterActivityWithOptions(activities.RunTrainingJob, activityRegisterOptions(app.RunTrainingJobActivity))
	trainingWorker.RegisterActivityWithOptions(activities.EvaluateTrainedModel, activityRegisterOptions(app.EvaluateTrainedModelActivity))
	trainingWorker.RegisterActivityWithOptions(activities.PublishModelTrainingCompleted, activityRegisterOptions(app.PublishModelTrainingCompletedActivity))
	trainingWorker.RegisterActivityWithOptions(activities.PublishModelTrainingFailed, activityRegisterOptions(app.PublishModelTrainingFailedActivity))
	return trainingWorker
}

func workflowRegisterOptions(name string) workflow.RegisterOptions {
	log.Trace("workflowRegisterOptions")

	return workflow.RegisterOptions{Name: name}
}

func activityRegisterOptions(name string) activity.RegisterOptions {
	log.Trace("activityRegisterOptions")

	return activity.RegisterOptions{Name: name}
}
