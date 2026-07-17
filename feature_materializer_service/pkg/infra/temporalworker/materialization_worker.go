package temporalworker

import (
	usecase "feature_materializer_service/pkg/app"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func NewMaterializationWorker(temporalClient client.Client, taskQueue string, activities *MaterializationActivities) worker.Worker {
	log.Trace("NewMaterializationWorker")

	materializationWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	materializationWorker.RegisterWorkflowWithOptions(usecase.MaterializeWorkflow, workflow.RegisterOptions{Name: usecase.MaterializeWorkflowName})
	materializationWorker.RegisterActivityWithOptions(activities.MaterializeRawSnapshot, activity.RegisterOptions{Name: usecase.MaterializeRawSnapshotActivityName})
	materializationWorker.RegisterActivityWithOptions(activities.BuildFeatureSnapshot, activity.RegisterOptions{Name: usecase.BuildFeatureSnapshotActivityName})
	materializationWorker.RegisterActivityWithOptions(activities.MaterializeEmbeddings, activity.RegisterOptions{Name: usecase.MaterializeEmbeddingsActivityName})
	materializationWorker.RegisterActivityWithOptions(activities.MaterializeGraph, activity.RegisterOptions{Name: usecase.MaterializeGraphActivityName})
	return materializationWorker
}
