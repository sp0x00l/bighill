package temporalworker

import (
	"context"
	"errors"

	"training_service/pkg/app"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type TrainingWorkflowStarter struct {
	temporalClient client.Client
	taskQueue      string
}

func NewTrainingWorkflowStarter(temporalClient client.Client, taskQueue string) *TrainingWorkflowStarter {
	log.Trace("NewTrainingWorkflowStarter")

	return &TrainingWorkflowStarter{
		temporalClient: temporalClient,
		taskQueue:      taskQueue,
	}
}

func (s *TrainingWorkflowStarter) StartTrainingWorkflow(ctx context.Context, request model.TrainingRunRequest) error {
	log.Trace("TrainingWorkflowStarter StartTrainingWorkflow")

	_, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    TrainingWorkflowID(request.TrainingRunID),
		TaskQueue:             s.taskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, app.TrainModelWorkflowName, request)
	if err == nil {
		return nil
	}
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	return err
}

func TrainingWorkflowID(trainingRunID string) string {
	log.Trace("TrainingWorkflowID")

	return "training:" + trainingRunID
}
