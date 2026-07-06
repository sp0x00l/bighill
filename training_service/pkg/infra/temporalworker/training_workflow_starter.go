package temporalworker

import (
	"context"
	"errors"
	"strings"

	"training_service/pkg/app"
	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

const (
	workflowStatusCanceled       = "CANCELED"
	workflowStatusCompleted      = "COMPLETED"
	workflowStatusContinuedAsNew = "CONTINUED_AS_NEW"
	workflowStatusFailed         = "FAILED"
	workflowStatusRunning        = "RUNNING"
	workflowStatusTerminated     = "TERMINATED"
	workflowStatusTimedOut       = "TIMED_OUT"
	workflowStatusUnspecified    = "UNSPECIFIED"
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

func (s *TrainingWorkflowStarter) ReadTrainingWorkflowStatus(ctx context.Context, trainingRunID string) (*model.TrainingRunStatusResult, error) {
	log.Trace("TrainingWorkflowStarter ReadTrainingWorkflowStatus")

	response, err := s.temporalClient.DescribeWorkflowExecution(ctx, TrainingWorkflowID(trainingRunID), "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil, domain.ErrTrainingRunNotFound.Extend(trainingRunID)
		}
		return nil, err
	}
	status := enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
	if info := response.GetWorkflowExecutionInfo(); info != nil {
		status = info.GetStatus()
	}
	return &model.TrainingRunStatusResult{
		TrainingRunID: trainingRunID,
		Status:        workflowExecutionStatusString(status),
	}, nil
}

func TrainingWorkflowID(trainingRunID string) string {
	log.Trace("TrainingWorkflowID")

	return "training:" + trainingRunID
}

func workflowExecutionStatusString(status enumspb.WorkflowExecutionStatus) string {
	log.Trace("workflowExecutionStatusString")

	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return workflowStatusRunning
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return workflowStatusCompleted
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return workflowStatusFailed
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return workflowStatusCanceled
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return workflowStatusTerminated
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return workflowStatusContinuedAsNew
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return workflowStatusTimedOut
	case enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		return workflowStatusUnspecified
	default:
		value := strings.TrimPrefix(status.String(), "WORKFLOW_EXECUTION_STATUS_")
		return strings.ToUpper(value)
	}
}
