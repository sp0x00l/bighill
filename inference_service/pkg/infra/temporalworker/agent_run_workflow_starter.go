package temporalworker

import (
	"context"
	"errors"
	"time"

	"inference_service/pkg/app"

	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type AgentRunWorkflowStarter struct {
	temporalClient client.Client
	taskQueue      string
}

func NewAgentRunWorkflowStarter(temporalClient client.Client, taskQueue string) *AgentRunWorkflowStarter {
	log.Trace("NewAgentRunWorkflowStarter")

	return &AgentRunWorkflowStarter{
		temporalClient: temporalClient,
		taskQueue:      taskQueue,
	}
}

func (s *AgentRunWorkflowStarter) StartAgentRunWorkflow(ctx context.Context, input app.AgentRunWorkflowInput) error {
	log.Trace("AgentRunWorkflowStarter StartAgentRunWorkflow")

	timeout := time.Duration(input.WallMs) * time.Millisecond
	_, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       app.AgentRunWorkflowID(input.Request.OrgID, input.Request.RequestID),
		TaskQueue:                s.taskQueue,
		WorkflowExecutionTimeout: timeout,
		WorkflowRunTimeout:       timeout,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, app.AgentRunWorkflowName, input)
	if err == nil {
		return nil
	}
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	return err
}
