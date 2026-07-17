package temporalworker

import (
	"context"

	"inference_service/pkg/app"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type AgentRunExecutor interface {
	PrepareAgentRunActivity(ctx context.Context, input app.PrepareAgentRunActivityInput) (app.AgentRunWorkflowState, error)
	GenerateAgentStepActivity(ctx context.Context, input app.GenerateAgentStepActivityInput) (app.GenerateAgentStepActivityOutput, error)
	RecordAgentStepActivity(ctx context.Context, input app.RecordAgentStepActivityInput) (uuid.UUID, error)
	InvokeAgentToolActivity(ctx context.Context, input app.InvokeAgentToolActivityInput) (app.InvokeAgentToolActivityOutput, error)
	CompleteAgentRunActivity(ctx context.Context, input app.CompleteAgentRunActivityInput) error
	FailAgentRunActivity(ctx context.Context, input app.FailAgentRunActivityInput) error
}

type AgentRunActivities struct {
	executor AgentRunExecutor
}

func NewAgentRunActivities(executor AgentRunExecutor) *AgentRunActivities {
	log.Trace("NewAgentRunActivities")

	return &AgentRunActivities{executor: executor}
}

func (a *AgentRunActivities) PrepareAgentRun(ctx context.Context, input app.PrepareAgentRunActivityInput) (app.AgentRunWorkflowState, error) {
	log.Trace("AgentRunActivities PrepareAgentRun")

	return a.executor.PrepareAgentRunActivity(ctx, input)
}

func (a *AgentRunActivities) GenerateAgentStep(ctx context.Context, input app.GenerateAgentStepActivityInput) (app.GenerateAgentStepActivityOutput, error) {
	log.Trace("AgentRunActivities GenerateAgentStep")

	return a.executor.GenerateAgentStepActivity(ctx, input)
}

func (a *AgentRunActivities) RecordAgentStep(ctx context.Context, input app.RecordAgentStepActivityInput) (uuid.UUID, error) {
	log.Trace("AgentRunActivities RecordAgentStep")

	return a.executor.RecordAgentStepActivity(ctx, input)
}

func (a *AgentRunActivities) InvokeAgentTool(ctx context.Context, input app.InvokeAgentToolActivityInput) (app.InvokeAgentToolActivityOutput, error) {
	log.Trace("AgentRunActivities InvokeAgentTool")

	return a.executor.InvokeAgentToolActivity(ctx, input)
}

func (a *AgentRunActivities) CompleteAgentRun(ctx context.Context, input app.CompleteAgentRunActivityInput) error {
	log.Trace("AgentRunActivities CompleteAgentRun")

	return a.executor.CompleteAgentRunActivity(ctx, input)
}

func (a *AgentRunActivities) FailAgentRun(ctx context.Context, input app.FailAgentRunActivityInput) error {
	log.Trace("AgentRunActivities FailAgentRun")

	return a.executor.FailAgentRunActivity(ctx, input)
}

func NewAgentRunWorker(temporalClient client.Client, taskQueue string, activities *AgentRunActivities) worker.Worker {
	log.Trace("NewAgentRunWorker")

	agentWorker := worker.New(temporalClient, taskQueue, worker.Options{})
	agentWorker.RegisterWorkflowWithOptions(app.AgentRunWorkflow, workflow.RegisterOptions{Name: app.AgentRunWorkflowName})
	agentWorker.RegisterActivity(activities.PrepareAgentRun)
	agentWorker.RegisterActivity(activities.GenerateAgentStep)
	agentWorker.RegisterActivity(activities.RecordAgentStep)
	agentWorker.RegisterActivity(activities.InvokeAgentTool)
	agentWorker.RegisterActivity(activities.CompleteAgentRun)
	agentWorker.RegisterActivity(activities.FailAgentRun)
	return agentWorker
}
