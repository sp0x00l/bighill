package app

import (
	"fmt"
	"strings"
	"time"

	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	AgentRunWorkflowName              = "inference.agent_run"
	PrepareAgentRunActivityName       = "PrepareAgentRun"
	GenerateAgentStepActivityName     = "GenerateAgentStep"
	RecordAgentStepActivityName       = "RecordAgentStep"
	InvokeAgentToolActivityName       = "InvokeAgentTool"
	CompleteAgentRunActivityName      = "CompleteAgentRun"
	FailAgentRunActivityName          = "FailAgentRun"
	DefaultAgentRunWorkflowTaskQueue  = "inference-service"
	agentRunEffectActivityMaxAttempts = 1
	agentRunRecordActivityMaxAttempts = 3
)

type AgentRunWorkflowInput struct {
	EndpointID    uuid.UUID
	AgentSpecHash string
	Request       model.GenerateRequest
	WallMs        int
}

type AgentRunWorkflowState struct {
	Request                   model.GenerateRequest
	EndpointID                uuid.UUID
	ModelID                   uuid.UUID
	AgentSpecHash             string
	DatasetIDs                []uuid.UUID
	ToolsetHash               string
	EffectiveBaseID           string
	DataSnapshotSet           []model.DatasetSnapshotRef
	DataSnapshotHash          string
	MergeStrategy             model.RAGMergeStrategy
	Budgets                   model.AgentBudgets
	ToolBindings              []model.ToolBinding
	ServingProtocol           string
	ServingModel              string
	ServingTarget             string
	LoraName                  string
	AdapterURI                string
	DecodingOptions           model.GenerationOptions
	TotalTokens               int
	LastToolCallSignature     string
	RepeatedToolCallCount     int
	TransientToolFailureCount map[string]int
}

type PrepareAgentRunActivityInput struct {
	EndpointID    uuid.UUID
	AgentSpecHash string
	Request       model.GenerateRequest
}

type GenerateAgentStepActivityInput struct {
	State      AgentRunWorkflowState
	StepIndex  int
	ToolChoice string
	Options    model.GenerationOptions
}

type GenerateAgentStepActivityOutput struct {
	Result              model.GenerationResult
	PromptTokenEstimate int
	TokenUsage          int
	StopReason          string
	ErrorMessage        string
}

type RecordAgentStepActivityInput struct {
	State            AgentRunWorkflowState
	StepIndex        int
	GenerationResult model.GenerationResult
}

type InvokeAgentToolActivityInput struct {
	State   AgentRunWorkflowState
	StepID  uuid.UUID
	Call    model.ToolCall
	CallKey string
}

type InvokeAgentToolActivityOutput struct {
	IsError       bool
	ErrorType     string
	TokenEstimate int
}

type CompleteAgentRunActivityInput struct {
	State  AgentRunWorkflowState
	Answer string
}

type FailAgentRunActivityInput struct {
	State        AgentRunWorkflowState
	StopReason   string
	ErrorMessage string
}

func AgentRunWorkflowID(orgID uuid.UUID, requestID uuid.UUID) string {
	return fmt.Sprintf("inference-agent:%s:%s", orgID, requestID)
}

func AgentRunWorkflow(ctx workflow.Context, input AgentRunWorkflowInput) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("AgentRunWorkflow started", "org_id", input.Request.OrgID.String(), "request_id", input.Request.RequestID.String(), "run_id", input.Request.AgentRunID.String())

	timeout := time.Duration(input.WallMs) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Minute
	}
	var state AgentRunWorkflowState
	if err := workflow.ExecuteActivity(
		agentRunRecordActivityContext(ctx, timeout, "prepare:"+input.Request.AgentRunID.String()),
		PrepareAgentRunActivityName,
		PrepareAgentRunActivityInput{EndpointID: input.EndpointID, AgentSpecHash: input.AgentSpecHash, Request: input.Request},
	).Get(ctx, &state); err != nil {
		return err
	}
	if state.TransientToolFailureCount == nil {
		state.TransientToolFailureCount = map[string]int{}
	}
	maxSteps := state.Budgets.MaxSteps
	for step := 0; step < maxSteps; step++ {
		toolChoice := agentWorkflowToolChoice(state.ToolBindings, step)
		options := agentWorkflowStepGenerationOptions(state)
		var generation GenerateAgentStepActivityOutput
		err := workflow.ExecuteActivity(
			agentRunEffectActivityContext(ctx, timeout, fmt.Sprintf("generate:%s:%d", state.Request.AgentRunID.String(), step)),
			GenerateAgentStepActivityName,
			GenerateAgentStepActivityInput{State: state, StepIndex: step, ToolChoice: toolChoice, Options: options},
		).Get(ctx, &generation)
		if err != nil {
			failErr := agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonRuntimeError, err.Error())
			if failErr != nil {
				return failErr
			}
			return err
		}
		if strings.TrimSpace(generation.StopReason) != "" {
			return agentWorkflowFail(ctx, timeout, state, agentWorkflowStopReason(generation.StopReason), generation.ErrorMessage)
		}
		state.TotalTokens += generation.TokenUsage
		var stepID uuid.UUID
		err = workflow.ExecuteActivity(
			agentRunRecordActivityContext(ctx, timeout, fmt.Sprintf("record-step:%s:%d", state.Request.AgentRunID.String(), step)),
			RecordAgentStepActivityName,
			RecordAgentStepActivityInput{State: state, StepIndex: step, GenerationResult: generation.Result},
		).Get(ctx, &stepID)
		if err != nil {
			failErr := agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonRuntimeError, err.Error())
			if failErr != nil {
				return failErr
			}
			return err
		}
		if agentWorkflowTokenBudgetExceeded(state) {
			return agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonBudget, "agent reached its token budget before producing a final answer")
		}
		if len(generation.Result.ToolCalls) == 0 {
			answer := strings.TrimSpace(generation.Result.Content)
			if answer == "" {
				answer = "The agent completed without a final answer."
			}
			if err := workflow.ExecuteActivity(
				agentRunRecordActivityContext(ctx, timeout, "complete:"+state.Request.AgentRunID.String()),
				CompleteAgentRunActivityName,
				CompleteAgentRunActivityInput{State: state, Answer: answer},
			).Get(ctx, nil); err != nil {
				return err
			}
			logger.Info("AgentRunWorkflow completed", "org_id", input.Request.OrgID.String(), "request_id", input.Request.RequestID.String(), "run_id", input.Request.AgentRunID.String())
			return nil
		}
		for toolIndex, call := range generation.Result.ToolCalls {
			signature := agentWorkflowToolCallSignature(call)
			if signature == state.LastToolCallSignature {
				state.RepeatedToolCallCount++
			} else {
				state.LastToolCallSignature = signature
				state.RepeatedToolCallCount = 0
			}
			if state.RepeatedToolCallCount >= 2 {
				return agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonLoopDetected, "agent repeated the same tool call")
			}
			callKey := agentToolCallKey(call, toolIndex)
			var toolResult InvokeAgentToolActivityOutput
			toolActivityID := fmt.Sprintf("invoke-tool:%s:%d:%s", state.Request.AgentRunID.String(), step, callKey)
			if err := workflow.ExecuteActivity(
				agentRunEffectActivityContext(ctx, timeout, toolActivityID),
				InvokeAgentToolActivityName,
				InvokeAgentToolActivityInput{State: state, StepID: stepID, Call: call, CallKey: callKey},
			).Get(ctx, &toolResult); err != nil {
				failErr := agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonToolError, err.Error())
				if failErr != nil {
					return failErr
				}
				return err
			}
			state.TotalTokens += toolResult.TokenEstimate
			switch agentToolFailureClass(toolResult.IsError, agentWorkflowToolErrorType(toolResult.ErrorType)) {
			case model.ToolErrorTypeTransient:
				state.TransientToolFailureCount[signature]++
				if state.TransientToolFailureCount[signature] > agentTransientToolFailureRetries {
					return agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonToolError, "agent transient tool failure retry limit exceeded")
				}
			case model.ToolErrorTypePermanent, model.ToolErrorTypePolicyDenied:
				return agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonToolError, "agent tool call failed")
			}
			if agentWorkflowTokenBudgetExceeded(state) {
				return agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonBudget, "agent reached its token budget before producing a final answer")
			}
		}
	}
	return agentWorkflowFail(ctx, timeout, state, model.AgentStopReasonMaxSteps, "agent reached its step limit before producing a final answer")
}

func agentRunEffectActivityContext(ctx workflow.Context, timeout time.Duration, activityID string) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             activityID,
		StartToCloseTimeout:    timeout,
		ScheduleToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: agentRunEffectActivityMaxAttempts,
		},
	})
}

func agentRunRecordActivityContext(ctx workflow.Context, timeout time.Duration, activityID string) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             activityID,
		StartToCloseTimeout:    timeout,
		ScheduleToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: agentRunRecordActivityMaxAttempts,
		},
	})
}

func agentWorkflowFail(ctx workflow.Context, timeout time.Duration, state AgentRunWorkflowState, reason model.AgentStopReason, message string) error {
	if err := workflow.ExecuteActivity(
		agentRunRecordActivityContext(ctx, timeout, "fail:"+state.Request.AgentRunID.String()+":"+reason.String()),
		FailAgentRunActivityName,
		FailAgentRunActivityInput{State: state, StopReason: reason.String(), ErrorMessage: message},
	).Get(ctx, nil); err != nil {
		return err
	}
	return temporal.NewApplicationError(message, reason.String())
}

func agentWorkflowToolErrorType(value string) model.ToolErrorType {
	if strings.TrimSpace(value) == "" || strings.EqualFold(strings.TrimSpace(value), model.ToolErrorTypeUnknown.String()) {
		return model.ToolErrorTypeUnknown
	}
	parsed, err := model.ToToolErrorType(value)
	if err != nil {
		return model.ToolErrorTypePermanent
	}
	return parsed
}

func agentWorkflowStopReason(value string) model.AgentStopReason {
	if strings.TrimSpace(value) == "" || strings.EqualFold(strings.TrimSpace(value), model.AgentStopReasonUnknown.String()) {
		return model.AgentStopReasonUnknown
	}
	parsed, err := model.ToAgentStopReason(value)
	if err != nil {
		return model.AgentStopReasonRuntimeError
	}
	return parsed
}

func agentWorkflowStepGenerationOptions(state AgentRunWorkflowState) model.GenerationOptions {
	options := state.DecodingOptions
	if state.Budgets.Token > 0 {
		remaining := state.Budgets.Token - state.TotalTokens
		if remaining < options.MaxOutputTokens || options.MaxOutputTokens <= 0 {
			options.MaxOutputTokens = remaining
		}
	}
	return options
}

func agentWorkflowTokenBudgetExceeded(state AgentRunWorkflowState) bool {
	return state.Budgets.Token > 0 && state.TotalTokens >= state.Budgets.Token
}

func agentWorkflowTokenBudgetWouldExceed(state AgentRunWorkflowState, additionalTokens int) bool {
	return state.Budgets.Token > 0 && state.TotalTokens+additionalTokens >= state.Budgets.Token
}

func agentWorkflowToolChoice(bindings []model.ToolBinding, step int) string {
	if step > 0 {
		return ""
	}
	for _, binding := range bindings {
		if binding.ToolChoice != "" {
			return binding.ToolChoice
		}
		if binding.Required {
			return agentToolChoiceRequired
		}
	}
	return ""
}

func agentWorkflowToolCallSignature(call model.ToolCall) string {
	return strings.Join([]string{strings.TrimSpace(call.Name), strings.TrimSpace(string(call.Arguments))}, "\x00")
}
