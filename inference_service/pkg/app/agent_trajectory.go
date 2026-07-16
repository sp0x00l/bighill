package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"inference_service/pkg/domain/model"
	"lib/shared_lib/authz"
	"lib/shared_lib/ctxutil"
	serializers "lib/shared_lib/serializer"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	agentTrajectorySchemaVersion = "agent_trajectory_v1"
)

func (u *inferenceUsecase) recordAgentRun(ctx context.Context, session *model.AgentSession, status model.AgentRunStatus, stopReason model.AgentStopReason) (*model.AgentRun, error) {
	log.Trace("InferenceUsecase recordAgentRun")

	decodingParams, err := json.Marshal(session.DecodingOptions)
	if err != nil {
		return nil, fmt.Errorf("marshal decoding params: %w", err)
	}
	toolsetHash, err := agentToolsetHash(session.ResolvedToolSpecs)
	if err != nil {
		return nil, err
	}
	run := &model.AgentRun{
		RunID:                   session.RunID,
		OrgID:                   session.OrgID,
		UserID:                  session.UserID,
		EndpointID:              session.Endpoint.EndpointID,
		AgentSpecHash:           session.Spec.ContentHash,
		ToolsetHash:             toolsetHash,
		TrajectorySchemaVersion: agentTrajectorySchemaVersion,
		DecodingParams:          decodingParams,
		Status:                  status,
		StopReason:              stopReason,
		TotalTokens:             session.TotalTokens,
		WallMs:                  session.Spec.Budgets.WallMs,
	}
	recorded, err := u.trajectoryRepository.RecordAgentRun(ctx, run)
	if err != nil {
		return nil, err
	}
	u.publishAgentRunUserEvent(ctx, recorded)
	return recorded, nil
}

func (u *inferenceUsecase) recordAgentStep(ctx context.Context, session *model.AgentSession, stepIndex int, toolSpecs []model.ToolSpec, generationResult model.GenerationResult) (uuid.UUID, error) {
	log.Trace("InferenceUsecase recordAgentStep")

	presentedToolSchemas, err := agentPresentedToolSchemas(toolSpecs)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal presented tool schemas: %w", err)
	}
	generationResultJSON, err := json.Marshal(generationResult)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal generation result: %w", err)
	}
	step := &model.AgentStep{
		RunID:                session.RunID,
		OrgID:                session.OrgID,
		StepIndex:            stepIndex,
		PresentedToolSchemas: presentedToolSchemas,
		GenerationResult:     generationResultJSON,
		FinishReason:         generationResult.FinishReason,
		PromptTokens:         generationResult.Usage.PromptTokens,
		CompletionTokens:     generationResult.Usage.CompletionTokens,
	}
	recorded, err := u.trajectoryRepository.RecordAgentStep(ctx, step)
	if err != nil {
		return uuid.Nil, err
	}
	u.publishAgentStepUserEvent(ctx, session, recorded)
	return recorded.StepID, nil
}

func (u *inferenceUsecase) recordAgentToolInvocation(ctx context.Context, session *model.AgentSession, stepID uuid.UUID, call model.ToolCall, result model.ToolResult) error {
	log.Trace("InferenceUsecase recordAgentToolInvocation")

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal tool result: %w", err)
	}
	errorType := result.ErrorType
	invocation := &model.AgentToolInvocation{
		InvocationID:    result.InvocationID,
		StepID:          stepID,
		RunID:           session.RunID,
		OrgID:           session.OrgID,
		ToolName:        call.Name,
		ToolImplVersion: result.ToolImplVersion,
		Arguments:       call.Arguments,
		Result:          resultJSON,
		ErrorType:       errorType,
		LatencyMs:       0,
	}
	recorded, err := u.trajectoryRepository.RecordToolInvocation(ctx, invocation)
	if err != nil {
		return err
	}
	u.publishAgentToolResultUserEvent(ctx, session, recorded)
	return nil
}

func (u *inferenceUsecase) ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) (trajectory *model.AgentTrajectory, err error) {
	log.Trace("InferenceUsecase ReadAgentTrajectory")

	ctx = ctxutil.WithOrgID(ctx, orgID)
	ctx, span := startInferenceSpan(ctx, "agent.trajectory.read",
		attribute.String("org_id", orgID.String()),
		attribute.String("run_id", runID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.trajectoryRepository.ReadAgentTrajectory(ctx, orgID, runID)
}

func (u *inferenceUsecase) ReapExpiredAgentRuns(ctx context.Context, safetyMultiplier int) (int64, error) {
	log.Trace("InferenceUsecase ReapExpiredAgentRuns")

	return u.trajectoryRepository.FailExpiredAgentRuns(ctx, safetyMultiplier)
}

func (u *inferenceUsecase) publishAgentRunUserEvent(ctx context.Context, run *model.AgentRun) {
	log.Trace("InferenceUsecase publishAgentRunUserEvent")

	if run == nil {
		return
	}
	eventType := userevents.EventTypeAgentRunStarted
	severity := userevents.SeverityInfo
	title := "Agent run started"
	message := "The agent run has started."
	switch run.Status {
	case model.AgentRunStatusCompleted:
		eventType = userevents.EventTypeAgentRunCompleted
		severity = userevents.SeveritySuccess
		title = "Agent run completed"
		message = "The agent run completed."
	case model.AgentRunStatusFailed:
		eventType = userevents.EventTypeAgentRunFailed
		severity = userevents.SeverityError
		title = "Agent run failed"
		message = "The agent run failed."
	}
	event := userevents.Event{
		EventID: userevents.DeterministicEventID(
			userevents.ResourceTypeAgentRun,
			run.RunID.String(),
			run.Status.String(),
			run.StopReason.String(),
		),
		SourceService:      "inference_service",
		EventType:          eventType,
		Severity:           severity,
		RequiredPermission: authz.PermissionInferenceAgentRunsRead,
		UserID:             optionalAgentEventUUID(run.UserID),
		Resource:           userevents.NewResource(userevents.ResourceTypeAgentRun, run.RunID, "", "/v1/inference/agent-runs/"+run.RunID.String()),
		Status: userevents.Status{
			State: run.Status.String(),
			Phase: userevents.StatusPhaseAgent,
		},
		Title:       title,
		Message:     message,
		ActionLabel: "View run",
		ActionHref:  "/agent-runs/" + run.RunID.String(),
		Metadata: map[string]string{
			"endpoint_id": run.EndpointID.String(),
		},
	}
	if err := u.userEventPublisher.Publish(ctx, event); err != nil {
		userevents.LogPublishFailure(ctx, err, event)
	}
}

func (u *inferenceUsecase) publishAgentStepUserEvent(ctx context.Context, session *model.AgentSession, step *model.AgentStep) {
	log.Trace("InferenceUsecase publishAgentStepUserEvent")

	if step == nil || session == nil {
		return
	}
	event := userevents.Event{
		EventID: userevents.DeterministicEventID(
			userevents.ResourceTypeAgentRun,
			step.RunID.String(),
			"step",
			fmt.Sprintf("%d", step.StepIndex),
		),
		SourceService:      "inference_service",
		EventType:          userevents.EventTypeAgentStepCompleted,
		Severity:           userevents.SeverityInfo,
		RequiredPermission: authz.PermissionInferenceAgentRunsRead,
		UserID:             optionalAgentEventUUID(session.UserID),
		Resource:           userevents.NewResource(userevents.ResourceTypeAgentRun, step.RunID, "", "/v1/inference/agent-runs/"+step.RunID.String()),
		Status:             userevents.Status{State: string(step.FinishReason), Phase: userevents.StatusPhaseAgent},
		Title:              "Agent step completed",
		Message:            "The agent completed a reasoning step.",
		ActionLabel:        "View run",
		ActionHref:         "/agent-runs/" + step.RunID.String(),
		Metadata: map[string]string{
			"step_id":    step.StepID.String(),
			"step_index": fmt.Sprintf("%d", step.StepIndex),
		},
	}
	if err := u.userEventPublisher.Publish(ctx, event); err != nil {
		userevents.LogPublishFailure(ctx, err, event)
	}
}

func (u *inferenceUsecase) publishAgentToolResultUserEvent(ctx context.Context, session *model.AgentSession, invocation *model.AgentToolInvocation) {
	log.Trace("InferenceUsecase publishAgentToolResultUserEvent")

	if invocation == nil || session == nil {
		return
	}
	severity := userevents.SeverityInfo
	state := "COMPLETED"
	if invocation.ErrorType != model.ToolErrorTypeUnknown {
		severity = userevents.SeverityWarning
		state = "FAILED"
	}
	event := userevents.Event{
		EventID: userevents.DeterministicEventID(
			userevents.ResourceTypeAgentRun,
			invocation.RunID.String(),
			"tool",
			invocation.InvocationID.String(),
		),
		SourceService:      "inference_service",
		EventType:          userevents.EventTypeAgentToolResult,
		Severity:           severity,
		RequiredPermission: authz.PermissionInferenceAgentRunsRead,
		UserID:             optionalAgentEventUUID(session.UserID),
		Resource:           userevents.NewResource(userevents.ResourceTypeAgentRun, invocation.RunID, "", "/v1/inference/agent-runs/"+invocation.RunID.String()),
		Status:             userevents.Status{State: state, Phase: userevents.StatusPhaseAgent},
		Title:              "Agent tool result recorded",
		Message:            "The agent recorded a tool result.",
		ActionLabel:        "View run",
		ActionHref:         "/agent-runs/" + invocation.RunID.String(),
		Metadata: map[string]string{
			"step_id":       invocation.StepID.String(),
			"invocation_id": invocation.InvocationID.String(),
			"tool_name":     invocation.ToolName,
			"error_type":    invocation.ErrorType.String(),
		},
	}
	if err := u.userEventPublisher.Publish(ctx, event); err != nil {
		userevents.LogPublishFailure(ctx, err, event)
	}
}

func optionalAgentEventUUID(value uuid.UUID) string {
	log.Trace("optionalAgentEventUUID")

	if value == uuid.Nil {
		return ""
	}
	return value.String()
}

func agentToolsetHash(toolSpecs []model.ToolSpec) (string, error) {
	log.Trace("agentToolsetHash")

	type hashedToolSpec struct {
		Name                  string `json:"name"`
		Description           string `json:"description"`
		Parameters            any    `json:"parameters"`
		Locality              string `json:"locality"`
		ImplementationVersion string `json:"implementation_version"`
	}
	resolved := make([]hashedToolSpec, 0, len(toolSpecs))
	for _, tool := range toolSpecs {
		var parameters any
		if len(tool.Parameters) == 0 {
			parameters = map[string]any{}
		} else if err := json.Unmarshal(tool.Parameters, &parameters); err != nil {
			return "", fmt.Errorf("canonicalize tool parameters for %s: %w", tool.Name, err)
		}
		resolved = append(resolved, hashedToolSpec{
			Name:                  strings.TrimSpace(tool.Name),
			Description:           strings.TrimSpace(tool.Description),
			Parameters:            parameters,
			Locality:              strings.TrimSpace(tool.Locality),
			ImplementationVersion: strings.TrimSpace(tool.ImplementationVersion),
		})
	}
	sort.Slice(resolved, func(i, j int) bool {
		if resolved[i].Name != resolved[j].Name {
			return resolved[i].Name < resolved[j].Name
		}
		if resolved[i].Locality != resolved[j].Locality {
			return resolved[i].Locality < resolved[j].Locality
		}
		return resolved[i].ImplementationVersion < resolved[j].ImplementationVersion
	})
	canonical, err := serializers.NewJSONSerializer().Serialize(resolved)
	if err != nil {
		return "", fmt.Errorf("canonicalize resolved toolset: %w", err)
	}
	return userevents.SHA256String(string(canonical)), nil
}

func agentPresentedToolSchemas(toolSpecs []model.ToolSpec) ([]byte, error) {
	log.Trace("agentPresentedToolSchemas")

	type presentedToolSpec struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	}
	presented := make([]presentedToolSpec, 0, len(toolSpecs))
	for _, tool := range toolSpecs {
		parameters := tool.Parameters
		if len(parameters) == 0 {
			parameters = json.RawMessage(`{}`)
		}
		presented = append(presented, presentedToolSpec{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parameters,
		})
	}
	return json.Marshal(presented)
}
