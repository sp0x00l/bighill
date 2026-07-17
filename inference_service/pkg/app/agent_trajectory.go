package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

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

	agentTrajectorySourceService = "inference_service"
	agentTrajectoryReadSpanName  = "agent.trajectory.read"

	agentToolInvocationIDNamespace = "agent_tool_invocation:"

	agentRunAPIHrefPrefix = "/v1/inference/agent-runs/"
	agentRunUIHrefPrefix  = "/agent-runs/"

	agentRunStartedTitle   = "Agent run started"
	agentRunStartedMessage = "The agent run has started."
	agentRunCompletedTitle = "Agent run completed"
	agentRunCompletedMsg   = "The agent run completed."
	agentRunFailedTitle    = "Agent run failed"
	agentRunFailedMessage  = "The agent run failed."

	agentStepCompletedTitle   = "Agent step completed"
	agentStepCompletedMessage = "The agent completed a reasoning step."

	agentToolResultTitle   = "Agent tool result recorded"
	agentToolResultMessage = "The agent recorded a tool result."

	agentEventActionViewRun = "View run"

	agentEventPartStep = "step"
	agentEventPartTool = "tool"

	agentToolResultStateCompleted = "COMPLETED"
	agentToolResultStateFailed    = "FAILED"

	agentEventMetadataEndpointID   = "endpoint_id"
	agentEventMetadataStepID       = "step_id"
	agentEventMetadataStepIndex    = "step_index"
	agentEventMetadataInvocationID = "invocation_id"
	agentEventMetadataToolName     = "tool_name"
	agentEventMetadataErrorType    = "error_type"

	agentTrajectoryAttrOrgID = "org_id"
	agentTrajectoryAttrRunID = "run_id"
)

func (u *inferenceUsecase) recordAgentRun(ctx context.Context, session *model.AgentSession, status model.AgentRunStatus, stopReason model.AgentStopReason) (*model.AgentRun, error) {
	log.Trace("InferenceUsecase recordAgentRun")

	decodingParams, err := agentDecodingOptionsJSON(session.DecodingOptions)
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
	generationResultJSON, err := agentGenerationResultJSON(generationResult)
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

func (u *inferenceUsecase) recordAgentToolInvocation(ctx context.Context, session *model.AgentSession, stepID uuid.UUID, callKey string, call model.ToolCall, result model.ToolResult) error {
	log.Trace("InferenceUsecase recordAgentToolInvocation")

	resultJSON, err := agentToolResultJSON(result)
	if err != nil {
		return fmt.Errorf("marshal tool result: %w", err)
	}
	errorType := result.ErrorType
	invocationID := result.InvocationID
	if invocationID == uuid.Nil {
		invocationID = deterministicAgentToolInvocationID(session.RunID, stepID, callKey)
	}
	invocation := &model.AgentToolInvocation{
		InvocationID:    invocationID,
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

func deterministicAgentToolInvocationID(runID uuid.UUID, stepID uuid.UUID, toolCallKey string) uuid.UUID {
	log.Trace("deterministicAgentToolInvocationID")

	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(agentToolInvocationIDNamespace+runID.String()+":"+stepID.String()+":"+strings.TrimSpace(toolCallKey)))
}

func (u *inferenceUsecase) ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) (trajectory *model.AgentTrajectory, err error) {
	log.Trace("InferenceUsecase ReadAgentTrajectory")

	ctx = ctxutil.WithOrgID(ctx, orgID)
	ctx, span := startInferenceSpan(ctx, agentTrajectoryReadSpanName,
		attribute.String(agentTrajectoryAttrOrgID, orgID.String()),
		attribute.String(agentTrajectoryAttrRunID, runID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.trajectoryRepository.ReadAgentTrajectory(ctx, orgID, runID)
}

func (u *inferenceUsecase) ReapExpiredAgentRuns(ctx context.Context, grace time.Duration) (int64, error) {
	log.Trace("InferenceUsecase ReapExpiredAgentRuns")

	return u.trajectoryRepository.FailExpiredAgentRuns(ctx, grace)
}

func (u *inferenceUsecase) publishAgentRunUserEvent(ctx context.Context, run *model.AgentRun) {
	log.Trace("InferenceUsecase publishAgentRunUserEvent")

	if run == nil {
		return
	}
	eventType := userevents.EventTypeAgentRunStarted
	severity := userevents.SeverityInfo
	title := agentRunStartedTitle
	message := agentRunStartedMessage
	switch run.Status {
	case model.AgentRunStatusCompleted:
		eventType = userevents.EventTypeAgentRunCompleted
		severity = userevents.SeveritySuccess
		title = agentRunCompletedTitle
		message = agentRunCompletedMsg
	case model.AgentRunStatusFailed:
		eventType = userevents.EventTypeAgentRunFailed
		severity = userevents.SeverityError
		title = agentRunFailedTitle
		message = agentRunFailedMessage
	}
	event := userevents.Event{
		EventID: userevents.DeterministicEventID(
			userevents.ResourceTypeAgentRun,
			run.RunID.String(),
			run.Status.String(),
			run.StopReason.String(),
		),
		SourceService:      agentTrajectorySourceService,
		EventType:          eventType,
		Severity:           severity,
		RequiredPermission: authz.PermissionInferenceAgentRunsRead,
		UserID:             optionalAgentEventUUID(run.UserID),
		Resource:           userevents.NewResource(userevents.ResourceTypeAgentRun, run.RunID, "", agentRunAPIHref(run.RunID)),
		Status: userevents.Status{
			State: run.Status.String(),
			Phase: userevents.StatusPhaseAgent,
		},
		Title:       title,
		Message:     message,
		ActionLabel: agentEventActionViewRun,
		ActionHref:  agentRunUIHref(run.RunID),
		Metadata: map[string]string{
			agentEventMetadataEndpointID: run.EndpointID.String(),
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
			agentEventPartStep,
			fmt.Sprintf("%d", step.StepIndex),
		),
		SourceService:      agentTrajectorySourceService,
		EventType:          userevents.EventTypeAgentStepCompleted,
		Severity:           userevents.SeverityInfo,
		RequiredPermission: authz.PermissionInferenceAgentRunsRead,
		UserID:             optionalAgentEventUUID(session.UserID),
		Resource:           userevents.NewResource(userevents.ResourceTypeAgentRun, step.RunID, "", agentRunAPIHref(step.RunID)),
		Status:             userevents.Status{State: string(step.FinishReason), Phase: userevents.StatusPhaseAgent},
		Title:              agentStepCompletedTitle,
		Message:            agentStepCompletedMessage,
		ActionLabel:        agentEventActionViewRun,
		ActionHref:         agentRunUIHref(step.RunID),
		Metadata: map[string]string{
			agentEventMetadataStepID:    step.StepID.String(),
			agentEventMetadataStepIndex: fmt.Sprintf("%d", step.StepIndex),
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
	state := agentToolResultStateCompleted
	if invocation.ErrorType != model.ToolErrorTypeUnknown {
		severity = userevents.SeverityWarning
		state = agentToolResultStateFailed
	}
	event := userevents.Event{
		EventID: userevents.DeterministicEventID(
			userevents.ResourceTypeAgentRun,
			invocation.RunID.String(),
			agentEventPartTool,
			invocation.InvocationID.String(),
		),
		SourceService:      agentTrajectorySourceService,
		EventType:          userevents.EventTypeAgentToolResult,
		Severity:           severity,
		RequiredPermission: authz.PermissionInferenceAgentRunsRead,
		UserID:             optionalAgentEventUUID(session.UserID),
		Resource:           userevents.NewResource(userevents.ResourceTypeAgentRun, invocation.RunID, "", agentRunAPIHref(invocation.RunID)),
		Status:             userevents.Status{State: state, Phase: userevents.StatusPhaseAgent},
		Title:              agentToolResultTitle,
		Message:            agentToolResultMessage,
		ActionLabel:        agentEventActionViewRun,
		ActionHref:         agentRunUIHref(invocation.RunID),
		Metadata: map[string]string{
			agentEventMetadataStepID:       invocation.StepID.String(),
			agentEventMetadataInvocationID: invocation.InvocationID.String(),
			agentEventMetadataToolName:     invocation.ToolName,
			agentEventMetadataErrorType:    invocation.ErrorType.String(),
		},
	}
	if err := u.userEventPublisher.Publish(ctx, event); err != nil {
		userevents.LogPublishFailure(ctx, err, event)
	}
}

func agentRunAPIHref(runID uuid.UUID) string {
	log.Trace("agentRunAPIHref")

	return agentRunAPIHrefPrefix + runID.String()
}

func agentRunUIHref(runID uuid.UUID) string {
	log.Trace("agentRunUIHref")

	return agentRunUIHrefPrefix + runID.String()
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

func agentDecodingOptionsJSON(options model.GenerationOptions) ([]byte, error) {
	log.Trace("agentDecodingOptionsJSON")

	return json.Marshal(agentGenerationOptionsDTO{
		Temperature:     options.Temperature,
		TopP:            options.TopP,
		Seed:            options.Seed,
		MaxOutputTokens: options.MaxOutputTokens,
	})
}

func agentGenerationResultJSON(result model.GenerationResult) ([]byte, error) {
	log.Trace("agentGenerationResultJSON")

	toolCalls := make([]agentToolCallDTO, 0, len(result.ToolCalls))
	for _, call := range result.ToolCalls {
		toolCalls = append(toolCalls, agentToolCallDTO{
			ID:        call.ID,
			Name:      call.Name,
			Arguments: call.Arguments,
		})
	}
	return json.Marshal(agentGenerationResultDTO{
		Content:      result.Content,
		ToolCalls:    toolCalls,
		FinishReason: result.FinishReason,
		Usage: agentTokenUsageDTO{
			PromptTokens:     result.Usage.PromptTokens,
			CompletionTokens: result.Usage.CompletionTokens,
			TotalTokens:      result.Usage.TotalTokens,
		},
		Options: agentGenerationOptionsDTO{
			Temperature:     result.Options.Temperature,
			TopP:            result.Options.TopP,
			Seed:            result.Options.Seed,
			MaxOutputTokens: result.Options.MaxOutputTokens,
		},
	})
}

func agentGenerationResultFromJSON(value []byte) (model.GenerationResult, error) {
	log.Trace("agentGenerationResultFromJSON")

	dto := agentGenerationResultDTO{}
	if err := json.Unmarshal(value, &dto); err != nil {
		return model.GenerationResult{}, err
	}
	toolCalls := make([]model.ToolCall, 0, len(dto.ToolCalls))
	for _, call := range dto.ToolCalls {
		toolCalls = append(toolCalls, model.ToolCall{
			ID:        call.ID,
			Name:      call.Name,
			Arguments: call.Arguments,
		})
	}
	return model.GenerationResult{
		Content:      dto.Content,
		ToolCalls:    toolCalls,
		FinishReason: dto.FinishReason,
		Usage: model.TokenUsage{
			PromptTokens:     dto.Usage.PromptTokens,
			CompletionTokens: dto.Usage.CompletionTokens,
			TotalTokens:      dto.Usage.TotalTokens,
		},
		Options: model.GenerationOptions{
			Temperature:     dto.Options.Temperature,
			TopP:            dto.Options.TopP,
			Seed:            dto.Options.Seed,
			MaxOutputTokens: dto.Options.MaxOutputTokens,
		},
	}, nil
}

func agentToolResultJSON(result model.ToolResult) ([]byte, error) {
	log.Trace("agentToolResultJSON")

	return json.Marshal(agentToolResultDTO{
		InvocationID:    result.InvocationID,
		CallID:          result.CallID,
		Name:            result.Name,
		Content:         result.Content,
		Contexts:        result.Contexts,
		IsError:         result.IsError,
		ErrorType:       result.ErrorType.String(),
		ToolImplVersion: result.ToolImplVersion,
		TokenEstimate:   result.TokenEstimate,
	})
}

func agentToolResultFromJSON(value []byte) (model.ToolResult, error) {
	log.Trace("agentToolResultFromJSON")

	dto := agentToolResultDTO{}
	if err := json.Unmarshal(value, &dto); err != nil {
		return model.ToolResult{}, err
	}
	errorType := model.ToolErrorTypeUnknown
	if strings.TrimSpace(dto.ErrorType) != "" && strings.TrimSpace(dto.ErrorType) != model.ToolErrorTypeUnknown.String() {
		parsed, err := model.ToToolErrorType(dto.ErrorType)
		if err != nil {
			return model.ToolResult{}, err
		}
		errorType = parsed
	}
	return model.ToolResult{
		InvocationID:    dto.InvocationID,
		CallID:          dto.CallID,
		Name:            dto.Name,
		Content:         dto.Content,
		Contexts:        dto.Contexts,
		IsError:         dto.IsError,
		ErrorType:       errorType,
		ToolImplVersion: dto.ToolImplVersion,
		TokenEstimate:   dto.TokenEstimate,
	}, nil
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

type agentGenerationResultDTO struct {
	Content      string                       `json:"content"`
	ToolCalls    []agentToolCallDTO           `json:"tool_calls,omitempty"`
	FinishReason model.GenerationFinishReason `json:"finish_reason"`
	Usage        agentTokenUsageDTO           `json:"usage"`
	Options      agentGenerationOptionsDTO    `json:"options"`
}

type agentToolCallDTO struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type agentTokenUsageDTO struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type agentGenerationOptionsDTO struct {
	Temperature     float64 `json:"temperature"`
	TopP            float64 `json:"top_p"`
	Seed            int64   `json:"seed,omitempty"`
	MaxOutputTokens int     `json:"max_output_tokens"`
}

type agentToolResultDTO struct {
	InvocationID    uuid.UUID                `json:"invocation_id,omitempty"`
	CallID          string                   `json:"call_id"`
	Name            string                   `json:"name"`
	Content         string                   `json:"content"`
	Contexts        []model.RetrievedContext `json:"contexts,omitempty"`
	IsError         bool                     `json:"is_error"`
	ErrorType       string                   `json:"error_type,omitempty"`
	ToolImplVersion string                   `json:"tool_impl_version,omitempty"`
	TokenEstimate   int                      `json:"token_estimate,omitempty"`
}
