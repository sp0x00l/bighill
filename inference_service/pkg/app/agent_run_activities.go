package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func (u *inferenceUsecase) PrepareAgentRunActivity(ctx context.Context, input PrepareAgentRunActivityInput) (AgentRunWorkflowState, error) {
	log.Trace("InferenceUsecase PrepareAgentRunActivity")

	request := input.Request
	if request.AgentRunID == uuid.Nil {
		request.AgentRunID = request.RequestID
	}
	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	endpoint, err := u.endpointRepository.ReadEndpoint(ctx, request.OrgID, input.EndpointID)
	if err != nil {
		return AgentRunWorkflowState{}, err
	}
	if !endpoint.IsReady() {
		return AgentRunWorkflowState{}, domain.ErrModelNotReady.Extend("inference endpoint is not ready")
	}
	if endpoint.Mode != model.AgentEndpointModeAgent {
		return AgentRunWorkflowState{}, domain.ErrValidationFailed.Extend("agent workflow requires an agent endpoint")
	}
	spec, err := u.agentSpecRepository.ReadAgentSpecByHash(ctx, request.OrgID, endpoint.AgentSpecHash)
	if err != nil {
		return AgentRunWorkflowState{}, err
	}
	inferenceModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, endpoint.ModelID)
	if err != nil {
		return AgentRunWorkflowState{}, err
	}
	datasets, err := u.readGenerateDatasets(ctx, request.OrgID, endpoint.DatasetIDs)
	if err != nil {
		return AgentRunWorkflowState{}, err
	}
	readyDatasets := filterReadyRAGDatasets(ctx, datasets)
	if len(readyDatasets) == 0 {
		return AgentRunWorkflowState{}, domain.ErrDatasetNotReady.Extend("no endpoint datasets have materialized embeddings")
	}
	request.DatasetID = readyDatasets[0].DatasetID
	request.ModelID = endpoint.ModelID
	generationProtocol := strings.TrimSpace(inferenceModel.ServingProtocol.String())
	generationModel := strings.TrimSpace(inferenceModel.ServingModel)
	if inferenceModel.Status != model.ModelStatusReady {
		return AgentRunWorkflowState{}, domain.ErrModelNotReady.Extend("model is not ready")
	}
	if inferenceModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		inferenceModel, err = u.ensureServingModelLoaded(ctx, request.OrgID, inferenceModel)
		if err != nil {
			return AgentRunWorkflowState{}, err
		}
		generationProtocol = strings.TrimSpace(inferenceModel.ServingProtocol.String())
		generationModel = strings.TrimSpace(inferenceModel.ServingModel)
	}
	if u.generationAdapters[generationProtocol] == nil {
		return AgentRunWorkflowState{}, domain.ErrModelNotReady.Extend(fmt.Sprintf("serving protocol %q is not supported", generationProtocol))
	}
	session := &model.AgentSession{
		RunID:           request.AgentRunID,
		OrgID:           request.OrgID,
		UserID:          request.UserID,
		Endpoint:        endpoint,
		Spec:            spec,
		Model:           inferenceModel,
		Datasets:        readyDatasets,
		Messages:        agentInitialMessages(spec, request.QueryText),
		DecodingOptions: agentDecodingOptions(spec, request),
	}
	return AgentRunWorkflowState{
		Request:                   request,
		EndpointID:                endpoint.EndpointID,
		ModelID:                   endpoint.ModelID,
		AgentSpecHash:             spec.ContentHash,
		DatasetIDs:                datasetIDsFromModels(readyDatasets),
		MergeStrategy:             endpoint.MergeStrategy,
		Budgets:                   spec.Budgets,
		ToolBindings:              spec.ToolBindings,
		ServingProtocol:           generationProtocol,
		ServingModel:              generationModel,
		ServingTarget:             strings.TrimSpace(inferenceModel.ServingTarget),
		DecodingOptions:           session.DecodingOptions,
		TransientToolFailureCount: map[string]int{},
	}, nil
}

func (u *inferenceUsecase) GenerateAgentStepActivity(ctx context.Context, input GenerateAgentStepActivityInput) (GenerateAgentStepActivityOutput, error) {
	log.Trace("InferenceUsecase GenerateAgentStepActivity")

	generationProtocol := strings.TrimSpace(input.State.ServingProtocol)
	generator := u.generationAdapters[generationProtocol]
	if generator == nil {
		return GenerateAgentStepActivityOutput{}, domain.ErrModelNotReady.Extend(fmt.Sprintf("serving protocol %q is not supported", generationProtocol))
	}
	messages, err := u.agentWorkflowMessages(ctx, input.State, input.StepIndex)
	if err != nil {
		return GenerateAgentStepActivityOutput{}, err
	}
	toolSpecs, err := u.agentWorkflowToolSpecs(ctx, input.State)
	if err != nil {
		return GenerateAgentStepActivityOutput{}, err
	}
	promptEstimate := estimateAgentPromptTokens(messages, toolSpecs)
	if agentWorkflowTokenBudgetWouldExceed(input.State, promptEstimate) {
		return GenerateAgentStepActivityOutput{
			PromptTokenEstimate: promptEstimate,
			StopReason:          model.AgentStopReasonBudget.String(),
			ErrorMessage:        "agent reached its token budget before generation",
		}, nil
	}
	result, err := generator.Generate(ctx, model.GenerationRequest{
		RequestID:  input.State.Request.RequestID,
		Model:      agentWorkflowModel(input.State),
		Query:      input.State.Request.QueryText,
		Messages:   messages,
		Tools:      toolSpecs,
		ToolChoice: input.ToolChoice,
		Options:    input.Options,
	})
	if err != nil {
		return GenerateAgentStepActivityOutput{}, err
	}
	result.Options = input.Options
	return GenerateAgentStepActivityOutput{
		Result:              result,
		PromptTokenEstimate: promptEstimate,
		TokenUsage:          agentGenerationTokenUsage(result.Usage, messages, toolSpecs, result),
	}, nil
}

func (u *inferenceUsecase) RecordAgentStepActivity(ctx context.Context, input RecordAgentStepActivityInput) (uuid.UUID, error) {
	log.Trace("InferenceUsecase RecordAgentStepActivity")

	session, err := u.agentWorkflowSession(ctx, input.State)
	if err != nil {
		return uuid.Nil, err
	}
	toolSpecs, err := u.agentWorkflowToolSpecs(ctx, input.State)
	if err != nil {
		return uuid.Nil, err
	}
	return u.recordAgentStep(ctx, session, input.StepIndex, toolSpecs, input.GenerationResult)
}

func (u *inferenceUsecase) InvokeAgentToolActivity(ctx context.Context, input InvokeAgentToolActivityInput) (InvokeAgentToolActivityOutput, error) {
	log.Trace("InferenceUsecase InvokeAgentToolActivity")

	session, err := u.agentWorkflowSession(ctx, input.State)
	if err != nil {
		return InvokeAgentToolActivityOutput{}, err
	}
	toolResult, err := u.toolInvoker.Invoke(ctx, appToolInvocationContext(session), input.Call)
	if err != nil && toolResult.CallID == "" {
		toolResult = model.ToolResult{
			CallID:    input.Call.ID,
			Name:      input.Call.Name,
			Content:   err.Error(),
			IsError:   true,
			ErrorType: agentToolErrorType(err),
		}
	}
	if err != nil && toolResult.ErrorType == model.ToolErrorTypeUnknown {
		toolResult.IsError = true
		toolResult.ErrorType = agentToolErrorType(err)
		if strings.TrimSpace(toolResult.Content) == "" {
			toolResult.Content = err.Error()
		}
	}
	if toolResult.IsError && toolResult.ErrorType == model.ToolErrorTypeUnknown {
		toolResult.ErrorType = model.ToolErrorTypePermanent
	}
	if recordErr := u.recordAgentToolInvocation(ctx, session, input.StepID, input.CallKey, input.Call, toolResult); recordErr != nil {
		return InvokeAgentToolActivityOutput{}, recordErr
	}
	return InvokeAgentToolActivityOutput{
		IsError:       toolResult.IsError,
		ErrorType:     toolResult.ErrorType.String(),
		TokenEstimate: toolResult.TokenEstimate,
	}, nil
}

func (u *inferenceUsecase) CompleteAgentRunActivity(ctx context.Context, input CompleteAgentRunActivityInput) error {
	log.Trace("InferenceUsecase CompleteAgentRunActivity")

	session, err := u.agentWorkflowSession(ctx, input.State)
	if err != nil {
		return err
	}
	recorded, err := u.recordAgentRun(ctx, session, model.AgentRunStatusCompleted, model.AgentStopReasonFinalAnswer)
	if err != nil {
		return err
	}
	return u.recordAgentWorkflowInferenceRequest(ctx, input.State, recorded.StartedAt, input.Answer, model.InferenceRequestStatusCompleted, "")
}

func (u *inferenceUsecase) FailAgentRunActivity(ctx context.Context, input FailAgentRunActivityInput) error {
	log.Trace("InferenceUsecase FailAgentRunActivity")

	session, err := u.agentWorkflowSession(ctx, input.State)
	if err != nil {
		return err
	}
	reason := model.AgentStopReasonRuntimeError
	if strings.TrimSpace(input.StopReason) != "" {
		parsed, parseErr := model.ToAgentStopReason(input.StopReason)
		if parseErr != nil {
			return parseErr
		}
		reason = parsed
	}
	if reason == model.AgentStopReasonUnknown {
		reason = model.AgentStopReasonRuntimeError
	}
	recorded, err := u.recordAgentRun(ctx, session, model.AgentRunStatusFailed, reason)
	if err != nil {
		return err
	}
	message := strings.TrimSpace(input.ErrorMessage)
	if message == "" {
		message = reason.String()
	}
	return u.recordAgentWorkflowInferenceRequest(ctx, input.State, recorded.StartedAt, "", model.InferenceRequestStatusFailed, message)
}

func (u *inferenceUsecase) recordAgentWorkflowInferenceRequest(ctx context.Context, state AgentRunWorkflowState, startedAt time.Time, answer string, status model.InferenceRequestStatus, errorMessage string) error {
	log.Trace("InferenceUsecase recordAgentWorkflowInferenceRequest")

	datasets, err := u.agentWorkflowDatasets(ctx, state)
	if err != nil {
		return err
	}
	if len(datasets) == 0 {
		return domain.ErrValidationFailed.Extend("agent workflow state missing dataset or model")
	}
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	contexts, err := u.agentWorkflowContexts(ctx, state)
	if err != nil {
		return err
	}
	modelForRecord := agentWorkflowModel(state)
	return u.recordInferenceRequest(ctx, state.Request, datasets[0], modelForRecord, contexts, "", answer, startedAt, state.ServingProtocol, state.ServingModel, status, errorMessage)
}

func (u *inferenceUsecase) agentWorkflowSession(ctx context.Context, state AgentRunWorkflowState) (*model.AgentSession, error) {
	datasets, err := u.agentWorkflowDatasets(ctx, state)
	if err != nil {
		return nil, err
	}
	return &model.AgentSession{
		RunID:             state.Request.AgentRunID,
		OrgID:             state.Request.OrgID,
		UserID:            state.Request.UserID,
		Endpoint:          agentWorkflowEndpoint(state),
		Spec:              agentWorkflowSpec(state),
		Model:             agentWorkflowModel(state),
		Datasets:          datasets,
		Messages:          nil,
		ResolvedToolSpecs: nil,
		DecodingOptions:   state.DecodingOptions,
		TotalTokens:       state.TotalTokens,
	}, nil
}

func (u *inferenceUsecase) agentWorkflowMessages(ctx context.Context, state AgentRunWorkflowState, stepIndex int) ([]model.ChatMessage, error) {
	log.Trace("InferenceUsecase agentWorkflowMessages")

	spec, err := u.agentSpecRepository.ReadAgentSpecByHash(ctx, state.Request.OrgID, state.AgentSpecHash)
	if err != nil {
		return nil, err
	}
	messages := agentInitialMessages(spec, state.Request.QueryText)
	trajectory, err := u.trajectoryRepository.ReadAgentTrajectory(ctx, state.Request.OrgID, state.Request.AgentRunID)
	if err != nil {
		return nil, err
	}
	invocationsByStep := agentWorkflowInvocationsByStep(trajectory.ToolInvocations)
	for _, step := range trajectory.Steps {
		if step == nil || step.StepIndex >= stepIndex {
			continue
		}
		generation, err := agentGenerationResultFromJSON(step.GenerationResult)
		if err != nil {
			return nil, fmt.Errorf("parse generation result for step %d: %w", step.StepIndex, err)
		}
		messages = append(messages, model.ChatMessage{
			Role:      model.ChatMessageRoleAssistant,
			Content:   generation.Content,
			ToolCalls: generation.ToolCalls,
		})
		for _, invocation := range invocationsByStep[step.StepID] {
			if invocation == nil {
				continue
			}
			toolResult, err := agentToolResultFromJSON(invocation.Result)
			if err != nil {
				return nil, fmt.Errorf("parse tool result for step %d: %w", step.StepIndex, err)
			}
			messages = append(messages, model.ChatMessage{
				Role:       model.ChatMessageRoleTool,
				Content:    toolResult.Content,
				ToolCallID: toolResult.CallID,
				Name:       invocation.ToolName,
			})
		}
	}
	return messages, nil
}

func (u *inferenceUsecase) agentWorkflowToolSpecs(ctx context.Context, state AgentRunWorkflowState) ([]model.ToolSpec, error) {
	log.Trace("InferenceUsecase agentWorkflowToolSpecs")

	session, err := u.agentWorkflowSession(ctx, state)
	if err != nil {
		return nil, err
	}
	return u.toolInvoker.Available(ctx, appToolResolutionContext(session), state.ToolBindings)
}

func (u *inferenceUsecase) agentWorkflowContexts(ctx context.Context, state AgentRunWorkflowState) ([]model.RetrievedContext, error) {
	log.Trace("InferenceUsecase agentWorkflowContexts")

	trajectory, err := u.trajectoryRepository.ReadAgentTrajectory(ctx, state.Request.OrgID, state.Request.AgentRunID)
	if err != nil {
		return nil, err
	}
	contexts := []model.RetrievedContext{}
	for _, invocation := range trajectory.ToolInvocations {
		if invocation == nil {
			continue
		}
		toolResult, err := agentToolResultFromJSON(invocation.Result)
		if err != nil {
			return nil, fmt.Errorf("parse tool result contexts: %w", err)
		}
		contexts = append(contexts, toolResult.Contexts...)
	}
	return contexts, nil
}

func agentWorkflowInvocationsByStep(invocations []*model.AgentToolInvocation) map[uuid.UUID][]*model.AgentToolInvocation {
	byStep := map[uuid.UUID][]*model.AgentToolInvocation{}
	for _, invocation := range invocations {
		if invocation == nil {
			continue
		}
		byStep[invocation.StepID] = append(byStep[invocation.StepID], invocation)
	}
	return byStep
}

func (u *inferenceUsecase) agentWorkflowDatasets(ctx context.Context, state AgentRunWorkflowState) ([]*model.InferenceDataset, error) {
	datasets, err := u.readGenerateDatasets(ctx, state.Request.OrgID, state.DatasetIDs)
	if err != nil {
		return nil, err
	}
	return filterReadyRAGDatasets(ctx, datasets), nil
}

func agentWorkflowEndpoint(state AgentRunWorkflowState) *model.PublishedEndpoint {
	return &model.PublishedEndpoint{
		EndpointID:    state.EndpointID,
		OrgID:         state.Request.OrgID,
		ModelID:       state.ModelID,
		Mode:          model.AgentEndpointModeAgent,
		AgentSpecHash: state.AgentSpecHash,
		DatasetIDs:    state.DatasetIDs,
		MergeStrategy: state.MergeStrategy,
		Status:        model.PublishedEndpointStatusReady,
	}
}

func agentWorkflowSpec(state AgentRunWorkflowState) *model.AgentSpec {
	return &model.AgentSpec{
		OrgID:        state.Request.OrgID,
		ContentHash:  state.AgentSpecHash,
		ToolBindings: state.ToolBindings,
		Budgets:      state.Budgets,
	}
}

func agentWorkflowModel(state AgentRunWorkflowState) *model.InferenceModel {
	servingProtocol, _ := model.ToServingProtocol(state.ServingProtocol)
	return &model.InferenceModel{
		ModelID:         state.ModelID,
		UserID:          state.Request.UserID,
		OrgID:           state.Request.OrgID,
		ServingProtocol: servingProtocol,
		ServingModel:    state.ServingModel,
		ServingTarget:   state.ServingTarget,
	}
}
