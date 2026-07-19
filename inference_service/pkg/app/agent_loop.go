package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	agentToolChoiceRequired          = "required"
	agentTransientToolFailureRetries = 1
)

type agentServingSelection struct {
	BaseModel       *model.InferenceModel
	AdapterModel    *model.InferenceModel
	ServingModelID  uuid.UUID
	Protocol        string
	BaseModelName   string
	Target          string
	LoraName        string
	AdapterURI      string
	EffectiveBaseID string
}

func (s agentServingSelection) generationModelName() string {
	if value := strings.TrimSpace(s.LoraName); value != "" {
		return value
	}
	return strings.TrimSpace(s.BaseModelName)
}

func (u *inferenceUsecase) resolveAgentServing(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint, spec *model.AgentSpec) (agentServingSelection, model.GenerateRequest, error) {
	log.Trace("InferenceUsecase resolveAgentServing")

	if endpoint == nil {
		return agentServingSelection{}, request, domain.ErrValidationFailed.Extend("agent endpoint is required")
	}
	if spec == nil {
		return agentServingSelection{}, request, domain.ErrValidationFailed.Extend("agent spec is required")
	}
	baseModelID := endpoint.ModelID
	if baseModelID == uuid.Nil {
		return agentServingSelection{}, request, domain.ErrValidationFailed.Extend("agent endpoint model is required")
	}
	if spec.ModelID != uuid.Nil && spec.ModelID != baseModelID {
		return agentServingSelection{}, request, domain.ErrValidationFailed.Extend("agent spec model does not match endpoint model")
	}
	baseModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, baseModelID)
	if err != nil {
		return agentServingSelection{}, request, err
	}
	if baseModel.Status != model.ModelStatusReady {
		return agentServingSelection{}, request, domain.ErrModelNotReady.Extend("model is not ready")
	}
	if baseModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		baseModel, err = u.ensureServingModelLoaded(ctx, request.OrgID, baseModel)
		if err != nil {
			return agentServingSelection{}, request, err
		}
	}
	effectiveBaseID := strings.TrimSpace(baseModel.EffectiveBaseID)
	if effectiveBaseID == "" {
		return agentServingSelection{}, request, domain.ErrModelNotReady.Extend("model effective base is not available")
	}
	selection := agentServingSelection{
		BaseModel:       baseModel,
		Protocol:        strings.TrimSpace(baseModel.ServingProtocol.String()),
		BaseModelName:   strings.TrimSpace(baseModel.ServingModel),
		Target:          strings.TrimSpace(baseModel.ServingTarget),
		EffectiveBaseID: effectiveBaseID,
	}
	servingModelID := uuid.Nil
	if endpoint.ServingModelID != uuid.Nil && endpoint.ServingModelID != baseModelID {
		servingModelID = endpoint.ServingModelID
	}
	if request.ServingModelID != uuid.Nil && request.ServingModelID != baseModelID {
		servingModelID = request.ServingModelID
	}
	request.ModelID = baseModelID
	request.ServingModelID = servingModelID
	if servingModelID == uuid.Nil {
		return selection, request, nil
	}
	adapterModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, servingModelID)
	if err != nil {
		return agentServingSelection{}, request, err
	}
	if adapterModel.ModelKind != model.ModelKindFineTuned {
		return agentServingSelection{}, request, domain.ErrValidationFailed.Extend("agent serving model must be a fine-tuned adapter")
	}
	if adapterModel.Status != model.ModelStatusReady {
		return agentServingSelection{}, request, domain.ErrModelNotReady.Extend("adapter model is not ready")
	}
	adapterBaseID := strings.TrimSpace(adapterModel.EffectiveBaseID)
	if adapterBaseID == "" {
		return agentServingSelection{}, request, domain.ErrModelNotReady.Extend("adapter effective base is not available")
	}
	if adapterBaseID != effectiveBaseID {
		return agentServingSelection{}, request, domain.ErrValidationFailed.Extend("adapter effective base does not match endpoint model")
	}
	if strings.TrimSpace(adapterModel.AdapterURI) == "" {
		return agentServingSelection{}, request, domain.ErrModelNotReady.Extend("adapter uri is not available")
	}
	if adapterModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		adapterModel, err = u.ensureServingModelLoaded(ctx, request.OrgID, adapterModel)
		if err != nil {
			return agentServingSelection{}, request, err
		}
	}
	loraName := strings.TrimSpace(adapterModel.ServingModel)
	if loraName == "" {
		return agentServingSelection{}, request, domain.ErrModelNotReady.Extend("adapter serving name is not available")
	}
	selection.AdapterModel = adapterModel
	selection.ServingModelID = servingModelID
	selection.LoraName = loraName
	selection.AdapterURI = strings.TrimSpace(adapterModel.AdapterURI)
	request.LoraName = selection.LoraName
	request.AdapterURI = selection.AdapterURI
	return selection, request, nil
}

func (u *inferenceUsecase) generateAgent(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint) (response *model.GenerateResponse, err error) {
	log.Trace("InferenceUsecase generateAgent")

	ctx, span := startInferenceSpan(ctx, "agent.generate",
		attribute.String("request_id", request.RequestID.String()),
		attribute.String("endpoint_id", endpoint.EndpointID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	startedAt := time.Now()
	spec, err := u.agentSpecRepository.ReadAgentSpecByHash(ctx, request.OrgID, endpoint.AgentSpecHash)
	if err != nil {
		return nil, err
	}
	serving, request, err := u.resolveAgentServing(ctx, request, endpoint, spec)
	if err != nil {
		return nil, err
	}
	inferenceModel := serving.BaseModel
	datasets, err := u.readGenerateDatasets(ctx, request.OrgID, endpoint.DatasetIDs)
	if err != nil {
		return nil, err
	}
	readyDatasets := filterReadyRAGDatasets(ctx, datasets)
	if len(readyDatasets) == 0 {
		return nil, domain.ErrDatasetNotReady.Extend("no endpoint datasets have materialized embeddings")
	}
	dataset := readyDatasets[0]
	request.DatasetID = dataset.DatasetID

	generationProtocol := serving.Protocol
	generationModel := serving.generationModelName()
	generator := u.generationAdapters[generationProtocol]
	if generator == nil {
		err = domain.ErrModelNotReady.Extend(fmt.Sprintf("serving protocol %q is not supported", generationProtocol))
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, nil, "", "", startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	result, err := u.runAgentLoop(ctx, request, endpoint, spec, inferenceModel, readyDatasets, generator)
	if err != nil {
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, result.Contexts, "", result.Answer, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	response = &model.GenerateResponse{
		RequestID:             request.RequestID,
		AgentRunID:            result.RunID,
		OrgID:                 request.OrgID,
		DatasetID:             dataset.DatasetID,
		DatasetIDs:            datasetIDsFromModels(readyDatasets),
		ModelID:               inferenceModel.ModelID,
		QueryText:             request.QueryText,
		Answer:                result.Answer,
		Contexts:              result.Contexts,
		PromptStrategyVersion: spec.ContentHash,
		GenerationProtocol:    generationProtocol,
		GenerationModel:       generationModel,
		RAGMergeStrategy:      endpoint.MergeStrategy,
	}
	if serving.ServingModelID != uuid.Nil {
		response.ModelID = serving.ServingModelID
	}
	if err := u.recordInferenceRequest(ctx, request, dataset, inferenceModel, result.Contexts, "", result.Answer, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusCompleted, ""); err != nil {
		return nil, err
	}
	return response, nil
}

func (u *inferenceUsecase) startAgentRunWorkflow(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint) (*model.GenerateResponse, error) {
	log.Trace("InferenceUsecase startAgentRunWorkflow")

	return u.startAgentRunWorkflowWithSpec(ctx, request, endpoint, endpoint.AgentSpecHash)
}

func (u *inferenceUsecase) startAgentRunWorkflowWithSpec(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint, agentSpecHash string) (*model.GenerateResponse, error) {
	log.Trace("InferenceUsecase startAgentRunWorkflowWithSpec")

	request.AgentRunID = request.RequestID
	spec, err := u.agentSpecRepository.ReadAgentSpecByHash(ctx, request.OrgID, agentSpecHash)
	if err != nil {
		return nil, err
	}
	serving, request, err := u.resolveAgentServing(ctx, request, endpoint, spec)
	if err != nil {
		return nil, err
	}
	datasets, err := u.readGenerateDatasets(ctx, request.OrgID, endpoint.DatasetIDs)
	if err != nil {
		return nil, err
	}
	readyDatasets := filterReadyRAGDatasets(ctx, datasets)
	if len(readyDatasets) == 0 {
		return nil, domain.ErrDatasetNotReady.Extend("no endpoint datasets have materialized embeddings")
	}
	session := &model.AgentSession{
		RunID:           request.AgentRunID,
		OrgID:           request.OrgID,
		UserID:          request.UserID,
		Endpoint:        agentEndpointWithAdapter(endpoint, serving.ServingModelID),
		Spec:            spec,
		Model:           serving.BaseModel,
		Datasets:        readyDatasets,
		DataSnapshotSet: agentDataSnapshotSet(readyDatasets),
		LoraName:        serving.LoraName,
		AdapterURI:      serving.AdapterURI,
		Messages:        agentInitialMessages(spec, request.QueryText),
		DecodingOptions: agentDecodingOptions(spec, request),
	}
	toolSpecs, err := u.toolInvoker.Available(ctx, appToolResolutionContext(session), spec.ToolBindings)
	if err != nil {
		return nil, err
	}
	session.ResolvedToolSpecs = toolSpecs
	session.ToolsetHash, err = agentToolsetHash(toolSpecs)
	if err != nil {
		return nil, err
	}
	if _, err := u.recordAgentRun(ctx, session, model.AgentRunStatusRunning, model.AgentStopReasonUnknown); err != nil {
		return nil, err
	}
	input := AgentRunWorkflowInput{
		EndpointID:    endpoint.EndpointID,
		AgentSpecHash: spec.ContentHash,
		Request:       request,
		WallMs:        spec.Budgets.WallMs,
	}
	if err := u.agentRunWorkflowStarter.StartAgentRunWorkflow(ctx, input); err != nil {
		_, _ = u.recordAgentRun(ctx, session, model.AgentRunStatusFailed, model.AgentStopReasonRuntimeError)
		return nil, err
	}
	return &model.GenerateResponse{
		RequestID:             request.RequestID,
		AgentRunID:            request.AgentRunID,
		Accepted:              true,
		OrgID:                 request.OrgID,
		DatasetID:             readyDatasets[0].DatasetID,
		DatasetIDs:            datasetIDsFromModels(readyDatasets),
		ModelID:               agentSelectedModelID(serving),
		QueryText:             request.QueryText,
		PromptStrategyVersion: spec.ContentHash,
		RAGMergeStrategy:      endpoint.MergeStrategy,
	}, nil
}

func agentEndpointWithAdapter(endpoint *model.PublishedEndpoint, servingModelID uuid.UUID) *model.PublishedEndpoint {
	if endpoint == nil {
		return endpoint
	}
	record := *endpoint
	record.ServingModelID = servingModelID
	return &record
}

func agentSelectedModelID(serving agentServingSelection) uuid.UUID {
	if serving.ServingModelID != uuid.Nil {
		return serving.ServingModelID
	}
	if serving.BaseModel != nil {
		return serving.BaseModel.ModelID
	}
	return uuid.Nil
}

func (u *inferenceUsecase) StartAgentEvalRun(ctx context.Context, endpointID uuid.UUID, agentSpecHash string, request model.GenerateRequest) (*model.GenerateResponse, error) {
	log.Trace("InferenceUsecase StartAgentEvalRun")

	endpoint, err := u.endpointRepository.ReadEndpoint(ctx, request.OrgID, endpointID)
	if err != nil {
		return nil, err
	}
	if endpoint.Mode != model.AgentEndpointModeAgent {
		return nil, domain.ErrValidationFailed.Extend("agent eval requires an agent endpoint")
	}
	return u.startAgentRunWorkflowWithSpec(ctx, request, endpoint, agentSpecHash)
}

func (u *inferenceUsecase) runAgentLoop(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint, spec *model.AgentSpec, inferenceModel *model.InferenceModel, datasets []*model.InferenceDataset, generator GenerationAdapter) (model.AgentResult, error) {
	log.Trace("InferenceUsecase runAgentLoop")

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(spec.Budgets.WallMs)*time.Millisecond)
	defer cancel()

	session := &model.AgentSession{
		RunID:           request.AgentRunID,
		OrgID:           request.OrgID,
		UserID:          request.UserID,
		Endpoint:        endpoint,
		Spec:            spec,
		Model:           inferenceModel,
		Datasets:        datasets,
		DataSnapshotSet: agentDataSnapshotSet(datasets),
		LoraName:        request.LoraName,
		AdapterURI:      request.AdapterURI,
		Messages:        agentInitialMessages(spec, request.QueryText),
		DecodingOptions: agentDecodingOptions(spec, request),
	}
	toolSpecs, err := u.toolInvoker.Available(runCtx, appToolResolutionContext(session), spec.ToolBindings)
	if err != nil {
		return model.AgentResult{}, err
	}
	session.ResolvedToolSpecs = toolSpecs
	session.ToolsetHash, err = agentToolsetHash(toolSpecs)
	if err != nil {
		return model.AgentResult{}, err
	}
	if session.RunID == uuid.Nil {
		run, err := u.recordAgentRun(ctx, session, model.AgentRunStatusRunning, model.AgentStopReasonUnknown)
		if err != nil {
			return model.AgentResult{}, err
		}
		session.RunID = run.RunID
	}
	maxSteps := spec.Budgets.MaxSteps
	var contexts []model.RetrievedContext
	lastToolCallSignature := ""
	repeatedToolCallCount := 0
	transientToolFailures := map[string]int{}
	for step := 0; step < maxSteps; step++ {
		if err := runCtx.Err(); err != nil {
			return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: model.AgentStopReasonDeadline, Steps: step}, u.failAgentRun(ctx, session, model.AgentStopReasonDeadline, err)
		}
		toolChoice := agentToolChoice(spec.ToolBindings, step)
		generationOptions := agentStepGenerationOptions(session)
		if agentTokenBudgetWouldExceed(session, estimateAgentPromptTokens(session.Messages, toolSpecs)) {
			err := domain.ErrGenerationFailed.Extend("agent reached its token budget before generation")
			return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: model.AgentStopReasonBudget, Steps: step}, u.failAgentRun(ctx, session, model.AgentStopReasonBudget, err)
		}
		gen, err := generator.Generate(runCtx, model.GenerationRequest{
			RequestID:  request.RequestID,
			Model:      inferenceModel,
			Query:      request.QueryText,
			Messages:   session.Messages,
			Tools:      toolSpecs,
			ToolChoice: toolChoice,
			Options:    generationOptions,
			LoraName:   session.LoraName,
			AdapterURI: session.AdapterURI,
		})
		if err != nil {
			reason := agentRuntimeStopReason(err)
			return model.AgentResult{RunID: session.RunID, Contexts: contexts, StopReason: reason, Steps: step + 1}, u.failAgentRun(ctx, session, reason, err)
		}
		session.TotalTokens += agentGenerationTokenUsage(gen.Usage, session.Messages, toolSpecs, gen)
		stepID, err := u.recordAgentStep(ctx, session, step, toolSpecs, gen)
		if err != nil {
			return model.AgentResult{RunID: session.RunID, Contexts: contexts, Steps: step + 1}, u.failAgentRun(ctx, session, model.AgentStopReasonRuntimeError, err)
		}
		if agentTokenBudgetExceeded(session) {
			err := domain.ErrGenerationFailed.Extend("agent reached its token budget before producing a final answer")
			return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: model.AgentStopReasonBudget, Steps: step + 1}, u.failAgentRun(ctx, session, model.AgentStopReasonBudget, err)
		}
		if len(gen.ToolCalls) == 0 {
			answer := strings.TrimSpace(gen.Content)
			if answer == "" {
				answer = "The agent completed without a final answer."
			}
			if _, err := u.recordAgentRun(ctx, session, model.AgentRunStatusCompleted, model.AgentStopReasonFinalAnswer); err != nil {
				return model.AgentResult{RunID: session.RunID, Contexts: contexts, Steps: step + 1}, err
			}
			return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Answer: answer, Contexts: contexts, StopReason: model.AgentStopReasonFinalAnswer, Steps: step + 1}, nil
		}
		session.Messages = append(session.Messages, model.ChatMessage{
			Role:      model.ChatMessageRoleAssistant,
			Content:   gen.Content,
			ToolCalls: gen.ToolCalls,
		})
		for toolIndex, call := range gen.ToolCalls {
			callKey := agentToolCallKey(call, toolIndex)
			signature := agentToolCallSignature(call)
			if signature == lastToolCallSignature {
				repeatedToolCallCount++
			} else {
				lastToolCallSignature = signature
				repeatedToolCallCount = 0
			}
			if repeatedToolCallCount >= 2 {
				err := domain.ErrGenerationFailed.Extend("agent repeated the same tool call")
				return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: model.AgentStopReasonLoopDetected, Steps: step + 1}, u.failAgentRun(ctx, session, model.AgentStopReasonLoopDetected, err)
			}
			toolResult, err := u.toolInvoker.Invoke(runCtx, appToolInvocationContext(session, stepID, callKey), call)
			if err != nil && toolResult.CallID == "" {
				toolResult = model.ToolResult{
					CallID:    call.ID,
					Name:      call.Name,
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
			contexts = append(contexts, toolResult.Contexts...)
			session.TotalTokens += toolResult.TokenEstimate
			if err := u.recordAgentToolInvocation(ctx, session, stepID, callKey, call, toolResult); err != nil {
				return model.AgentResult{RunID: session.RunID, Contexts: contexts, Steps: step + 1}, u.failAgentRun(ctx, session, model.AgentStopReasonRuntimeError, err)
			}
			switch agentToolResultErrorType(toolResult) {
			case model.ToolErrorTypeTransient:
				transientToolFailures[signature]++
				if transientToolFailures[signature] > agentTransientToolFailureRetries {
					if err == nil {
						err = domain.ErrGenerationFailed.Extend("agent transient tool failure retry limit exceeded")
					}
					reason := agentToolStopReason(err)
					return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: reason, Steps: step + 1}, u.failAgentRun(ctx, session, reason, err)
				}
			case model.ToolErrorTypePermanent, model.ToolErrorTypePolicyDenied:
				if err == nil {
					err = domain.ErrGenerationFailed.Extend("agent tool call failed")
				}
				reason := agentToolStopReason(err)
				return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: reason, Steps: step + 1}, u.failAgentRun(ctx, session, reason, err)
			default:
				if err != nil {
					reason := agentToolStopReason(err)
					return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: reason, Steps: step + 1}, u.failAgentRun(ctx, session, reason, err)
				}
			}
			if agentTokenBudgetExceeded(session) {
				err := domain.ErrGenerationFailed.Extend("agent reached its token budget before producing a final answer")
				return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: model.AgentStopReasonBudget, Steps: step + 1}, u.failAgentRun(ctx, session, model.AgentStopReasonBudget, err)
			}
			session.Messages = append(session.Messages, model.ChatMessage{
				Role:       model.ChatMessageRoleTool,
				Content:    toolResult.Content,
				ToolCallID: call.ID,
				Name:       call.Name,
			})
		}
	}
	err = domain.ErrGenerationFailed.Extend("agent reached its step limit before producing a final answer")
	return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: model.AgentStopReasonMaxSteps, Steps: maxSteps}, u.failAgentRun(ctx, session, model.AgentStopReasonMaxSteps, err)
}

func appToolResolutionContext(session *model.AgentSession) ToolResolutionContext {
	log.Trace("appToolResolutionContext")

	return ToolResolutionContext{
		OrgID:    session.OrgID,
		UserID:   session.UserID,
		Spec:     session.Spec,
		Datasets: session.Datasets,
	}
}

func appToolInvocationContext(session *model.AgentSession, stepID uuid.UUID, callKey string) ToolInvocationContext {
	log.Trace("appToolInvocationContext")

	return ToolInvocationContext{
		OrgID:        session.OrgID,
		UserID:       session.UserID,
		RunID:        session.RunID,
		InvocationID: deterministicAgentToolInvocationID(session.RunID, stepID, callKey),
		Datasets:     session.Datasets,
	}
}

func agentRuntimeStopReason(err error) model.AgentStopReason {
	log.Trace("agentRuntimeStopReason")

	if errors.Is(err, context.DeadlineExceeded) {
		return model.AgentStopReasonDeadline
	}
	return model.AgentStopReasonRuntimeError
}

func agentToolStopReason(err error) model.AgentStopReason {
	log.Trace("agentToolStopReason")

	if errors.Is(err, context.DeadlineExceeded) {
		return model.AgentStopReasonDeadline
	}
	return model.AgentStopReasonToolError
}

func agentToolResultErrorType(result model.ToolResult) model.ToolErrorType {
	log.Trace("agentToolResultErrorType")

	return agentToolFailureClass(result.IsError, result.ErrorType)
}

func agentToolFailureClass(isError bool, errorType model.ToolErrorType) model.ToolErrorType {
	log.Trace("agentToolFailureClass")

	switch errorType {
	case model.ToolErrorTypeTransient, model.ToolErrorTypePermanent, model.ToolErrorTypePolicyDenied:
		return errorType
	case model.ToolErrorTypeUnknown:
		if isError {
			return model.ToolErrorTypePermanent
		}
		return model.ToolErrorTypeUnknown
	default:
		return model.ToolErrorTypePermanent
	}
}

func (u *inferenceUsecase) failAgentRun(ctx context.Context, session *model.AgentSession, reason model.AgentStopReason, err error) error {
	log.Trace("InferenceUsecase failAgentRun")

	_, recordErr := u.recordAgentRun(ctx, session, model.AgentRunStatusFailed, reason)
	return errors.Join(err, recordErr)
}

func agentToolCallSignature(call model.ToolCall) string {
	log.Trace("agentToolCallSignature")

	return strings.Join([]string{strings.TrimSpace(call.Name), strings.TrimSpace(string(call.Arguments))}, "\x00")
}

func agentToolCallKey(call model.ToolCall, index int) string {
	log.Trace("agentToolCallKey")

	if value := strings.TrimSpace(call.ID); value != "" {
		return value
	}
	return fmt.Sprintf("%d", index)
}

func agentDecodingOptions(spec *model.AgentSpec, request model.GenerateRequest) model.GenerationOptions {
	log.Trace("agentDecodingOptions")

	tokenBudget := 0
	if spec != nil {
		tokenBudget = spec.Budgets.Token
	}
	return model.GenerationOptions{
		Temperature:     0,
		TopP:            1,
		Seed:            agentSeed(request.RequestID),
		MaxOutputTokens: tokenBudget,
	}
}

func agentStepGenerationOptions(session *model.AgentSession) model.GenerationOptions {
	log.Trace("agentStepGenerationOptions")

	options := session.DecodingOptions
	if session.Spec != nil && session.Spec.Budgets.Token > 0 {
		remaining := session.Spec.Budgets.Token - session.TotalTokens
		if remaining < options.MaxOutputTokens || options.MaxOutputTokens <= 0 {
			options.MaxOutputTokens = remaining
		}
	}
	return options
}

func agentSeed(requestID uuid.UUID) int64 {
	log.Trace("agentSeed")

	var seed int64
	for _, b := range requestID {
		seed = (seed << 5) - seed + int64(b)
	}
	if seed < 0 {
		return -seed
	}
	return seed
}

func generationTokenUsage(usage model.TokenUsage) int {
	log.Trace("generationTokenUsage")

	if usage.TotalTokens > 0 {
		return usage.TotalTokens
	}
	return usage.PromptTokens + usage.CompletionTokens
}

func agentGenerationTokenUsage(usage model.TokenUsage, messages []model.ChatMessage, tools []model.ToolSpec, result model.GenerationResult) int {
	log.Trace("agentGenerationTokenUsage")

	if tokens := generationTokenUsage(usage); tokens > 0 {
		return tokens
	}
	return estimateAgentGenerationTokens(messages, tools, result)
}

func estimateAgentGenerationTokens(messages []model.ChatMessage, tools []model.ToolSpec, result model.GenerationResult) int {
	log.Trace("estimateAgentGenerationTokens")

	total := 0
	for _, message := range messages {
		total += estimateTextTokens(string(message.Role))
		total += estimateTextTokens(message.Content)
		total += estimateTextTokens(message.ToolCallID)
		total += estimateTextTokens(message.Name)
		for _, call := range message.ToolCalls {
			total += estimateTextTokens(call.ID)
			total += estimateTextTokens(call.Name)
			total += estimateTextTokens(string(call.Arguments))
		}
	}
	for _, tool := range tools {
		total += estimateTextTokens(tool.Name)
		total += estimateTextTokens(tool.Description)
		total += estimateTextTokens(string(tool.Parameters))
	}
	total += estimateTextTokens(result.Content)
	for _, call := range result.ToolCalls {
		total += estimateTextTokens(call.ID)
		total += estimateTextTokens(call.Name)
		total += estimateTextTokens(string(call.Arguments))
	}
	if total <= 0 {
		return 1
	}
	return total
}

func estimateTextTokens(value string) int {
	log.Trace("estimateTextTokens")

	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	return (len([]rune(value)) + 3) / 4
}

func estimateAgentPromptTokens(messages []model.ChatMessage, tools []model.ToolSpec) int {
	log.Trace("estimateAgentPromptTokens")

	return estimateAgentGenerationTokens(messages, tools, model.GenerationResult{})
}

func agentTokenBudgetExceeded(session *model.AgentSession) bool {
	log.Trace("agentTokenBudgetExceeded")

	return session != nil && session.Spec != nil && session.Spec.Budgets.Token > 0 && session.TotalTokens >= session.Spec.Budgets.Token
}

func agentTokenBudgetWouldExceed(session *model.AgentSession, additionalTokens int) bool {
	log.Trace("agentTokenBudgetWouldExceed")

	return session != nil &&
		session.Spec != nil &&
		session.Spec.Budgets.Token > 0 &&
		session.TotalTokens+additionalTokens >= session.Spec.Budgets.Token
}

func agentToolErrorType(err error) model.ToolErrorType {
	log.Trace("agentToolErrorType")

	if err == nil {
		return model.ToolErrorTypeUnknown
	}
	if errors.Is(err, domain.ErrValidationFailed) {
		return model.ToolErrorTypePolicyDenied
	}
	if errors.Is(err, domain.ErrModelNotReady) ||
		errors.Is(err, domain.ErrRetrievalFailed) ||
		errors.Is(err, context.DeadlineExceeded) {
		return model.ToolErrorTypeTransient
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return model.ToolErrorTypeTransient
	}
	return model.ToolErrorTypePermanent
}

func agentInitialMessages(spec *model.AgentSpec, queryText string) []model.ChatMessage {
	log.Trace("agentInitialMessages")

	messages := make([]model.ChatMessage, 0, 2)
	if spec != nil && strings.TrimSpace(spec.SystemPrompt) != "" {
		messages = append(messages, model.ChatMessage{
			Role:    model.ChatMessageRoleSystem,
			Content: strings.TrimSpace(spec.SystemPrompt),
		})
	}
	messages = append(messages, model.ChatMessage{
		Role:    model.ChatMessageRoleUser,
		Content: queryText,
	})
	return messages
}

func agentToolChoice(bindings []model.ToolBinding, step int) string {
	log.Trace("agentToolChoice")

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
