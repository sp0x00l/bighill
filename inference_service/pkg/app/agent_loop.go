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
	inferenceModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, endpoint.ModelID)
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
	dataset := readyDatasets[0]
	request.DatasetID = dataset.DatasetID

	generationProtocol := strings.TrimSpace(inferenceModel.ServingProtocol.String())
	generationModel := strings.TrimSpace(inferenceModel.ServingModel)
	if inferenceModel.Status != model.ModelStatusReady {
		err = domain.ErrModelNotReady.Extend("model is not ready")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, nil, "", "", startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if inferenceModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		inferenceModel, err = u.ensureServingModelLoaded(ctx, request.OrgID, inferenceModel)
		if err != nil {
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, nil, "", "", startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
		generationProtocol = strings.TrimSpace(inferenceModel.ServingProtocol.String())
		generationModel = strings.TrimSpace(inferenceModel.ServingModel)
	}
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
	if err := u.recordInferenceRequest(ctx, request, dataset, inferenceModel, result.Contexts, "", result.Answer, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusCompleted, ""); err != nil {
		return nil, err
	}
	return response, nil
}

func (u *inferenceUsecase) runAgentLoop(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint, spec *model.AgentSpec, inferenceModel *model.InferenceModel, datasets []*model.InferenceDataset, generator GenerationAdapter) (model.AgentResult, error) {
	log.Trace("InferenceUsecase runAgentLoop")

	runCtx, cancel := context.WithTimeout(ctx, time.Duration(spec.Budgets.WallMs)*time.Millisecond)
	defer cancel()

	session := &model.AgentSession{
		OrgID:           request.OrgID,
		UserID:          request.UserID,
		Endpoint:        endpoint,
		Spec:            spec,
		Model:           inferenceModel,
		Datasets:        datasets,
		Messages:        agentInitialMessages(spec, request.QueryText),
		DecodingOptions: agentDecodingOptions(spec, request),
	}
	toolSpecs, err := u.toolInvoker.Available(runCtx, appToolResolutionContext(session), spec.ToolBindings)
	if err != nil {
		return model.AgentResult{}, err
	}
	session.ResolvedToolSpecs = toolSpecs
	run, err := u.recordAgentRun(ctx, session, model.AgentRunStatusRunning, model.AgentStopReasonUnknown)
	if err != nil {
		return model.AgentResult{}, err
	}
	session.RunID = run.RunID
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
		for _, call := range gen.ToolCalls {
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
			toolResult, err := u.toolInvoker.Invoke(runCtx, appToolInvocationContext(session), call)
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
			if err := u.recordAgentToolInvocation(ctx, session, stepID, call, toolResult); err != nil {
				return model.AgentResult{RunID: session.RunID, Contexts: contexts, Steps: step + 1}, u.failAgentRun(ctx, session, model.AgentStopReasonRuntimeError, err)
			}
			if agentToolResultIsTransient(toolResult) {
				transientToolFailures[signature]++
				if transientToolFailures[signature] > agentTransientToolFailureRetries {
					if err == nil {
						err = domain.ErrGenerationFailed.Extend("agent transient tool failure retry limit exceeded")
					}
					reason := agentToolStopReason(err)
					return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: reason, Steps: step + 1}, u.failAgentRun(ctx, session, reason, err)
				}
			} else if err != nil || toolResult.IsError || toolResult.ErrorType != model.ToolErrorTypeUnknown {
				if err == nil {
					err = domain.ErrGenerationFailed.Extend("agent tool call failed")
				}
				reason := agentToolStopReason(err)
				return model.AgentResult{RequestID: request.RequestID, RunID: session.RunID, Contexts: contexts, StopReason: reason, Steps: step + 1}, u.failAgentRun(ctx, session, reason, err)
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

func agentToolResultIsTransient(result model.ToolResult) bool {
	log.Trace("agentToolResultIsTransient")

	return result.IsError && result.ErrorType == model.ToolErrorTypeTransient
}

func appToolResolutionContext(session *model.AgentSession) ToolResolutionContext {
	log.Trace("appToolResolutionContext")

	return ToolResolutionContext{
		OrgID:  session.OrgID,
		UserID: session.UserID,
		Spec:   session.Spec,
	}
}

func appToolInvocationContext(session *model.AgentSession) ToolInvocationContext {
	log.Trace("appToolInvocationContext")

	return ToolInvocationContext{
		OrgID:    session.OrgID,
		UserID:   session.UserID,
		RunID:    session.RunID,
		Datasets: session.Datasets,
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

func (u *inferenceUsecase) failAgentRun(ctx context.Context, session *model.AgentSession, reason model.AgentStopReason, err error) error {
	log.Trace("InferenceUsecase failAgentRun")

	_, recordErr := u.recordAgentRun(ctx, session, model.AgentRunStatusFailed, reason)
	return errors.Join(err, recordErr)
}

func agentToolCallSignature(call model.ToolCall) string {
	log.Trace("agentToolCallSignature")

	return strings.Join([]string{strings.TrimSpace(call.Name), strings.TrimSpace(string(call.Arguments))}, "\x00")
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
