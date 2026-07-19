package rest

import (
	"context"
	"errors"
	"net/http"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/network/adapter"

	"lib/shared_lib/ctxutil"
	"lib/shared_lib/transport"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	pathInferenceEndpoints             = "/v1/inference/endpoints"
	pathInferenceEndpoint              = "/v1/inference/endpoints/{endpointId}"
	pathInferenceAgentSpecs            = "/v1/inference/agent-specs"
	pathInferenceAgentSpec             = "/v1/inference/agent-specs/{agentSpecHash}"
	pathInferenceEndpointDatasets      = "/v1/inference/endpoints/{endpointId}/datasets"
	pathInferenceEndpointMergeStrategy = "/v1/inference/endpoints/{endpointId}/merge-strategy"
	pathInferenceEndpointGenerations   = "/v1/inference/endpoints/{endpointId}/generations"
	pathInferenceEndpointAgentEvalRuns = "/v1/inference/endpoints/{endpointId}/agent-eval-runs/{agentSpecHash}"
	pathInferenceEndpointPreferences   = "/v1/inference/endpoints/{endpointId}/preference-datasets"
	pathInferencePreferenceDatasets    = "/v1/inference/preference-datasets"
	pathInferencePreferenceDataset     = "/v1/inference/preference-datasets/{preferenceDatasetId}"
	pathInferenceAgentRun              = "/v1/inference/agent-runs/{runId}"
	pathInferenceFeedback              = "/v1/inference/feedback"
)

type InferenceHandlers struct {
	usecase                app.InferenceUsecase
	endpointAdapter        adapter.EndpointDTOAdapter
	generationAdapter      adapter.GenerationDTOAdapter
	feedbackAdapter        adapter.FeedbackDTOAdapter
	agentSpecAdapter       adapter.AgentSpecDTOAdapter
	preferenceAdapter      adapter.PreferenceDatasetDTOAdapter
	agentTrajectoryAdapter adapter.AgentTrajectoryDTOAdapter
}

func NewInferenceHandlers(
	usecase app.InferenceUsecase,
	endpointAdapter adapter.EndpointDTOAdapter,
	generationAdapter adapter.GenerationDTOAdapter,
	feedbackAdapter adapter.FeedbackDTOAdapter,
	agentSpecAdapter adapter.AgentSpecDTOAdapter,
	preferenceAdapter adapter.PreferenceDatasetDTOAdapter,
	agentTrajectoryAdapter adapter.AgentTrajectoryDTOAdapter,
) *InferenceHandlers {
	log.Trace("NewInferenceHandlers")

	return &InferenceHandlers{
		usecase:                usecase,
		endpointAdapter:        endpointAdapter,
		generationAdapter:      generationAdapter,
		feedbackAdapter:        feedbackAdapter,
		agentSpecAdapter:       agentSpecAdapter,
		preferenceAdapter:      preferenceAdapter,
		agentTrajectoryAdapter: agentTrajectoryAdapter,
	}
}

func (h *InferenceHandlers) GetRoutes() []Route {
	log.Trace("InferenceHandlers GetRoutes")

	return []Route{
		{
			Path:     pathInferenceEndpoints,
			Handler:  h.ListEndpoints,
			Method:   http.MethodGet,
			SpanName: "list-inference-endpoints",
		},
		{
			Path:     pathInferenceEndpoints,
			Handler:  h.PublishEndpoint,
			Method:   http.MethodPost,
			SpanName: "publish-inference-endpoint",
		},
		{
			Path:     pathInferenceEndpoint,
			Handler:  h.ReadEndpoint,
			Method:   http.MethodGet,
			SpanName: "read-inference-endpoint",
		},
		{
			Path:     pathInferenceAgentSpecs,
			Handler:  h.PublishAgentSpec,
			Method:   http.MethodPost,
			SpanName: "publish-inference-agent-spec",
		},
		{
			Path:     pathInferenceAgentSpec,
			Handler:  h.ReadAgentSpec,
			Method:   http.MethodGet,
			SpanName: "read-inference-agent-spec",
		},
		{
			Path:     pathInferenceEndpointDatasets,
			Handler:  h.SetEndpointDatasets,
			Method:   http.MethodPut,
			SpanName: "set-inference-endpoint-datasets",
		},
		{
			Path:     pathInferenceEndpointMergeStrategy,
			Handler:  h.SetEndpointMergeStrategy,
			Method:   http.MethodPut,
			SpanName: "set-inference-endpoint-merge-strategy",
		},
		{
			Path:     pathInferenceEndpointGenerations,
			Handler:  h.Generate,
			Method:   http.MethodPost,
			SpanName: "generate-from-inference-endpoint",
		},
		{
			Path:     pathInferenceEndpointAgentEvalRuns,
			Handler:  h.StartAgentEvalRun,
			Method:   http.MethodPost,
			SpanName: "start-agent-eval-run",
		},
		{
			Path:     pathInferenceFeedback,
			Handler:  h.RecordFeedback,
			Method:   http.MethodPost,
			SpanName: "record-inference-feedback",
		},
		{
			Path:     pathInferenceEndpointPreferences,
			Handler:  h.BuildPreferenceDataset,
			Method:   http.MethodPost,
			SpanName: "build-inference-preference-dataset",
		},
		{
			Path:     pathInferencePreferenceDatasets,
			Handler:  h.ListPreferenceDatasets,
			Method:   http.MethodGet,
			SpanName: "list-inference-preference-datasets",
		},
		{
			Path:     pathInferencePreferenceDataset,
			Handler:  h.ReadPreferenceDataset,
			Method:   http.MethodGet,
			SpanName: "read-inference-preference-dataset",
		},
		{
			Path:     pathInferenceAgentRun,
			Handler:  h.ReadAgentRun,
			Method:   http.MethodGet,
			SpanName: "read-inference-agent-run",
		},
	}
}

func (h *InferenceHandlers) ListEndpoints(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers ListEndpoints")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	endpoints, err := h.usecase.ListEndpoints(ctx, orgID)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.endpointAdapter.ToDTOs(ctx, endpoints)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode inference endpoints")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) PublishEndpoint(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers PublishEndpoint")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	command, err := h.endpointAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid publish endpoint request")
	}
	command.UserID = actor
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.PublishEndpoint(ctx, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.endpointAdapter.ToDetailDTOs(ctx, []*model.PublishedEndpoint{result})
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode inference endpoint")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *InferenceHandlers) ReadEndpoint(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers ReadEndpoint")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	endpointID, err := uuid.Parse(mux.Vars(req)["endpointId"])
	if err != nil || endpointID == uuid.Nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid endpoint ID")
	}
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	endpoint, err := h.usecase.ReadEndpoint(ctx, orgID, endpointID)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.endpointAdapter.ToDetailDTOs(ctx, []*model.PublishedEndpoint{endpoint})
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode inference endpoint")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) PublishAgentSpec(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers PublishAgentSpec")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	command, err := h.agentSpecAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent spec")
	}
	command.UserID = actor
	command.OrgID = orgID
	if command.Spec != nil {
		command.Spec.OrgID = orgID
	}
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.PublishAgentSpec(ctx, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.agentSpecAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent spec")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *InferenceHandlers) ReadAgentSpec(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers ReadAgentSpec")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	agentSpecHash := mux.Vars(req)["agentSpecHash"]
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	spec, err := h.usecase.ReadAgentSpec(ctx, orgID, agentSpecHash)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.agentSpecAdapter.ToDTO(ctx, spec)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent spec")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) SetEndpointDatasets(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers SetEndpointDatasets")

	actor, orgID, endpointID, body, err := h.readEndpointMutation(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.endpointAdapter.FromDatasetBindingDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid endpoint datasets request")
	}
	command.UserID = actor
	command.OrgID = orgID
	command.EndpointID = endpointID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.SetEndpointDatasets(ctx, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.endpointAdapter.ToDetailDTOs(ctx, []*model.PublishedEndpoint{result})
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode inference endpoint")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) SetEndpointMergeStrategy(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers SetEndpointMergeStrategy")

	actor, orgID, endpointID, body, err := h.readEndpointMutation(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.endpointAdapter.FromMergeConfigurationDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid endpoint merge strategy request")
	}
	command.UserID = actor
	command.OrgID = orgID
	command.EndpointID = endpointID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.SetEndpointMergeStrategy(ctx, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.endpointAdapter.ToDetailDTOs(ctx, []*model.PublishedEndpoint{result})
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode inference endpoint")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) Generate(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers Generate")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	requestID, err := transport.ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("X-Request-ID is required")
	}
	endpointID, err := readEndpointID(req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid endpoint id")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	command, err := h.generationAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid generation request")
	}
	command.RequestID = requestID
	command.UserID = actor
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.GenerateForEndpoint(ctx, endpointID, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.generationAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode generation response")
	}
	if result.Accepted {
		return NewResponseWithPayload(http.StatusAccepted, payload), nil
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) StartAgentEvalRun(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers StartAgentEvalRun")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	requestID, err := transport.ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("X-Request-ID is required")
	}
	endpointID, err := readEndpointID(req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid endpoint id")
	}
	agentSpecHash := mux.Vars(req)["agentSpecHash"]
	if agentSpecHash == "" {
		return nil, ErrBadRequest().WithMessage("Invalid agent spec hash")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	command, err := h.generationAdapter.FromAgentEvalRunDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent eval run request")
	}
	command.RequestID = requestID
	command.UserID = actor
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.StartAgentEvalRun(ctx, endpointID, agentSpecHash, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.generationAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent eval run response")
	}
	return NewResponseWithPayload(http.StatusAccepted, payload), nil
}

func (h *InferenceHandlers) RecordFeedback(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers RecordFeedback")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	feedbackID, err := transport.ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("X-Request-ID is required")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	feedback, err := h.feedbackAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid feedback request")
	}
	feedback.FeedbackID = feedbackID
	feedback.UserID = actor
	feedback.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.RecordFeedback(ctx, feedback, feedbackID)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.feedbackAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode feedback response")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *InferenceHandlers) BuildPreferenceDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers BuildPreferenceDataset")

	actor, orgID, endpointID, body, err := h.readEndpointMutation(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.preferenceAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid preference dataset request")
	}
	command.UserID = actor
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.BuildPreferenceDatasetForEndpoint(ctx, endpointID, command)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.preferenceAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode preference dataset")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *InferenceHandlers) ListPreferenceDatasets(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers ListPreferenceDatasets")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	filter, err := preferenceDatasetFilter(req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid preference dataset filter")
	}
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.ListPreferenceDatasets(ctx, orgID, filter)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.preferenceAdapter.ToDTOs(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode preference datasets")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) ReadPreferenceDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers ReadPreferenceDataset")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	preferenceDatasetID, err := readPreferenceDatasetID(req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid preference dataset id")
	}
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.ReadPreferenceDataset(ctx, orgID, preferenceDatasetID)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.preferenceAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode preference dataset")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) ReadAgentRun(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers ReadAgentRun")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	runID, err := readAgentRunID(req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent run id")
	}
	ctx = ctxutil.WithActorOrg(ctx, actor, orgID)
	result, err := h.usecase.ReadAgentTrajectory(ctx, orgID, runID)
	if err != nil {
		return nil, mapInferenceError(err)
	}
	payload, err := h.agentTrajectoryAdapter.ToDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent run")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *InferenceHandlers) readEndpointMutation(ctx context.Context, req *http.Request) (uuid.UUID, uuid.UUID, uuid.UUID, []byte, error) {
	log.Trace("InferenceHandlers readEndpointMutation")

	actor, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	endpointID, err := readEndpointID(req)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid endpoint id")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	return actor, orgID, endpointID, body, nil
}

func readActorOrg(ctx context.Context, req *http.Request) (uuid.UUID, uuid.UUID, error) {
	log.Trace("readActorOrg")

	userID, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	orgID, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return userID, orgID, nil
}

func readEndpointID(req *http.Request) (uuid.UUID, error) {
	log.Trace("readEndpointID")

	endpointID, err := uuid.Parse(mux.Vars(req)["endpointId"])
	if err != nil || endpointID == uuid.Nil {
		return uuid.Nil, domain.ErrValidationFailed.Extend("endpoint_id is invalid")
	}
	return endpointID, nil
}

func readPreferenceDatasetID(req *http.Request) (uuid.UUID, error) {
	log.Trace("readPreferenceDatasetID")

	preferenceDatasetID, err := uuid.Parse(mux.Vars(req)["preferenceDatasetId"])
	if err != nil || preferenceDatasetID == uuid.Nil {
		return uuid.Nil, domain.ErrValidationFailed.Extend("preference_dataset_id is invalid")
	}
	return preferenceDatasetID, nil
}

func readAgentRunID(req *http.Request) (uuid.UUID, error) {
	log.Trace("readAgentRunID")

	runID, err := uuid.Parse(mux.Vars(req)["runId"])
	if err != nil || runID == uuid.Nil {
		return uuid.Nil, domain.ErrValidationFailed.Extend("run_id is invalid")
	}
	return runID, nil
}

func preferenceDatasetFilter(req *http.Request) (model.PreferenceDatasetFilter, error) {
	log.Trace("preferenceDatasetFilter")

	filter := model.PreferenceDatasetFilter{}
	if value := req.URL.Query().Get("model_id"); value != "" {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil {
			return filter, domain.ErrValidationFailed.Extend("model_id is invalid")
		}
		filter.ModelID = id
	}
	if value := req.URL.Query().Get("endpoint_id"); value != "" {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil {
			return filter, domain.ErrValidationFailed.Extend("endpoint_id is invalid")
		}
		filter.EndpointID = id
	}
	return filter, nil
}

func mapInferenceError(err error) error {
	log.Trace("mapInferenceError")

	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrValidationFailed) {
		return ErrBadRequest().Wrap(err).WithMessage(err.Error())
	}
	if errors.Is(err, domain.ErrModelNotFound) || errors.Is(err, domain.ErrDatasetNotFound) || errors.Is(err, domain.ErrAgentRunNotFound) {
		return ErrNotFound().Wrap(err).WithMessage(err.Error())
	}
	if errors.Is(err, domain.ErrModelNotReady) || errors.Is(err, domain.ErrModelMismatch) || errors.Is(err, domain.ErrDatasetNotReady) {
		return ErrConflict().Wrap(err).WithMessage(err.Error())
	}
	if errors.Is(err, domain.ErrRetrievalFailed) || errors.Is(err, domain.ErrRerankFailed) || errors.Is(err, domain.ErrGenerationFailed) {
		return ErrInternalServer().Wrap(err).WithMessage(err.Error())
	}
	return ErrInternalServer().Wrap(err).WithMessage("Inference request failed")
}
