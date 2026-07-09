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
	pathInferenceEndpointDatasets      = "/v1/inference/endpoints/{endpointId}/datasets"
	pathInferenceEndpointMergeStrategy = "/v1/inference/endpoints/{endpointId}/merge-strategy"
	pathInferenceEndpointGenerations   = "/v1/inference/endpoints/{endpointId}/generations"
	pathInferenceFeedback              = "/v1/inference/feedback"
)

type InferenceHandlers struct {
	usecase app.InferenceUsecase
	adapter adapter.InferenceDTOAdapter
}

func NewInferenceHandlers(usecase app.InferenceUsecase, adapter adapter.InferenceDTOAdapter) *InferenceHandlers {
	log.Trace("NewInferenceHandlers")

	return &InferenceHandlers{usecase: usecase, adapter: adapter}
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
			Path:     pathInferenceFeedback,
			Handler:  h.RecordFeedback,
			Method:   http.MethodPost,
			SpanName: "record-inference-feedback",
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
	payload, err := h.adapter.ToEndpointDTOs(ctx, endpoints)
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
	command, err := h.adapter.FromEndpointPublicationDTO(ctx, body)
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
	payload, err := h.adapter.ToEndpointDetailDTOs(ctx, []*model.PublishedEndpoint{result})
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode inference endpoint")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *InferenceHandlers) SetEndpointDatasets(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("InferenceHandlers SetEndpointDatasets")

	actor, orgID, endpointID, body, err := h.readEndpointMutation(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromEndpointDatasetBindingDTO(ctx, body)
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
	payload, err := h.adapter.ToEndpointDetailDTOs(ctx, []*model.PublishedEndpoint{result})
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
	command, err := h.adapter.FromEndpointMergeConfigurationDTO(ctx, body)
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
	payload, err := h.adapter.ToEndpointDetailDTOs(ctx, []*model.PublishedEndpoint{result})
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
	command, err := h.adapter.FromGenerateDTO(ctx, body)
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
	payload, err := h.adapter.ToGenerateDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode generation response")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
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
	feedback, err := h.adapter.FromFeedbackDTO(ctx, body)
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
	payload, err := h.adapter.ToFeedbackDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode feedback response")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
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

func mapInferenceError(err error) error {
	log.Trace("mapInferenceError")

	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrValidationFailed) {
		return ErrBadRequest().Wrap(err).WithMessage(err.Error())
	}
	if errors.Is(err, domain.ErrModelNotFound) || errors.Is(err, domain.ErrDatasetNotFound) {
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
