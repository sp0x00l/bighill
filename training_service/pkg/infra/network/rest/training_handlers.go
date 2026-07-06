package rest

import (
	"context"
	"errors"
	"net/http"

	"training_service/pkg/app"
	"training_service/pkg/domain"
	"training_service/pkg/infra/network/adapter"

	"lib/shared_lib/ctxutil"
	"lib/shared_lib/transport"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	pathTrainingRuns      = "/v1/training-runs"
	pathTrainingRunStatus = "/v1/training-runs/{trainingRunId}"
)

type TrainingHandlers struct {
	usecase app.TrainingCommandUsecase
	adapter adapter.TrainingRunDTOAdapter
}

func NewTrainingHandlers(usecase app.TrainingCommandUsecase, adapter adapter.TrainingRunDTOAdapter) *TrainingHandlers {
	log.Trace("NewTrainingHandlers")

	return &TrainingHandlers{usecase: usecase, adapter: adapter}
}

func (h *TrainingHandlers) GetRoutes() []Route {
	log.Trace("TrainingHandlers GetRoutes")

	return []Route{
		{
			Path:     pathTrainingRuns,
			Handler:  h.StartTrainingRun,
			Method:   http.MethodPost,
			SpanName: "start-training-run",
		},
		{
			Path:     pathTrainingRunStatus,
			Handler:  h.ReadTrainingRun,
			Method:   http.MethodGet,
			SpanName: "read-training-run",
		},
	}
}

func (h *TrainingHandlers) StartTrainingRun(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("TrainingHandlers StartTrainingRun")

	userID, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User ID is required")
	}
	idempotencyKey, err := transport.ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("X-Request-ID is required")
	}
	body, err := transport.ReadReqBody(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	command, err := h.adapter.FromStartTrainingRunDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid training run request")
	}
	command.IdempotencyKey = idempotencyKey.String()
	ctx = ctxutil.WithTenantID(ctx, userID)
	result, err := h.usecase.StartTrainingRun(ctx, command)
	if err != nil {
		if errors.Is(err, domain.ErrValidationFailed) {
			return nil, ErrBadRequest().Wrap(err).WithMessage(err.Error())
		}
		if errors.Is(err, domain.ErrDependencyFailed) {
			return nil, ErrBadGateway().Wrap(err).WithMessage(err.Error())
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to start training run")
	}
	payload, err := h.adapter.ToStartTrainingRunDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode training run response")
	}
	return NewResponseWithPayload(http.StatusAccepted, payload), nil
}

func (h *TrainingHandlers) ReadTrainingRun(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("TrainingHandlers ReadTrainingRun")

	trainingRunID := mux.Vars(req)["trainingRunId"]
	result, err := h.usecase.ReadTrainingRun(ctx, trainingRunID)
	if err != nil {
		if errors.Is(err, domain.ErrValidationFailed) {
			return nil, ErrBadRequest().Wrap(err).WithMessage(err.Error())
		}
		if errors.Is(err, domain.ErrTrainingRunNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage(err.Error())
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to read training run")
	}
	payload, err := h.adapter.ToTrainingRunStatusDTO(ctx, result)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode training run status response")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}
