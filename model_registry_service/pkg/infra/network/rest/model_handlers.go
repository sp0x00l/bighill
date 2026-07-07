package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"model_registry_service/pkg/app"
	modeldomain "model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	"model_registry_service/pkg/infra/network/adapter"

	"lib/shared_lib/ctxutil"
	"lib/shared_lib/transport"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	pathModels = "/v1/models"
)

type ModelHandlers struct {
	usecase         app.ModelRegistryUsecase
	modelDTOAdapter adapter.ModelDTOAdapter
}

func NewModelHandlers(usecase app.ModelRegistryUsecase, modelDTOAdapter adapter.ModelDTOAdapter) *ModelHandlers {
	log.Trace("NewModelHandlers")

	return &ModelHandlers{
		usecase:         usecase,
		modelDTOAdapter: modelDTOAdapter,
	}
}

func (h *ModelHandlers) GetRoutes() []Route {
	log.Trace("ModelHandlers GetRoutes")

	return []Route{
		{
			Path:     pathModels,
			Handler:  h.ListModels,
			Method:   http.MethodGet,
			SpanName: "list-models",
		},
		{
			Path:     pathModels + "/{modelId}",
			Handler:  h.ReadModel,
			Method:   http.MethodGet,
			SpanName: "read-model",
		},
	}
}

func (h *ModelHandlers) ListModels(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("ModelHandlers ListModels")

	userID, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User ID is required")
	}
	orgID, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Org ID is required")
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	pagination, err := transport.ReadPagination(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid pagination")
	}
	filter, err := modelListFilterFromRequest(req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid model filters")
	}

	models, total, err := h.usecase.ListModels(ctx, userID, *pagination, filter)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to list models")
	}
	metadata, _ := transport.NewMetadata(ctx, total, *pagination, nil, req.URL.String())
	paginatedResponse := &transport.PaginatedResponse{Metadata: metadata}
	if len(models) > 0 {
		paginatedResponse.Resources = h.modelDTOAdapter.ToDTOs(ctx, models, pathModels)
	}
	return NewResponseWithPagination(http.StatusOK, paginatedResponse), nil
}

func (h *ModelHandlers) ReadModel(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("ModelHandlers ReadModel")

	userID, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("User ID is required")
	}
	orgID, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Org ID is required")
	}
	modelID, err := uuid.Parse(mux.Vars(req)["modelId"])
	if err != nil || modelID == uuid.Nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid model ID")
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	modelRecord, err := h.usecase.ReadModelForUser(ctx, userID, modelID)
	if err != nil {
		if errors.Is(err, modeldomain.ErrModelNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Model not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to read model")
	}
	payload, err := h.modelDTOAdapter.ToDTO(ctx, modelRecord, pathModels)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode model")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func modelListFilterFromRequest(req *http.Request) (model.ListFilter, error) {
	log.Trace("modelListFilterFromRequest")

	var filter model.ListFilter
	query := req.URL.Query()
	if raw := query.Get("kind"); raw != "" {
		kind := model.ToModelKind(raw)
		if !model.IsKnownModelKind(kind) {
			return filter, fmt.Errorf("invalid model kind %q", raw)
		}
		filter.Kind = kind
		filter.KindSet = true
	}
	if raw := query.Get("source"); raw != "" {
		source := model.ToModelSource(raw)
		if !model.IsKnownModelSource(source) {
			return filter, fmt.Errorf("invalid model source %q", raw)
		}
		filter.Source = source
		filter.SourceSet = true
	}
	if raw := query.Get("status"); raw != "" {
		status, err := model.ToModelStatus(raw)
		if err != nil {
			return filter, err
		}
		filter.Status = status
		filter.StatusSet = true
	}
	if raw := query.Get("trainable"); raw == "true" || raw == "1" {
		filter.Trainable = true
	}
	return filter, nil
}
