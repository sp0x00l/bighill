package rest

import (
	"context"
	"errors"
	"net/http"

	"agent_registry_service/pkg/app"
	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	"agent_registry_service/pkg/infra/network/adapter"
	"lib/shared_lib/ctxutil"
	"lib/shared_lib/transport"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	pathAgentSpecVersions     = "/v1/agent-registry/spec-versions"
	pathAgentEndpointBindings = "/v1/agent-registry/endpoint-bindings"
	pathAgentChampion         = "/v1/agent-registry/champions"
	pathGoldenTasks           = "/v1/agent-registry/golden-tasks"
	pathAgentRunLabels        = "/v1/agent-registry/run-labels"
	pathTrajectoryDatasets    = "/v1/agent-registry/trajectory-datasets"
	pathTrajectoryDataset     = "/v1/agent-registry/trajectory-datasets/{datasetId}"
	pathAgentAdapters         = "/v1/agent-registry/adapters"
	pathAgentAdapter          = "/v1/agent-registry/adapters/{adapterId}"
	pathAdapterCandidateEvals = "/v1/agent-registry/evaluations/adapter-candidates"
	pathAdapterPromotions     = "/v1/agent-registry/adapters/promotions"
	pathSpecChampionEvals     = "/v1/agent-registry/evaluations/spec-champions"
	pathAgentEvalReport       = "/v1/agent-registry/evaluations/{reportId}"
)

type AgentRegistryHandlers struct {
	usecase app.AgentRegistryUsecase
	adapter adapter.AgentRegistryDTOAdapter
}

func NewAgentRegistryHandlers(usecase app.AgentRegistryUsecase, adapter adapter.AgentRegistryDTOAdapter) *AgentRegistryHandlers {
	log.Trace("NewAgentRegistryHandlers")

	return &AgentRegistryHandlers{usecase: usecase, adapter: adapter}
}

func (h *AgentRegistryHandlers) GetRoutes() []Route {
	log.Trace("AgentRegistryHandlers GetRoutes")

	return []Route{
		{
			Path:     pathAgentSpecVersions,
			Handler:  h.RegisterAgentSpecVersion,
			Method:   http.MethodPost,
			SpanName: "register-agent-spec-version",
		},
		{
			Path:     pathAgentEndpointBindings,
			Handler:  h.RegisterEndpointBinding,
			Method:   http.MethodPost,
			SpanName: "register-agent-endpoint-binding",
		},
		{
			Path:     pathAgentChampion,
			Handler:  h.PromoteSpecChampion,
			Method:   http.MethodPost,
			SpanName: "promote-agent-spec-champion",
		},
		{
			Path:     pathGoldenTasks,
			Handler:  h.ImportGoldenTasks,
			Method:   http.MethodPost,
			SpanName: "import-golden-tasks",
		},
		{
			Path:     pathGoldenTasks,
			Handler:  h.ListGoldenTasks,
			Method:   http.MethodGet,
			SpanName: "list-golden-tasks",
		},
		{
			Path:     pathAgentRunLabels,
			Handler:  h.LabelAgentRun,
			Method:   http.MethodPost,
			SpanName: "label-agent-run",
		},
		{
			Path:     pathAgentRunLabels,
			Handler:  h.ListAgentRunLabels,
			Method:   http.MethodGet,
			SpanName: "list-agent-run-labels",
		},
		{
			Path:     pathTrajectoryDatasets,
			Handler:  h.BuildTrajectoryDataset,
			Method:   http.MethodPost,
			SpanName: "build-agent-trajectory-dataset",
		},
		{
			Path:     pathTrajectoryDataset,
			Handler:  h.ReadTrajectoryDataset,
			Method:   http.MethodGet,
			SpanName: "read-agent-trajectory-dataset",
		},
		{
			Path:     pathAgentAdapters,
			Handler:  h.DispatchAgentAdapterTraining,
			Method:   http.MethodPost,
			SpanName: "train-agent-adapter",
		},
		{
			Path:     pathAgentAdapter,
			Handler:  h.ReadAgentAdapter,
			Method:   http.MethodGet,
			SpanName: "read-agent-adapter",
		},
		{
			Path:     pathAdapterCandidateEvals,
			Handler:  h.EvaluateAdapterCandidate,
			Method:   http.MethodPost,
			SpanName: "evaluate-adapter-candidate",
		},
		{
			Path:     pathAdapterPromotions,
			Handler:  h.PromoteAgentAdapter,
			Method:   http.MethodPost,
			SpanName: "promote-agent-adapter",
		},
		{
			Path:     pathSpecChampionEvals,
			Handler:  h.EvaluateSpecChampion,
			Method:   http.MethodPost,
			SpanName: "evaluate-spec-champion",
		},
		{
			Path:     pathAgentEvalReport,
			Handler:  h.ReadAgentEvalReport,
			Method:   http.MethodGet,
			SpanName: "read-agent-eval-report",
		},
	}
}

func (h *AgentRegistryHandlers) RegisterAgentSpecVersion(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers RegisterAgentSpecVersion")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromRegisterSpecVersionDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent spec version request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	version, err := h.usecase.RegisterAgentSpecVersion(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToSpecVersionDTO(ctx, version)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent spec version")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) RegisterEndpointBinding(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers RegisterEndpointBinding")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromRegisterEndpointBindingDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent endpoint binding request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	binding, err := h.usecase.RegisterEndpointBinding(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToEndpointBindingDTO(ctx, binding)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent endpoint binding")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) PromoteSpecChampion(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers PromoteSpecChampion")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromPromoteSpecChampionDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent champion request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	state, err := h.usecase.PromoteSpecChampion(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToChampionStateDTO(ctx, state)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent champion state")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *AgentRegistryHandlers) ImportGoldenTasks(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers ImportGoldenTasks")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromImportGoldenTasksDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid golden task import request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	tasks, err := h.usecase.ImportGoldenTasks(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToGoldenTaskDTOs(ctx, tasks)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode golden tasks")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) ListGoldenTasks(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers ListGoldenTasks")

	userID, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromListGoldenTasksDTO(ctx, singleQueryValues(req))
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid golden task list request")
	}
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	tasks, err := h.usecase.ListGoldenTasks(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToGoldenTaskDTOs(ctx, tasks)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode golden tasks")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *AgentRegistryHandlers) LabelAgentRun(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers LabelAgentRun")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromLabelAgentRunDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent run label request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	label, err := h.usecase.LabelAgentRun(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentRunLabelDTOs(ctx, []*model.AgentRunLabel{label})
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent run label")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) ListAgentRunLabels(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers ListAgentRunLabels")

	userID, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromListAgentRunLabelsDTO(ctx, singleQueryValues(req))
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent run label list request")
	}
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	labels, err := h.usecase.ListAgentRunLabels(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentRunLabelDTOs(ctx, labels)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent run labels")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *AgentRegistryHandlers) BuildTrajectoryDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers BuildTrajectoryDataset")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromBuildTrajectoryDatasetDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid trajectory dataset request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	dataset, err := h.usecase.BuildTrajectoryDataset(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToTrajectoryDatasetDTO(ctx, dataset)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode trajectory dataset")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) ReadTrajectoryDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers ReadTrajectoryDataset")

	_, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, err
	}
	datasetID, err := uuid.Parse(mux.Vars(req)["datasetId"])
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid trajectory dataset id")
	}
	dataset, err := h.usecase.ReadTrajectoryDataset(ctx, orgID, datasetID)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToTrajectoryDatasetDTO(ctx, dataset)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode trajectory dataset")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *AgentRegistryHandlers) DispatchAgentAdapterTraining(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers DispatchAgentAdapterTraining")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromDispatchAgentAdapterTrainingDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid train agent adapter request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	adapterRecord, err := h.usecase.DispatchAgentAdapterTraining(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentAdapterDTO(ctx, adapterRecord)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent adapter")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) ReadAgentAdapter(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers ReadAgentAdapter")

	_, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, err
	}
	adapterID, err := uuid.Parse(mux.Vars(req)["adapterId"])
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent adapter id")
	}
	record, err := h.usecase.ReadAgentAdapter(ctx, orgID, adapterID)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentAdapterDTO(ctx, record)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent adapter")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *AgentRegistryHandlers) EvaluateAdapterCandidate(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers EvaluateAdapterCandidate")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromEvaluateAdapterCandidateDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid adapter candidate eval request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	report, err := h.usecase.EvaluateAdapterCandidate(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentEvalReportDTO(ctx, report)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode adapter candidate eval report")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) PromoteAgentAdapter(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers PromoteAgentAdapter")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromPromoteAgentAdapterDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent adapter promotion request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	record, err := h.usecase.PromoteAgentAdapter(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentAdapterDTO(ctx, record)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode promoted agent adapter")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func (h *AgentRegistryHandlers) EvaluateSpecChampion(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers EvaluateSpecChampion")

	userID, orgID, body, err := readActorOrgBody(ctx, req)
	if err != nil {
		return nil, err
	}
	command, err := h.adapter.FromEvaluateSpecChampionDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent eval request")
	}
	command.UserID = userID
	command.OrgID = orgID
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	report, err := h.usecase.EvaluateSpecChampion(ctx, command)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentEvalReportDTO(ctx, report)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent eval report")
	}
	return NewResponseWithPayload(http.StatusCreated, payload), nil
}

func (h *AgentRegistryHandlers) ReadAgentEvalReport(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("AgentRegistryHandlers ReadAgentEvalReport")

	userID, orgID, err := readActorOrg(ctx, req)
	if err != nil {
		return nil, err
	}
	reportID, err := uuid.Parse(mux.Vars(req)["reportId"])
	if err != nil || reportID == uuid.Nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid agent eval report id")
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	report, err := h.usecase.ReadAgentEvalReport(ctx, orgID, reportID)
	if err != nil {
		return nil, mapAgentRegistryError(err)
	}
	payload, err := h.adapter.ToAgentEvalReportDTO(ctx, report)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode agent eval report")
	}
	return NewResponseWithPayload(http.StatusOK, payload), nil
}

func readActorOrgBody(ctx context.Context, req *http.Request) (userID uuid.UUID, orgID uuid.UUID, body []byte, err error) {
	log.Trace("readActorOrgBody")

	user, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	org, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	body, err = transport.ReadReqBody(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid request body")
	}
	return user, org, body, nil
}

func readActorOrg(ctx context.Context, req *http.Request) (userID uuid.UUID, orgID uuid.UUID, err error) {
	log.Trace("readActorOrg")

	user, err := transport.ReadUserIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	org, err := transport.ReadOrgIDHeader(ctx, req)
	if err != nil {
		return uuid.Nil, uuid.Nil, ErrBadRequest().Wrap(err).WithMessage("User and org headers are required")
	}
	return user, org, nil
}

func singleQueryValues(req *http.Request) map[string]string {
	values := map[string]string{}
	for key, raw := range req.URL.Query() {
		if len(raw) > 0 {
			values[key] = raw[0]
		}
	}
	return values
}

func mapAgentRegistryError(err error) error {
	log.Trace("mapAgentRegistryError")

	switch {
	case errors.Is(err, domain.ErrAgentRegistryValidation), errors.Is(err, domain.ErrGoldenTaskLeak), errors.Is(err, domain.ErrAgentEvalFailed), errors.Is(err, domain.ErrAgentTrainingFailed), errors.Is(err, domain.ErrAgentPromotionFailed):
		return ErrBadRequest().Wrap(err).WithMessage("Invalid agent registry request")
	case errors.Is(err, domain.ErrAgentSpecUnavailable), errors.Is(err, domain.ErrEndpointUnavailable), errors.Is(err, domain.ErrAgentVersionNotFound), errors.Is(err, domain.ErrAgentLabelNotFound), errors.Is(err, domain.ErrTrajectoryDatasetNotFound), errors.Is(err, domain.ErrAgentAdapterNotFound), errors.Is(err, domain.ErrAgentChampionNotFound):
		return ErrNotFound().Wrap(err).WithMessage("Agent registry dependency not found")
	default:
		return ErrInternalServer().Wrap(err).WithMessage("Agent registry request failed")
	}
}
