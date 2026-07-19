package app

import (
	"context"

	"agent_registry_service/pkg/domain/model"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type AgentRegistryRepository interface {
	EnsureLineage(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, agentLineage string, userID uuid.UUID) error
	UpsertAgentSpecVersion(ctx context.Context, tx pgx.Tx, version *model.AgentSpecVersion) (*model.AgentSpecVersion, error)
	UpsertEndpointBinding(ctx context.Context, tx pgx.Tx, binding *model.AgentEndpointBinding) (*model.AgentEndpointBinding, error)
	ReadSpecVersion(ctx context.Context, orgID uuid.UUID, agentSpecHash string) (*model.AgentSpecVersion, error)
	RecordChampionState(ctx context.Context, tx pgx.Tx, state *model.AgentChampionState) (*model.AgentChampionState, error)
	ListEndpointBindings(ctx context.Context, orgID uuid.UUID, agentLineage string) ([]*model.AgentEndpointBinding, error)
	CreateGoldenTask(ctx context.Context, tx pgx.Tx, task *model.GoldenTask) (*model.GoldenTask, error)
	FindGoldenTaskLeakConflicts(ctx context.Context, tx pgx.Tx, task *model.GoldenTask) ([]model.GoldenTaskLeakConflict, error)
	ListGoldenTasks(ctx context.Context, command model.ListGoldenTasksCommand) ([]*model.GoldenTask, error)
	RecordAgentRunLabel(ctx context.Context, tx pgx.Tx, label *model.AgentRunLabel) (*model.AgentRunLabel, error)
	ListAgentRunLabels(ctx context.Context, command model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error)
	RecordTrajectoryDataset(ctx context.Context, tx pgx.Tx, dataset *model.AgentTrajectoryDataset) (*model.AgentTrajectoryDataset, error)
	ReadTrajectoryDataset(ctx context.Context, orgID uuid.UUID, datasetID uuid.UUID) (*model.AgentTrajectoryDataset, error)
	RecordAgentAdapter(ctx context.Context, tx pgx.Tx, adapter *model.AgentAdapter) (*model.AgentAdapter, error)
	ReadAgentAdapter(ctx context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentAdapter, error)
	CompleteAgentAdapterTraining(ctx context.Context, tx pgx.Tx, completion model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error)
	FailAgentAdapterTraining(ctx context.Context, tx pgx.Tx, failure model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error)
	UpdateAgentAdapterPromotion(ctx context.Context, tx pgx.Tx, adapterID uuid.UUID, status model.AgentAdapterStatus, promotionPassed bool) (*model.AgentAdapter, error)
	ReadChampionState(ctx context.Context, orgID uuid.UUID, agentLineage string) (*model.AgentChampionState, error)
	ReadLatestEvalReportForAdapter(ctx context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentEvalReport, error)
	RecordAgentEvalReport(ctx context.Context, tx pgx.Tx, report *model.AgentEvalReport) (*model.AgentEvalReport, error)
	ReadAgentEvalReport(ctx context.Context, orgID uuid.UUID, reportID uuid.UUID) (*model.AgentEvalReport, error)
}

type InferenceVerifier interface {
	ReadAgentSpec(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, agentSpecHash string) (*model.AgentSpecRef, error)
	ReadEndpoint(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, endpointID uuid.UUID) (*model.EndpointRef, error)
	ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, runID uuid.UUID) (*model.AgentTrajectoryRef, error)
}

type AgentRegistryUnitOfWork interface {
	Do(ctx context.Context, fn shareduow.TxFunc) error
}

type AgentRegistryEventBuilder interface {
	AgentChampionUpdatedMessage(state *model.AgentChampionState, binding *model.AgentEndpointBinding) shareduow.OutboundMessage
}

type AgentTaskRunner interface {
	RunAgentTask(ctx context.Context, command model.AgentTaskRunCommand) (model.AgentTaskRunResult, error)
}

type AgentAdapterTrainingDispatcher interface {
	DispatchAgentAdapterTraining(ctx context.Context, request model.AgentAdapterTrainingRequest) (*model.AgentAdapterTrainingResult, error)
}
