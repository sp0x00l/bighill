package app

import (
	"context"
	"time"

	"inference_service/pkg/domain/model"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type InferenceModelRepository interface {
	UpsertModel(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	ReadByID(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error)
}

type InferenceDatasetRepository interface {
	UpsertDataset(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error)
	ReadDataset(ctx context.Context, orgID uuid.UUID, datasetID uuid.UUID) (*model.InferenceDataset, error)
}

type PublishedEndpointRepository interface {
	UpsertEndpoint(ctx context.Context, endpoint *model.PublishedEndpoint) (*model.PublishedEndpoint, error)
	UpsertEndpointProjection(ctx context.Context, endpoint *model.PublishedEndpoint) (*model.PublishedEndpoint, error)
	SetEndpointDatasets(ctx context.Context, orgID uuid.UUID, endpointID uuid.UUID, datasetIDs []uuid.UUID) (*model.PublishedEndpoint, error)
	ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error)
	ReadEndpoint(ctx context.Context, orgID uuid.UUID, endpointID uuid.UUID) (*model.PublishedEndpoint, error)
	ApplyAgentChampionUpdate(ctx context.Context, update model.AgentChampionUpdate) (*model.PublishedEndpoint, error)
}

type AgentSpecRepository interface {
	UpsertAgentSpec(ctx context.Context, spec *model.AgentSpec) (*model.AgentSpec, error)
	ReadAgentSpecByHash(ctx context.Context, orgID uuid.UUID, contentHash string) (*model.AgentSpec, error)
}

type CapabilityReportRepository interface {
	RecordCapabilityReport(ctx context.Context, report *model.CapabilityReport) (*model.CapabilityReport, error)
	ReadCapabilityReportForEffectiveBase(ctx context.Context, effectiveBaseID string) (*model.CapabilityReport, error)
}

type InferenceRequestRepository interface {
	RecordInferenceRequest(ctx context.Context, request *model.InferenceRequest) error
}

type AgentTrajectoryRepository interface {
	RecordAgentRun(ctx context.Context, run *model.AgentRun) (*model.AgentRun, error)
	RecordAgentStep(ctx context.Context, step *model.AgentStep) (*model.AgentStep, error)
	RecordToolInvocation(ctx context.Context, invocation *model.AgentToolInvocation) (*model.AgentToolInvocation, error)
	ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) (*model.AgentTrajectory, error)
	FailExpiredAgentRuns(ctx context.Context, grace time.Duration) (int64, error)
}

type AgentRunWorkflowStarter interface {
	StartAgentRunWorkflow(ctx context.Context, input AgentRunWorkflowInput) error
}

type UserEventPublisher interface {
	Publish(ctx context.Context, event userevents.Event) error
}

type ToolResolutionContext struct {
	OrgID    uuid.UUID
	UserID   uuid.UUID
	Spec     *model.AgentSpec
	Datasets []*model.InferenceDataset
}

type ToolInvocationContext struct {
	OrgID        uuid.UUID
	UserID       uuid.UUID
	RunID        uuid.UUID
	InvocationID uuid.UUID
	Datasets     []*model.InferenceDataset
}

type ToolInvoker interface {
	Available(ctx context.Context, resolution ToolResolutionContext, bindings []model.ToolBinding) ([]model.ToolSpec, error)
	Invoke(ctx context.Context, invocation ToolInvocationContext, call model.ToolCall) (model.ToolResult, error)
}

type InferenceFeedbackRepository interface {
	RecordFeedback(ctx context.Context, tx pgx.Tx, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error)
	ReadPreferenceDataset(ctx context.Context, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error)
	RecordPreferenceDatasetSnapshot(ctx context.Context, tx pgx.Tx, dataset *model.PreferenceDataset, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error)
	ReadPreferenceDatasetSnapshot(ctx context.Context, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (*model.PreferenceDataset, error)
	ListPreferenceDatasetSnapshots(ctx context.Context, orgID uuid.UUID, filter model.PreferenceDatasetFilter) ([]*model.PreferenceDataset, error)
}

type LineageEvalSetRepository interface {
	ReadActiveEvalSet(ctx context.Context, orgID uuid.UUID, lineageName string) (*model.LineageEvalSet, error)
	FreezeEvalSet(ctx context.Context, tx pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error)
	RegisterCuratedEvalSet(ctx context.Context, tx pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error)
}

type InferenceUnitOfWorkAdapter interface {
	Do(ctx context.Context, fn shareduow.TxFunc) error
}

type PreferenceDatasetWriter interface {
	WritePreferenceDataset(ctx context.Context, dataset *model.PreferenceDataset) (*model.PreferenceDataset, error)
}

type RetrievalClient interface {
	SearchEmbeddings(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error)
	SearchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) ([]model.RetrievedContext, error)
	Close() error
}

type QueryTransformer interface {
	TransformQuery(ctx context.Context, request model.QueryTransformRequest) (*model.QueryTransformResult, error)
}

type ContextPacker interface {
	Pack(ctx context.Context, request model.ContextPackRequest) ([]model.RetrievedContext, error)
}

type Reranker interface {
	Rerank(ctx context.Context, query string, candidates []model.RetrievedContext, topK int) ([]model.RetrievedContext, error)
}

type PromptBuilder interface {
	BuildPrompt(ctx context.Context, request model.PromptBuildRequest) (*model.PromptPackage, error)
}

type GenerationAdapter interface {
	Generate(ctx context.Context, request model.GenerationRequest) (model.GenerationResult, error)
}

type ModelServingLoadTrigger interface {
	TriggerModelLoad(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) error
}
