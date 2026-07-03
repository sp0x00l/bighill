package app

import (
	"context"

	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
)

type InferenceModelRepository interface {
	UpsertModel(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	ReadByID(ctx context.Context, modelID uuid.UUID) (*model.InferenceModel, error)
}

type InferenceDatasetRepository interface {
	UpsertDataset(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error)
	ReadDataset(ctx context.Context, datasetID uuid.UUID) (*model.InferenceDataset, error)
}

type InferenceRequestRepository interface {
	RecordInferenceRequest(ctx context.Context, request *model.InferenceRequest) error
}

type InferenceFeedbackRepository interface {
	RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error)
	ReadPreferenceDataset(ctx context.Context, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error)
	RecordPreferenceDatasetSnapshot(ctx context.Context, dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error)
}

type PreferenceDatasetWriter interface {
	WritePreferenceDataset(ctx context.Context, dataset *model.PreferenceDataset) (*model.PreferenceDataset, error)
}

type RetrievalClient interface {
	SearchEmbeddings(ctx context.Context, datasetID uuid.UUID, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error)
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
	Generate(ctx context.Context, request model.GenerationRequest) (string, error)
	Provider() string
	Model() string
}
