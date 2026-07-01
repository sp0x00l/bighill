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

type RetrievalClient interface {
	SearchEmbeddings(ctx context.Context, datasetID uuid.UUID, queryText string, topK int) ([]model.RetrievedContext, error)
	Close() error
}

type GenerationAdapter interface {
	Generate(ctx context.Context, request model.GenerationRequest) (string, error)
}
