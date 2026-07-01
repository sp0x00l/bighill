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
