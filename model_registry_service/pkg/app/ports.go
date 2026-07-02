package app

import (
	"context"

	"model_registry_service/pkg/domain/model"

	"github.com/google/uuid"
)

type ModelRepository interface {
	Close()
	Create(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	ReadByID(ctx context.Context, modelID uuid.UUID) (*model.Model, error)
	ReadByTrainingRunID(ctx context.Context, trainingRunID uuid.UUID) (*model.Model, error)
	UpdateStatus(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error)
	UpdateServingStatus(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, failureReason string) (*model.Model, error)
}

type ModelServingDeployer interface {
	EnsureServedModel(ctx context.Context, registeredModel *model.Model) error
}
