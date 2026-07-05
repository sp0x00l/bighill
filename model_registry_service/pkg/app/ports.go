package app

import (
	"context"

	"model_registry_service/pkg/domain/model"

	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ModelRepository interface {
	Close()
	Create(ctx context.Context, tx pgx.Tx, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	ReadByID(ctx context.Context, modelID uuid.UUID) (*model.Model, error)
	ReadByTrainingRunID(ctx context.Context, trainingRunID uuid.UUID) (*model.Model, error)
	ReadChampion(ctx context.Context, lineage model.Lineage) (*model.Model, error)
	UpdateStatus(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error)
	UpdateServingStatus(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, servingLoadStatus model.ModelLoadStatus, servingTarget string, servingModel string, failureReason string, idempotencyKey uuid.UUID) (*model.Model, bool, error)
	UpdatePromotionDecision(ctx context.Context, tx pgx.Tx, modelID uuid.UUID, status model.ModelStatus, promotionReportURI string, promotionDeltas string, promotionDecision string, failureReason string) (*model.Model, error)
}

type ModelUnitOfWorkAdapter interface {
	Do(ctx context.Context, fn shareduow.TxFunc) error
}

type ModelEventBuilder interface {
	ModelUpdatedMessage(modelRecord *model.Model) shareduow.OutboundMessage
	PromotionRequestedMessage(candidate *model.Model, champion *model.Model) shareduow.OutboundMessage
}

type ModelServingDeployer interface {
	EnsureServedModel(ctx context.Context, registeredModel *model.Model) error
}
