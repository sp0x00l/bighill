package app

import (
	"context"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type InferenceUsecase interface {
	RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	ReadModel(ctx context.Context, modelID uuid.UUID) (*model.InferenceModel, error)
}

type inferenceUsecase struct {
	repository InferenceModelRepository
}

func NewInferenceUsecase(repository InferenceModelRepository) InferenceUsecase {
	log.Trace("NewInferenceUsecase")

	return &inferenceUsecase{
		repository: repository,
	}
}

func (u *inferenceUsecase) RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceUsecase RecordModelUpdated")

	if inferenceModel == nil {
		return nil, domain.ErrValidationFailed.Extend("model update is required")
	}
	if inferenceModel.ModelID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("model id is required")
	}
	if inferenceModel.TrainingRunID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("training run id is required")
	}
	if inferenceModel.DatasetID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("dataset id is required")
	}
	if strings.TrimSpace(inferenceModel.ArtifactLocation) == "" && inferenceModel.Status == model.ModelStatusReady {
		return nil, domain.ErrValidationFailed.Extend("artifact location is required for ready models")
	}
	if idempotencyKey == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("idempotency key is required")
	}
	if u.repository == nil {
		return nil, domain.ErrValidationFailed.Extend("inference model repository is required")
	}
	return u.repository.UpsertModel(ctx, inferenceModel, idempotencyKey)
}

func (u *inferenceUsecase) ReadModel(ctx context.Context, modelID uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceUsecase ReadModel")

	if modelID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("model id is required")
	}
	if u.repository == nil {
		return nil, domain.ErrValidationFailed.Extend("inference model repository is required")
	}
	return u.repository.ReadByID(ctx, modelID)
}
