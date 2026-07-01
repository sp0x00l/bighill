package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type ModelRegistryUsecase interface {
	RegisterModel(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	ReadModel(ctx context.Context, modelID uuid.UUID) (*model.Model, error)
	MarkModelReady(ctx context.Context, modelID uuid.UUID, artifactLocation string) (*model.Model, error)
	MarkModelFailed(ctx context.Context, modelID uuid.UUID, failureReason string) (*model.Model, error)
	RecordModelTrainingCompleted(ctx context.Context, trainedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	RecordModelTrainingFailed(ctx context.Context, failedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
}

type modelRegistryUsecase struct {
	repo ModelRepository
}

func NewModelRegistryUsecase(repo ModelRepository) ModelRegistryUsecase {
	log.Trace("NewModelRegistryUsecase")

	return &modelRegistryUsecase{
		repo: repo,
	}
}

func (u *modelRegistryUsecase) RegisterModel(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RegisterModel")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.register",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := validateModelRegistration(registeredModel); err != nil {
		return nil, err
	}
	model.NormalizeModel(registeredModel)
	out, err = u.repo.Create(ctx, registeredModel, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) ReadModel(ctx context.Context, modelID uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase ReadModel")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.read",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if modelID == uuid.Nil {
		return nil, fmt.Errorf("%w: model id is required", domain.ErrValidationFailed)
	}
	return u.repo.ReadByID(ctx, modelID)
}

func (u *modelRegistryUsecase) MarkModelReady(ctx context.Context, modelID uuid.UUID, artifactLocation string) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase MarkModelReady")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.mark_ready",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if modelID == uuid.Nil {
		return nil, fmt.Errorf("%w: model id is required", domain.ErrValidationFailed)
	}
	if strings.TrimSpace(artifactLocation) == "" {
		return nil, fmt.Errorf("%w: artifact location is required", domain.ErrValidationFailed)
	}
	out, err = u.repo.UpdateStatus(ctx, modelID, model.ModelStatusReady, strings.TrimSpace(artifactLocation), "")
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) MarkModelFailed(ctx context.Context, modelID uuid.UUID, failureReason string) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase MarkModelFailed")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.mark_failed",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if modelID == uuid.Nil {
		return nil, fmt.Errorf("%w: model id is required", domain.ErrValidationFailed)
	}
	if strings.TrimSpace(failureReason) == "" {
		return nil, fmt.Errorf("%w: failure reason is required", domain.ErrValidationFailed)
	}
	out, err = u.repo.UpdateStatus(ctx, modelID, model.ModelStatusFailed, "", strings.TrimSpace(failureReason))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) RecordModelTrainingCompleted(ctx context.Context, trainedModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelTrainingCompleted")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_training_completed",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := validateModelRegistration(trainedModel); err != nil {
		return nil, err
	}
	if strings.TrimSpace(trainedModel.ArtifactLocation) == "" {
		return nil, fmt.Errorf("%w: artifact location is required", domain.ErrValidationFailed)
	}
	trainedModel.Status = model.ModelStatusReady
	model.NormalizeModel(trainedModel)
	out, err = u.repo.Create(ctx, trainedModel, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			return u.repo.ReadByTrainingRunID(ctx, trainedModel.TrainingRunID)
		}
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) RecordModelTrainingFailed(ctx context.Context, failedModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelTrainingFailed")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_training_failed",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := validateModelRegistration(failedModel); err != nil {
		return nil, err
	}
	if strings.TrimSpace(failedModel.FailureReason) == "" {
		return nil, fmt.Errorf("%w: failure reason is required", domain.ErrValidationFailed)
	}
	failedModel.Status = model.ModelStatusFailed
	model.NormalizeModel(failedModel)
	out, err = u.repo.Create(ctx, failedModel, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			return u.repo.ReadByTrainingRunID(ctx, failedModel.TrainingRunID)
		}
		return nil, err
	}
	return out, nil
}

func validateModelRegistration(registeredModel *model.Model) error {
	log.Trace("validateModelRegistration")

	if registeredModel == nil {
		return fmt.Errorf("%w: model is required", domain.ErrValidationFailed)
	}
	if registeredModel.TrainingRunID == uuid.Nil {
		return fmt.Errorf("%w: training run id is required", domain.ErrValidationFailed)
	}
	if registeredModel.DatasetID == uuid.Nil {
		return fmt.Errorf("%w: dataset id is required", domain.ErrValidationFailed)
	}
	if strings.TrimSpace(registeredModel.BaseModel) == "" {
		return fmt.Errorf("%w: base model is required", domain.ErrValidationFailed)
	}
	return nil
}
