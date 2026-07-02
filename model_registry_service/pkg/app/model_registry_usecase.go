package app

import (
	"context"
	"errors"

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

	return u.repo.ReadByID(ctx, modelID)
}

func (u *modelRegistryUsecase) MarkModelReady(ctx context.Context, modelID uuid.UUID, artifactLocation string) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase MarkModelReady")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.mark_ready",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	out, err = u.repo.UpdateStatus(ctx, modelID, model.ModelStatusReady, artifactLocation, "")
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

	out, err = u.repo.UpdateStatus(ctx, modelID, model.ModelStatusFailed, "", failureReason)
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

	trainedModel.Status = model.ModelStatusReady
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

	failedModel.Status = model.ModelStatusFailed
	out, err = u.repo.Create(ctx, failedModel, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			return u.repo.ReadByTrainingRunID(ctx, failedModel.TrainingRunID)
		}
		return nil, err
	}
	return out, nil
}
