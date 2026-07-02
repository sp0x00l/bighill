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
	RecordModelServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, idempotencyKey uuid.UUID) (*model.Model, error)
}

type modelRegistryUsecase struct {
	repo            ModelRepository
	servingDeployer ModelServingDeployer
}

type ModelRegistryOption func(*modelRegistryUsecase)

func WithModelServingDeployer(deployer ModelServingDeployer) ModelRegistryOption {
	log.Trace("WithModelServingDeployer")

	return func(u *modelRegistryUsecase) {
		u.servingDeployer = deployer
	}
}

func NewModelRegistryUsecase(repo ModelRepository, opts ...ModelRegistryOption) ModelRegistryUsecase {
	log.Trace("NewModelRegistryUsecase")

	u := &modelRegistryUsecase{
		repo: repo,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
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

	trainedModel.Status = model.ModelStatusEvaluated
	if trainedModel.ServingLoadStatus == model.ModelLoadStatusLoaded {
		trainedModel.Status = model.ModelStatusReady
	}
	out, err = u.repo.Create(ctx, trainedModel, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			out, err = u.repo.ReadByTrainingRunID(ctx, trainedModel.TrainingRunID)
			if err != nil {
				return nil, err
			}
			return out, u.ensureServedModel(ctx, out)
		}
		return nil, err
	}
	return out, u.ensureServedModel(ctx, out)
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

func (u *modelRegistryUsecase) RecordModelServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelServingStatus")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_serving_status",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	status := model.ModelStatusEvaluated
	failureReason := ""
	switch servedModelStatus.ServingLoadStatus {
	case model.ModelLoadStatusLoaded:
		status = model.ModelStatusReady
	case model.ModelLoadStatusFailed:
		status = model.ModelStatusFailed
		failureReason = servedModelStatus.FailureReason
	}
	return u.repo.UpdateServingStatus(ctx, servedModelStatus.ModelID, status, servedModelStatus.ServingLoadStatus, servedModelStatus.ServingTarget, servedModelStatus.ServingModel, failureReason, idempotencyKey)
}

func (u *modelRegistryUsecase) ensureServedModel(ctx context.Context, registeredModel *model.Model) error {
	log.Trace("ModelRegistryUsecase ensureServedModel")

	if u.servingDeployer == nil || registeredModel.ServingLoadStatus == model.ModelLoadStatusLoaded || registeredModel.Status == model.ModelStatusFailed {
		return nil
	}
	return u.servingDeployer.EnsureServedModel(ctx, registeredModel)
}
