package app

import (
	"context"
	"fmt"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type InferenceUsecase interface {
	RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error)
	ReadModel(ctx context.Context, modelID uuid.UUID) (*model.InferenceModel, error)
	Generate(ctx context.Context, request model.GenerateRequest) (*model.GenerateResponse, error)
}

type inferenceUsecase struct {
	modelRepository   InferenceModelRepository
	datasetRepository InferenceDatasetRepository
	retrievalClient   RetrievalClient
	generator         GenerationAdapter
}

type InferenceOption func(*inferenceUsecase)

func WithInferenceDatasetRepository(repository InferenceDatasetRepository) InferenceOption {
	log.Trace("WithInferenceDatasetRepository")

	return func(u *inferenceUsecase) {
		u.datasetRepository = repository
	}
}

func WithRetrievalClient(client RetrievalClient) InferenceOption {
	log.Trace("WithRetrievalClient")

	return func(u *inferenceUsecase) {
		u.retrievalClient = client
	}
}

func WithGenerationAdapter(generator GenerationAdapter) InferenceOption {
	log.Trace("WithGenerationAdapter")

	return func(u *inferenceUsecase) {
		u.generator = generator
	}
}

func NewInferenceUsecase(repository InferenceModelRepository, opts ...InferenceOption) InferenceUsecase {
	log.Trace("NewInferenceUsecase")

	u := &inferenceUsecase{
		modelRepository: repository,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
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
	if u.modelRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("inference model repository is required")
	}
	return u.modelRepository.UpsertModel(ctx, inferenceModel, idempotencyKey)
}

func (u *inferenceUsecase) RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error) {
	log.Trace("InferenceUsecase RecordDatasetUpdated")

	if dataset == nil {
		return nil, domain.ErrValidationFailed.Extend("dataset update is required")
	}
	if dataset.DatasetID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("dataset id is required")
	}
	if dataset.UserID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("user id is required")
	}
	if dataset.DatasetVersion <= 0 {
		return nil, domain.ErrValidationFailed.Extend("dataset version is required")
	}
	if idempotencyKey == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("idempotency key is required")
	}
	if u.datasetRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("inference dataset repository is required")
	}
	return u.datasetRepository.UpsertDataset(ctx, dataset, idempotencyKey)
}

func (u *inferenceUsecase) ReadModel(ctx context.Context, modelID uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceUsecase ReadModel")

	if modelID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("model id is required")
	}
	if u.modelRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("inference model repository is required")
	}
	return u.modelRepository.ReadByID(ctx, modelID)
}

func (u *inferenceUsecase) Generate(ctx context.Context, request model.GenerateRequest) (*model.GenerateResponse, error) {
	log.Trace("InferenceUsecase Generate")

	if request.DatasetID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("dataset id is required")
	}
	request.QueryText = strings.TrimSpace(request.QueryText)
	if request.QueryText == "" {
		return nil, domain.ErrValidationFailed.Extend("query text is required")
	}
	if request.TopK <= 0 {
		request.TopK = 5
	}
	if u.datasetRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("inference dataset repository is required")
	}
	if u.retrievalClient == nil {
		return nil, domain.ErrValidationFailed.Extend("retrieval client is required")
	}
	if u.generator == nil {
		return nil, domain.ErrValidationFailed.Extend("generation adapter is required")
	}

	dataset, err := u.datasetRepository.ReadDataset(ctx, request.DatasetID)
	if err != nil {
		return nil, err
	}
	if !dataset.IsRAGReady() {
		return nil, domain.ErrDatasetNotReady.Extend("dataset embeddings are not materialized")
	}

	var inferenceModel *model.InferenceModel
	if request.ModelID != uuid.Nil {
		if u.modelRepository == nil {
			return nil, domain.ErrValidationFailed.Extend("inference model repository is required")
		}
		inferenceModel, err = u.modelRepository.ReadByID(ctx, request.ModelID)
		if err != nil {
			return nil, err
		}
		if inferenceModel.Status != model.ModelStatusReady {
			return nil, domain.ErrValidationFailed.Extend("model is not ready")
		}
		if inferenceModel.DatasetID != request.DatasetID {
			return nil, domain.ErrValidationFailed.Extend("model dataset does not match request dataset")
		}
	}

	contexts, err := u.retrievalClient.SearchEmbeddings(ctx, request.DatasetID, request.QueryText, request.TopK)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", domain.ErrRetrievalFailed, err)
	}
	answer, err := u.generator.Generate(ctx, model.GenerationRequest{
		Dataset:  dataset,
		Model:    inferenceModel,
		Query:    request.QueryText,
		Contexts: contexts,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", domain.ErrGenerationFailed, err)
	}

	modelID := uuid.Nil
	if inferenceModel != nil {
		modelID = inferenceModel.ModelID
	}
	return &model.GenerateResponse{
		DatasetID: request.DatasetID,
		ModelID:   modelID,
		QueryText: request.QueryText,
		Answer:    answer,
		Contexts:  contexts,
	}, nil
}
