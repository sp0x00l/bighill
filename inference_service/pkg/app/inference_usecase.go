package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

const maxConcurrentDatasetRetrievals = 4

type InferenceUsecase interface {
	RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error)
	ReadModel(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error)
	ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error)
	PublishEndpoint(ctx context.Context, request model.EndpointPublication) (*model.PublishedEndpoint, error)
	SetEndpointDatasets(ctx context.Context, request model.EndpointDatasetBinding) (*model.PublishedEndpoint, error)
	SetEndpointMergeStrategy(ctx context.Context, request model.EndpointMergeConfiguration) (*model.PublishedEndpoint, error)
	GenerateForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.GenerateRequest) (*model.GenerateResponse, error)
	Generate(ctx context.Context, request model.GenerateRequest) (*model.GenerateResponse, error)
	RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error)
	ExportPreferenceDataset(ctx context.Context, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error)
}

type inferenceUsecase struct {
	modelRepository           InferenceModelRepository
	datasetRepository         InferenceDatasetRepository
	endpointRepository        PublishedEndpointRepository
	requestRepository         InferenceRequestRepository
	feedbackRepository        InferenceFeedbackRepository
	inferenceUnitOfWork       InferenceUnitOfWorkAdapter
	preferenceEventBuilder    PreferenceDatasetEventBuilder
	retrievalClient           RetrievalClient
	queryTransformer          QueryTransformer
	contextPacker             ContextPacker
	reranker                  Reranker
	promptBuilder             PromptBuilder
	generationAdapters        map[string]GenerationAdapter
	preferenceDatasetWriter   PreferenceDatasetWriter
	promptStrategy            model.PromptStrategy
	queryTransformerTimeout   time.Duration
	rerankCandidateMultiplier int
	defaultRAGMergeStrategy   model.RAGMergeStrategy
}

type InferenceOption func(*inferenceUsecase)

func WithInferenceDatasetRepository(repository InferenceDatasetRepository) InferenceOption {
	log.Trace("WithInferenceDatasetRepository")

	return func(u *inferenceUsecase) {
		u.datasetRepository = repository
	}
}

func WithPublishedEndpointRepository(repository PublishedEndpointRepository) InferenceOption {
	log.Trace("WithPublishedEndpointRepository")

	return func(u *inferenceUsecase) {
		u.endpointRepository = repository
	}
}

func WithRetrievalClient(client RetrievalClient) InferenceOption {
	log.Trace("WithRetrievalClient")

	return func(u *inferenceUsecase) {
		u.retrievalClient = client
	}
}

func WithQueryTransformer(transformer QueryTransformer) InferenceOption {
	log.Trace("WithQueryTransformer")

	return func(u *inferenceUsecase) {
		u.queryTransformer = transformer
	}
}

func WithQueryTransformerTimeout(timeout time.Duration) InferenceOption {
	log.Trace("WithQueryTransformerTimeout")

	return func(u *inferenceUsecase) {
		u.queryTransformerTimeout = timeout
	}
}

func WithInferenceRequestRepository(repository InferenceRequestRepository) InferenceOption {
	log.Trace("WithInferenceRequestRepository")

	return func(u *inferenceUsecase) {
		u.requestRepository = repository
	}
}

func WithInferenceFeedbackRepository(repository InferenceFeedbackRepository) InferenceOption {
	log.Trace("WithInferenceFeedbackRepository")

	return func(u *inferenceUsecase) {
		u.feedbackRepository = repository
	}
}

func WithInferenceUnitOfWork(unitOfWork InferenceUnitOfWorkAdapter, preferenceEventBuilder PreferenceDatasetEventBuilder) InferenceOption {
	log.Trace("WithInferenceUnitOfWork")

	return func(u *inferenceUsecase) {
		u.inferenceUnitOfWork = unitOfWork
		u.preferenceEventBuilder = preferenceEventBuilder
	}
}

func WithPreferenceDatasetWriter(writer PreferenceDatasetWriter) InferenceOption {
	log.Trace("WithPreferenceDatasetWriter")

	return func(u *inferenceUsecase) {
		u.preferenceDatasetWriter = writer
	}
}

func WithContextPacker(packer ContextPacker) InferenceOption {
	log.Trace("WithContextPacker")

	return func(u *inferenceUsecase) {
		u.contextPacker = packer
	}
}

func WithReranker(reranker Reranker) InferenceOption {
	log.Trace("WithReranker")

	return func(u *inferenceUsecase) {
		u.reranker = reranker
	}
}

func WithRerankCandidateMultiplier(multiplier int) InferenceOption {
	log.Trace("WithRerankCandidateMultiplier")

	return func(u *inferenceUsecase) {
		u.rerankCandidateMultiplier = multiplier
	}
}

func WithDefaultRAGMergeStrategy(strategy model.RAGMergeStrategy) InferenceOption {
	log.Trace("WithDefaultRAGMergeStrategy")

	return func(u *inferenceUsecase) {
		u.defaultRAGMergeStrategy = strategy
	}
}

func WithPromptBuilder(builder PromptBuilder) InferenceOption {
	log.Trace("WithPromptBuilder")

	return func(u *inferenceUsecase) {
		u.promptBuilder = builder
	}
}

func WithPromptStrategy(strategy model.PromptStrategy) InferenceOption {
	log.Trace("WithPromptStrategy")

	return func(u *inferenceUsecase) {
		u.promptStrategy = strategy
	}
}

func WithGenerationAdapters(adapters map[string]GenerationAdapter) InferenceOption {
	log.Trace("WithGenerationAdapters")

	return func(u *inferenceUsecase) {
		u.generationAdapters = adapters
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

	if inferenceModel != nil {
		ctx = contextForActorOrg(ctx, inferenceModel.UserID, inferenceModel.OrgID)
	}
	record, err := u.modelRepository.UpsertModel(ctx, inferenceModel, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if err := u.upsertEndpointProjection(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (u *inferenceUsecase) RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error) {
	log.Trace("InferenceUsecase RecordDatasetUpdated")

	if dataset != nil {
		ctx = contextForActorOrg(ctx, dataset.UserID, dataset.OrgID)
	}
	return u.datasetRepository.UpsertDataset(ctx, dataset, idempotencyKey)
}

func (u *inferenceUsecase) ReadModel(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error) {
	log.Trace("InferenceUsecase ReadModel")

	ctx = ctxutil.WithOrgID(ctx, orgID)
	return u.modelRepository.ReadByID(ctx, orgID, modelID)
}

func (u *inferenceUsecase) ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error) {
	log.Trace("InferenceUsecase ListEndpoints")

	if u.endpointRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("published endpoint repository is not configured")
	}
	ctx = ctxutil.WithOrgID(ctx, orgID)
	endpoints, err := u.endpointRepository.ListEndpoints(ctx, orgID)
	if err != nil {
		return nil, err
	}
	out := make([]*model.PublishedEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint != nil && endpoint.IsReady() {
			out = append(out, endpoint)
		}
	}
	return out, nil
}

func (u *inferenceUsecase) PublishEndpoint(ctx context.Context, request model.EndpointPublication) (*model.PublishedEndpoint, error) {
	log.Trace("InferenceUsecase PublishEndpoint")

	if u.endpointRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("published endpoint repository is not configured")
	}
	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	inferenceModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, request.ModelID)
	if err != nil {
		return nil, err
	}
	if inferenceModel.OrgID != request.OrgID {
		return nil, domain.ErrModelNotFound
	}
	if len(request.DatasetIDs) == 0 {
		return nil, domain.ErrValidationFailed.Extend("at least one dataset_id is required")
	}
	if err := u.ensureDatasetsExist(ctx, request.OrgID, request.DatasetIDs); err != nil {
		return nil, err
	}
	strategy, err := u.endpointMergeStrategy(request.MergeStrategy)
	if err != nil {
		return nil, err
	}
	if strategy == model.RAGMergeStrategyReranker && u.reranker == nil {
		return nil, domain.ErrValidationFailed.Extend("reranker merge strategy requires a configured reranker")
	}
	displayName := strings.TrimSpace(request.DisplayName)
	if displayName == "" {
		displayName = inferenceModel.Name
	}
	return u.endpointRepository.UpsertEndpoint(ctx, &model.PublishedEndpoint{
		OrgID:           request.OrgID,
		ModelID:         request.ModelID,
		DatasetIDs:      dedupeUUIDs(request.DatasetIDs),
		MergeStrategy:   strategy,
		Status:          model.PublishedEndpointStatusReady,
		DisplayName:     displayName,
		CreatedByUserID: request.UserID,
	})
}

func (u *inferenceUsecase) SetEndpointDatasets(ctx context.Context, request model.EndpointDatasetBinding) (*model.PublishedEndpoint, error) {
	log.Trace("InferenceUsecase SetEndpointDatasets")

	if u.endpointRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("published endpoint repository is not configured")
	}
	if len(request.DatasetIDs) == 0 {
		return nil, domain.ErrValidationFailed.Extend("at least one dataset_id is required")
	}
	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	if err := u.ensureDatasetsExist(ctx, request.OrgID, request.DatasetIDs); err != nil {
		return nil, err
	}
	return u.endpointRepository.SetEndpointDatasets(ctx, request.OrgID, request.EndpointID, dedupeUUIDs(request.DatasetIDs))
}

func (u *inferenceUsecase) SetEndpointMergeStrategy(ctx context.Context, request model.EndpointMergeConfiguration) (*model.PublishedEndpoint, error) {
	log.Trace("InferenceUsecase SetEndpointMergeStrategy")

	if u.endpointRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("published endpoint repository is not configured")
	}
	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	endpoint, err := u.endpointRepository.ReadEndpoint(ctx, request.OrgID, request.EndpointID)
	if err != nil {
		return nil, err
	}
	strategy, err := u.endpointMergeStrategy(request.MergeStrategy)
	if err != nil {
		return nil, err
	}
	if strategy == model.RAGMergeStrategyReranker && u.reranker == nil {
		return nil, domain.ErrValidationFailed.Extend("reranker merge strategy requires a configured reranker")
	}
	endpoint.MergeStrategy = strategy
	return u.endpointRepository.UpsertEndpoint(ctx, endpoint)
}

func (u *inferenceUsecase) GenerateForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.GenerateRequest) (*model.GenerateResponse, error) {
	log.Trace("InferenceUsecase GenerateForEndpoint")

	if u.endpointRepository == nil {
		return nil, domain.ErrValidationFailed.Extend("published endpoint repository is not configured")
	}
	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	endpoint, err := u.endpointRepository.ReadEndpoint(ctx, request.OrgID, endpointID)
	if err != nil {
		return nil, err
	}
	if !endpoint.IsReady() {
		return nil, domain.ErrModelNotReady.Extend("inference endpoint is not ready")
	}
	request.ModelID = endpoint.ModelID
	return u.generate(ctx, request, endpoint)
}

func (u *inferenceUsecase) RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	log.Trace("InferenceUsecase RecordFeedback")

	if feedback != nil {
		ctx = contextForActorOrg(ctx, feedback.UserID, feedback.OrgID)
	}
	record, err := u.recordFeedback(ctx, feedback, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (u *inferenceUsecase) ExportPreferenceDataset(ctx context.Context, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	log.Trace("InferenceUsecase ExportPreferenceDataset")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	dataset, err := u.feedbackRepository.ReadPreferenceDataset(ctx, request)
	if err != nil {
		return nil, err
	}
	if request.OutputURI != "" {
		dataset.PreferenceDatasetID = preferenceDatasetID(dataset)
		dataset.OutputURI = preferenceDatasetOutputURI(request.OutputURI, dataset)
		dataset.EvaluationOutputURI = preferenceDatasetEvaluationOutputURI(request.OutputURI, dataset)
	}
	if dataset.ExampleCount() < request.MinExamples {
		return dataset, nil
	}
	if u.preferenceDatasetWriter == nil {
		return dataset, nil
	}
	written, err := u.preferenceDatasetWriter.WritePreferenceDataset(ctx, dataset)
	if err != nil {
		return nil, err
	}
	if written != nil && written.Exported {
		written.PreferenceDatasetID = preferenceDatasetID(written)
		written.Format = preferenceDatasetFormat(written.Format)
		written.EligibilityPolicy = preferenceDatasetEligibilityPolicy(written.EligibilityPolicy)
		written.MinExamples = request.MinExamples
		written.Limit = request.Limit
		return u.recordPreferenceDatasetSnapshot(ctx, written, request)
	}
	return written, nil
}

func (u *inferenceUsecase) recordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	log.Trace("InferenceUsecase recordFeedback")

	var record *model.InferenceFeedback
	err := u.inferenceUnitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		out, err := u.feedbackRepository.RecordFeedback(ctx, tx, feedback, idempotencyKey)
		if err != nil {
			return err
		}
		record = out
		return nil
	})
	return record, err
}

func (u *inferenceUsecase) recordPreferenceDatasetSnapshot(ctx context.Context, dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	log.Trace("InferenceUsecase recordPreferenceDatasetSnapshot")

	var record *model.PreferenceDataset
	err := u.inferenceUnitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		out, err := u.feedbackRepository.RecordPreferenceDatasetSnapshot(ctx, tx, dataset, request)
		if err != nil {
			return err
		}
		if err := enqueue(u.preferenceEventBuilder.PreferenceDatasetReadyMessage(out, request)); err != nil {
			return fmt.Errorf("enqueue preference dataset ready: %w", err)
		}
		record = out
		return nil
	})
	return record, err
}

func (u *inferenceUsecase) Generate(ctx context.Context, request model.GenerateRequest) (response *model.GenerateResponse, err error) {
	log.Trace("InferenceUsecase Generate")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	return u.generate(ctx, request, nil)
}

func (u *inferenceUsecase) generate(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint) (response *model.GenerateResponse, err error) {
	log.Trace("InferenceUsecase generate")

	startedAt := time.Now()

	var dataset *model.InferenceDataset
	var inferenceModel *model.InferenceModel
	var contexts []model.RetrievedContext
	var promptText string
	var answerText string

	inferenceModel, err = u.modelRepository.ReadByID(ctx, request.OrgID, request.ModelID)
	if err != nil {
		return nil, err
	}
	datasetIDs, err := u.generateDatasetIDs(request, endpoint)
	if err != nil {
		return nil, err
	}
	datasets, err := u.readGenerateDatasets(ctx, request.OrgID, datasetIDs)
	if err != nil {
		return nil, err
	}
	if len(datasets) == 0 {
		return nil, domain.ErrDatasetNotFound
	}
	dataset = datasets[0]
	request.DatasetID = dataset.DatasetID

	generationProtocol := strings.TrimSpace(inferenceModel.ServingProtocol.String())
	generationModel := strings.TrimSpace(inferenceModel.ServingModel)
	if inferenceModel.Status != model.ModelStatusReady {
		err = domain.ErrModelNotReady.Extend("model is not ready")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if inferenceModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		err = domain.ErrModelNotReady.Extend("model is not loaded by serving layer")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if generationProtocol == "" {
		err = domain.ErrModelNotReady.Extend("serving protocol is required")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	generator := u.generationAdapters[generationProtocol]
	if generator == nil {
		err = domain.ErrModelNotReady.Extend(fmt.Sprintf("serving protocol %q is not supported", generationProtocol))
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if inferenceModel.RequiresDatasetMatch() && !containsUUID(datasetIDs, inferenceModel.DatasetID) {
		err = domain.ErrModelMismatch.Extend("model dataset does not match request dataset")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	// Raw Generate is an explicit single-dataset request, so fail closed if that dataset is not usable.
	// Endpoint Generate is a model-level binding and can degrade by skipping unusable bound datasets below.
	if endpoint == nil {
		if !dataset.IsRAGReady() {
			err = domain.ErrDatasetNotReady.Extend("dataset embeddings are not materialized")
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
		if !dataset.HasSupportedEmbeddingProvider() {
			err = domain.ErrDatasetNotReady.Extend(fmt.Sprintf("dataset embedding provider %q is not supported", dataset.EmbeddingProvider))
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
	}
	readyDatasets := filterReadyRAGDatasets(ctx, datasets)
	if len(readyDatasets) == 0 {
		err = domain.ErrDatasetNotReady.Extend("no endpoint datasets have materialized embeddings")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	dataset = readyDatasets[0]
	request.DatasetID = dataset.DatasetID

	retrievalQuery := request.QueryText
	retrievalFilters := copyMetadataFilters(request.MetadataFilters)
	if u.queryTransformer != nil {
		transformCtx := ctx
		cancel := func() {}
		if u.queryTransformerTimeout > 0 {
			transformCtx, cancel = context.WithTimeout(ctx, u.queryTransformerTimeout)
		}
		transformed, transformErr := u.queryTransformer.TransformQuery(transformCtx, model.QueryTransformRequest{
			RequestID:       request.RequestID,
			UserID:          request.UserID,
			OrgID:           request.OrgID,
			DatasetID:       request.DatasetID,
			ModelID:         request.ModelID,
			Model:           inferenceModel,
			QueryText:       request.QueryText,
			MetadataFilters: copyMetadataFilters(request.MetadataFilters),
		})
		cancel()
		if transformErr != nil {
			log.WithContext(ctx).WithError(transformErr).Warn("query transform failed; falling back to raw query")
		} else if transformed != nil {
			if strings.TrimSpace(transformed.QueryText) != "" {
				retrievalQuery = strings.TrimSpace(transformed.QueryText)
			}
			retrievalFilters = mergeMetadataFilters(request.MetadataFilters, transformed.MetadataFilters)
		}
	}

	mergeStrategy, err := u.generateMergeStrategy(endpoint)
	if err != nil {
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	candidateK := request.TopK
	if mergeStrategy == model.RAGMergeStrategyReranker && u.reranker != nil && u.rerankCandidateMultiplier > 1 {
		candidateK = request.TopK * u.rerankCandidateMultiplier
	}

	contexts, err = u.retrieveFromDatasets(ctx, request.UserID, readyDatasets, retrievalQuery, candidateK, retrievalFilters)
	if err != nil {
		err = fmt.Errorf("%w: %w", domain.ErrRetrievalFailed, err)
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	switch mergeStrategy {
	case model.RAGMergeStrategyReranker:
		if u.reranker == nil {
			err = domain.ErrRerankFailed.Extend("reranker merge strategy requires a configured reranker")
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
		contexts, err = u.reranker.Rerank(ctx, retrievalQuery, contexts, request.TopK)
		if err != nil {
			err = fmt.Errorf("%w: %w", domain.ErrRerankFailed, err)
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
	case model.RAGMergeStrategyScoreNormalized:
		contexts = scoreNormalizedMerge(contexts, request.TopK)
	default:
		err = domain.ErrValidationFailed.Extend(fmt.Sprintf("unsupported rag merge strategy %q", mergeStrategy))
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	contexts, err = u.contextPacker.Pack(ctx, model.ContextPackRequest{
		Query:    retrievalQuery,
		Contexts: contexts,
	})
	if err != nil {
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	dataset = primaryDatasetForContexts(readyDatasets, contexts)
	request.DatasetID = dataset.DatasetID
	promptPackage, err := u.promptBuilder.BuildPrompt(ctx, model.PromptBuildRequest{
		Query:    request.QueryText,
		Dataset:  dataset,
		Model:    inferenceModel,
		Contexts: contexts,
	})
	if err != nil {
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	promptText = promptPackage.Prompt
	answer, err := generator.Generate(ctx, model.GenerationRequest{
		RequestID:             request.RequestID,
		Dataset:               dataset,
		Model:                 inferenceModel,
		Query:                 request.QueryText,
		Prompt:                promptPackage.Prompt,
		PromptStrategyVersion: promptPackage.Strategy.Version,
		Contexts:              promptPackage.Contexts,
	})
	if err != nil {
		err = fmt.Errorf("%w: %w", domain.ErrGenerationFailed, err)
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	answerText = answer

	response = &model.GenerateResponse{
		RequestID:             request.RequestID,
		OrgID:                 request.OrgID,
		DatasetID:             request.DatasetID,
		DatasetIDs:            datasetIDsFromModels(readyDatasets),
		ModelID:               inferenceModel.ModelID,
		QueryText:             request.QueryText,
		Answer:                answer,
		Contexts:              contexts,
		PromptStrategyVersion: promptPackage.Strategy.Version,
		GenerationProtocol:    generationProtocol,
		GenerationModel:       generationModel,
		RAGMergeStrategy:      mergeStrategy,
	}
	if err := u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusCompleted, ""); err != nil {
		return nil, err
	}
	return response, nil
}

func (u *inferenceUsecase) generateDatasetIDs(request model.GenerateRequest, endpoint *model.PublishedEndpoint) ([]uuid.UUID, error) {
	log.Trace("InferenceUsecase generateDatasetIDs")

	if endpoint != nil {
		datasetIDs := dedupeUUIDs(endpoint.DatasetIDs)
		if len(datasetIDs) == 0 {
			return nil, domain.ErrValidationFailed.Extend("published endpoint has no datasets")
		}
		return datasetIDs, nil
	}
	if request.DatasetID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("dataset_id is required")
	}
	return []uuid.UUID{request.DatasetID}, nil
}

func (u *inferenceUsecase) readGenerateDatasets(ctx context.Context, orgID uuid.UUID, datasetIDs []uuid.UUID) ([]*model.InferenceDataset, error) {
	log.Trace("InferenceUsecase readGenerateDatasets")

	datasets := make([]*model.InferenceDataset, 0, len(datasetIDs))
	for _, datasetID := range datasetIDs {
		if datasetID == uuid.Nil {
			return nil, domain.ErrValidationFailed.Extend("dataset_id is required")
		}
		dataset, err := u.datasetRepository.ReadDataset(ctx, orgID, datasetID)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, dataset)
	}
	return datasets, nil
}

func (u *inferenceUsecase) ensureDatasetsExist(ctx context.Context, orgID uuid.UUID, datasetIDs []uuid.UUID) error {
	log.Trace("InferenceUsecase ensureDatasetsExist")

	_, err := u.readGenerateDatasets(ctx, orgID, dedupeUUIDs(datasetIDs))
	return err
}

func (u *inferenceUsecase) endpointMergeStrategy(strategy model.RAGMergeStrategy) (model.RAGMergeStrategy, error) {
	log.Trace("InferenceUsecase endpointMergeStrategy")

	if strategy != "" {
		return model.ToRAGMergeStrategy(strategy.String())
	}
	if u.defaultRAGMergeStrategy != "" {
		return model.ToRAGMergeStrategy(u.defaultRAGMergeStrategy.String())
	}
	return model.RAGMergeStrategyReranker, nil
}

func (u *inferenceUsecase) generateMergeStrategy(endpoint *model.PublishedEndpoint) (model.RAGMergeStrategy, error) {
	log.Trace("InferenceUsecase generateMergeStrategy")

	if endpoint != nil {
		return u.endpointMergeStrategy(endpoint.MergeStrategy)
	}
	if u.defaultRAGMergeStrategy != "" {
		return model.ToRAGMergeStrategy(u.defaultRAGMergeStrategy.String())
	}
	if u.reranker != nil {
		return model.RAGMergeStrategyReranker, nil
	}
	return model.RAGMergeStrategyScoreNormalized, nil
}

func filterReadyRAGDatasets(ctx context.Context, datasets []*model.InferenceDataset) []*model.InferenceDataset {
	log.Trace("filterReadyRAGDatasets")

	ready := make([]*model.InferenceDataset, 0, len(datasets))
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		if !dataset.IsRAGReady() {
			log.WithContext(ctx).WithField("dataset_id", dataset.DatasetID).Warn("skipping endpoint dataset without materialized embeddings")
			continue
		}
		if !dataset.HasSupportedEmbeddingProvider() {
			log.WithContext(ctx).
				WithField("dataset_id", dataset.DatasetID).
				WithField("embedding_provider", dataset.EmbeddingProvider).
				Warn("skipping endpoint dataset with unsupported embedding provider")
			continue
		}
		ready = append(ready, dataset)
	}
	return ready
}

func (u *inferenceUsecase) retrieveFromDatasets(ctx context.Context, userID uuid.UUID, datasets []*model.InferenceDataset, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error) {
	log.Trace("InferenceUsecase retrieveFromDatasets")

	contexts := make([]model.RetrievedContext, 0, topK*len(datasets))
	errs := make([]error, 0)
	successfulRetrievals := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	limit := min(len(datasets), maxConcurrentDatasetRetrievals)
	if limit <= 0 {
		return nil, nil
	}
	sem := make(chan struct{}, limit)
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		wg.Add(1)
		go func(dataset *model.InferenceDataset) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			datasetContexts, err := u.retrievalClient.SearchEmbeddings(ctx, userID, dataset.DatasetID, queryText, topK, metadataFilters)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("dataset %s: %w", dataset.DatasetID, err))
				mu.Unlock()
				log.WithContext(ctx).WithError(err).WithField("dataset_id", dataset.DatasetID).Warn("retrieval failed for endpoint dataset")
				return
			}
			for i := range datasetContexts {
				if datasetContexts[i].DatasetID == uuid.Nil {
					datasetContexts[i].DatasetID = dataset.DatasetID
				}
				if datasetContexts[i].DatasetID != dataset.DatasetID {
					mu.Lock()
					errs = append(errs, fmt.Errorf("retrieval returned context for dataset %s while querying dataset %s", datasetContexts[i].DatasetID, dataset.DatasetID))
					mu.Unlock()
					return
				}
			}
			mu.Lock()
			successfulRetrievals++
			contexts = append(contexts, datasetContexts...)
			mu.Unlock()
		}(dataset)
	}
	wg.Wait()
	if successfulRetrievals == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if len(errs) > 0 {
		log.WithContext(ctx).WithField("failed_dataset_count", len(errs)).Warn("continuing generation with partial endpoint retrieval results")
	}
	return contexts, nil
}

func scoreNormalizedMerge(contexts []model.RetrievedContext, topK int) []model.RetrievedContext {
	log.Trace("scoreNormalizedMerge")

	if len(contexts) == 0 || topK <= 0 {
		return nil
	}
	minScore := contexts[0].Similarity
	maxScore := contexts[0].Similarity
	for _, context := range contexts {
		score := context.Similarity
		if score < minScore {
			minScore = score
		}
		if score > maxScore {
			maxScore = score
		}
	}
	merged := make([]model.RetrievedContext, len(contexts))
	copy(merged, contexts)
	for i, context := range merged {
		if maxScore == minScore {
			merged[i].RerankScore = 1
			continue
		}
		merged[i].RerankScore = (context.Similarity - minScore) / (maxScore - minScore)
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].RerankScore == merged[j].RerankScore {
			return merged[i].Similarity > merged[j].Similarity
		}
		return merged[i].RerankScore > merged[j].RerankScore
	})
	if len(merged) > topK {
		return merged[:topK]
	}
	return merged
}

func primaryDatasetForContexts(datasets []*model.InferenceDataset, contexts []model.RetrievedContext) *model.InferenceDataset {
	log.Trace("primaryDatasetForContexts")

	if len(datasets) == 0 {
		return nil
	}
	if len(contexts) == 0 {
		return datasets[0]
	}
	contextDatasetID := contexts[0].DatasetID
	for _, dataset := range datasets {
		if dataset != nil && dataset.DatasetID == contextDatasetID {
			return dataset
		}
	}
	return datasets[0]
}

func datasetIDsFromModels(datasets []*model.InferenceDataset) []uuid.UUID {
	log.Trace("datasetIDsFromModels")

	ids := make([]uuid.UUID, 0, len(datasets))
	for _, dataset := range datasets {
		if dataset != nil && dataset.DatasetID != uuid.Nil {
			ids = append(ids, dataset.DatasetID)
		}
	}
	return ids
}

func dedupeUUIDs(ids []uuid.UUID) []uuid.UUID {
	log.Trace("dedupeUUIDs")

	if len(ids) == 0 {
		return nil
	}
	seen := map[uuid.UUID]struct{}{}
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func containsUUID(ids []uuid.UUID, needle uuid.UUID) bool {
	log.Trace("containsUUID")

	for _, id := range ids {
		if id == needle {
			return true
		}
	}
	return false
}

func preferenceDatasetOutputURI(uriTemplate string, dataset *model.PreferenceDataset) string {
	log.Trace("preferenceDatasetOutputURI")

	outputURI := renderPreferenceDatasetOutputURI(uriTemplate, dataset)
	if strings.Contains(uriTemplate, "{preference_dataset_id}") {
		return outputURI
	}
	return suffixPreferenceDatasetOutputURI(outputURI, dataset.PreferenceDatasetID.String())
}

func preferenceDatasetEvaluationOutputURI(uriTemplate string, dataset *model.PreferenceDataset) string {
	log.Trace("preferenceDatasetEvaluationOutputURI")

	outputURI := renderPreferenceDatasetOutputURI(uriTemplate, dataset)
	evalSuffix := dataset.PreferenceDatasetID.String() + "-eval"
	if strings.Contains(uriTemplate, "{preference_dataset_id}") {
		return strings.ReplaceAll(outputURI, dataset.PreferenceDatasetID.String(), evalSuffix)
	}
	return suffixPreferenceDatasetOutputURI(outputURI, evalSuffix)
}

func renderPreferenceDatasetOutputURI(uriTemplate string, dataset *model.PreferenceDataset) string {
	log.Trace("renderPreferenceDatasetOutputURI")

	outputURI := strings.TrimSpace(uriTemplate)
	outputURI = strings.ReplaceAll(outputURI, "{request_id}", dataset.RequestID.String())
	outputURI = strings.ReplaceAll(outputURI, "{dataset_id}", dataset.DatasetID.String())
	outputURI = strings.ReplaceAll(outputURI, "{model_id}", dataset.ModelID.String())
	outputURI = strings.ReplaceAll(outputURI, "{preference_dataset_id}", dataset.PreferenceDatasetID.String())
	outputURI = strings.ReplaceAll(outputURI, "{parent_model_version}", fmt.Sprintf("%d", dataset.ParentModelVersion))
	return outputURI
}

func suffixPreferenceDatasetOutputURI(outputURI string, suffix string) string {
	log.Trace("suffixPreferenceDatasetOutputURI")

	if strings.HasSuffix(outputURI, ".jsonl") {
		return strings.TrimSuffix(outputURI, ".jsonl") + "-" + suffix + ".jsonl"
	}
	return strings.TrimRight(outputURI, "/") + "/" + suffix + ".jsonl"
}

func preferenceDatasetID(dataset *model.PreferenceDataset) uuid.UUID {
	log.Trace("preferenceDatasetID")

	if dataset.PreferenceDatasetID != uuid.Nil {
		return dataset.PreferenceDatasetID
	}
	parts := []string{
		"preference-dataset",
		dataset.OrgID.String(),
		dataset.DatasetID.String(),
		dataset.ModelID.String(),
		dataset.ParentModelKind.String(),
		strings.TrimSpace(dataset.ParentArtifactURI),
		strings.TrimSpace(dataset.ParentArtifactChecksum),
		strings.TrimSpace(dataset.ParentAdapterURI),
		strings.TrimSpace(dataset.ParentBaseModel),
		fmt.Sprintf("%d", dataset.ParentModelVersion),
	}
	for _, example := range dataset.Examples {
		parts = append(parts, example.PreferenceExampleID.String())
		parts = append(parts, example.Split)
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, ":")))
}

func (u *inferenceUsecase) upsertEndpointProjection(ctx context.Context, inferenceModel *model.InferenceModel) error {
	log.Trace("InferenceUsecase upsertEndpointProjection")

	if u.endpointRepository == nil || inferenceModel == nil {
		return nil
	}
	if inferenceModel.OrgID == uuid.Nil || inferenceModel.UserID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("published endpoint requires org_id and user_id")
	}
	if inferenceModel.DatasetID == uuid.Nil {
		return nil
	}
	status := model.PublishedEndpointStatusDisabled
	if inferenceModel.Status == model.ModelStatusReady && inferenceModel.ServingLoadStatus == model.ModelLoadStatusLoaded {
		status = model.PublishedEndpointStatusReady
	}
	strategy, err := u.endpointMergeStrategy("")
	if err != nil {
		return err
	}
	_, err = u.endpointRepository.UpsertEndpoint(ctx, &model.PublishedEndpoint{
		OrgID:           inferenceModel.OrgID,
		ModelID:         inferenceModel.ModelID,
		DatasetIDs:      []uuid.UUID{inferenceModel.DatasetID},
		MergeStrategy:   strategy,
		Status:          status,
		DisplayName:     inferenceModel.Name,
		CreatedByUserID: inferenceModel.UserID,
	})
	return err
}

func contextForActorOrg(ctx context.Context, userID uuid.UUID, orgID uuid.UUID) context.Context {
	log.Trace("contextForActorOrg")

	if orgID == uuid.Nil && userID == uuid.Nil {
		return ctx
	}
	return ctxutil.WithActorOrg(ctx, userID, orgID)
}

func preferenceDatasetFormat(format string) string {
	log.Trace("preferenceDatasetFormat")

	format = strings.TrimSpace(format)
	if format != "" {
		return format
	}
	return "DPO_JSONL"
}

func preferenceDatasetEligibilityPolicy(policy string) string {
	log.Trace("preferenceDatasetEligibilityPolicy")

	policy = strings.TrimSpace(policy)
	if policy != "" {
		return policy
	}
	return "complete_rejected_pairs_train_eval_split_v1"
}

func copyMetadataFilters(filters map[string]string) map[string]string {
	if len(filters) == 0 {
		return nil
	}
	out := make(map[string]string, len(filters))
	for k, v := range filters {
		out[k] = v
	}
	return out
}

func mergeMetadataFilters(base map[string]string, overrides map[string]string) map[string]string {
	out := copyMetadataFilters(base)
	if len(overrides) == 0 {
		return out
	}
	if out == nil {
		out = make(map[string]string, len(overrides))
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

func (u *inferenceUsecase) recordInferenceRequest(ctx context.Context, request model.GenerateRequest, dataset *model.InferenceDataset, inferenceModel *model.InferenceModel, contexts []model.RetrievedContext, promptText string, answerText string, startedAt time.Time, generationProtocol string, generationModel string, status model.InferenceRequestStatus, errorMessage string) error {
	log.Trace("InferenceUsecase recordInferenceRequest")

	record, err := inferenceRequestRecord(request, dataset, inferenceModel, contexts, promptText, answerText, u.promptStrategy, startedAt, generationProtocol, generationModel, status, errorMessage)
	if err != nil {
		return err
	}
	if err := u.requestRepository.RecordInferenceRequest(ctx, record); err != nil {
		return fmt.Errorf("record inference request: %w", err)
	}
	return nil
}

func inferenceRequestRecord(request model.GenerateRequest, dataset *model.InferenceDataset, inferenceModel *model.InferenceModel, contexts []model.RetrievedContext, promptText string, answerText string, strategy model.PromptStrategy, startedAt time.Time, generationProtocol string, generationModel string, status model.InferenceRequestStatus, errorMessage string) (*model.InferenceRequest, error) {
	log.Trace("inferenceRequestRecord")

	metadataFilters, err := marshalMetadataFilters(request.MetadataFilters)
	if err != nil {
		return nil, err
	}
	retrievedContextIDs, err := marshalRetrievedContextIDs(contexts)
	if err != nil {
		return nil, err
	}
	retrievedContexts, err := marshalRetrievedContexts(contexts)
	if err != nil {
		return nil, err
	}
	return &model.InferenceRequest{
		RequestID:             request.RequestID,
		UserID:                request.UserID,
		OrgID:                 request.OrgID,
		DatasetID:             dataset.DatasetID,
		ModelID:               inferenceModel.ModelID,
		EmbeddingSnapshotID:   dataset.EmbeddingSnapshotID,
		QueryText:             request.QueryText,
		TopK:                  request.TopK,
		MetadataFilters:       metadataFilters,
		RetrievedContextIDs:   retrievedContextIDs,
		RetrievedContexts:     retrievedContexts,
		PromptText:            promptText,
		AnswerText:            answerText,
		PromptStrategyVersion: strategy.Version,
		GenerationProtocol:    generationProtocol,
		GenerationModel:       generationModel,
		LatencyMs:             time.Since(startedAt).Milliseconds(),
		Status:                status,
		ErrorMessage:          errorMessage,
	}, nil
}

func marshalMetadataFilters(filters map[string]string) (string, error) {
	log.Trace("marshalMetadataFilters")

	raw, err := json.Marshal(filters)
	if err != nil {
		return "", fmt.Errorf("marshal metadata filters: %w", err)
	}
	return string(raw), nil
}

func marshalRetrievedContextIDs(contexts []model.RetrievedContext) (string, error) {
	log.Trace("marshalRetrievedContextIDs")

	ids := make([]string, 0, len(contexts))
	for i, retrieved := range contexts {
		if retrieved.EmbeddingRecordID == uuid.Nil {
			return "", fmt.Errorf("retrieved context %d has empty embedding record id", i)
		}
		ids = append(ids, retrieved.EmbeddingRecordID.String())
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return "", fmt.Errorf("marshal retrieved context ids: %w", err)
	}
	return string(raw), nil
}

func marshalRetrievedContexts(contexts []model.RetrievedContext) (string, error) {
	log.Trace("marshalRetrievedContexts")

	type retrievedContextAuditRecord struct {
		EmbeddingRecordID   string  `json:"embedding_record_id"`
		EmbeddingSnapshotID string  `json:"embedding_snapshot_id"`
		ChunkIndex          int     `json:"chunk_index"`
		SourceText          string  `json:"source_text"`
		Distance            float64 `json:"distance"`
		Similarity          float64 `json:"similarity"`
		RerankScore         float64 `json:"rerank_score,omitempty"`
	}

	records := make([]retrievedContextAuditRecord, 0, len(contexts))
	for i, retrieved := range contexts {
		if retrieved.EmbeddingRecordID == uuid.Nil {
			return "", fmt.Errorf("retrieved context %d has empty embedding record id", i)
		}
		if retrieved.EmbeddingSnapshotID == uuid.Nil {
			return "", fmt.Errorf("retrieved context %d has empty embedding snapshot id", i)
		}
		records = append(records, retrievedContextAuditRecord{
			EmbeddingRecordID:   retrieved.EmbeddingRecordID.String(),
			EmbeddingSnapshotID: retrieved.EmbeddingSnapshotID.String(),
			ChunkIndex:          retrieved.ChunkIndex,
			SourceText:          retrieved.SourceText,
			Distance:            retrieved.Distance,
			Similarity:          retrieved.Similarity,
			RerankScore:         retrieved.RerankScore,
		})
	}
	raw, err := json.Marshal(records)
	if err != nil {
		return "", fmt.Errorf("marshal retrieved contexts: %w", err)
	}
	return string(raw), nil
}
