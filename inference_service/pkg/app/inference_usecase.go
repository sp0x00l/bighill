package app

import (
	"context"
	"crypto/sha256"
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
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	maxConcurrentDatasetRetrievals = 4
	minimumFrozenEvalExamples      = 1
	capabilityProbeTimeout         = 3 * time.Second
)

type InferenceUsecase interface {
	RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error)
	ReadModel(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error)
	PublishAgentSpec(ctx context.Context, request model.AgentSpecPublication) (*model.AgentSpec, error)
	ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error)
	PublishEndpoint(ctx context.Context, request model.EndpointPublication) (*model.PublishedEndpoint, error)
	SetEndpointDatasets(ctx context.Context, request model.EndpointDatasetBinding) (*model.PublishedEndpoint, error)
	SetEndpointMergeStrategy(ctx context.Context, request model.EndpointMergeConfiguration) (*model.PublishedEndpoint, error)
	GenerateForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.GenerateRequest) (*model.GenerateResponse, error)
	Generate(ctx context.Context, request model.GenerateRequest) (*model.GenerateResponse, error)
	RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error)
	BuildPreferenceDatasetForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error)
	ReadPreferenceDataset(ctx context.Context, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (*model.PreferenceDataset, error)
	ListPreferenceDatasets(ctx context.Context, orgID uuid.UUID, filter model.PreferenceDatasetFilter) ([]*model.PreferenceDataset, error)
	BuildPreferenceDataset(ctx context.Context, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error)
	ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, runID uuid.UUID) (*model.AgentTrajectory, error)
	ReapExpiredAgentRuns(ctx context.Context, safetyMultiplier int) (int64, error)
}

type inferenceUsecase struct {
	modelRepository            InferenceModelRepository
	datasetRepository          InferenceDatasetRepository
	endpointRepository         PublishedEndpointRepository
	agentSpecRepository        AgentSpecRepository
	capabilityReportRepository CapabilityReportRepository
	requestRepository          InferenceRequestRepository
	trajectoryRepository       AgentTrajectoryRepository
	feedbackRepository         InferenceFeedbackRepository
	lineageEvalSetRepository   LineageEvalSetRepository
	inferenceUnitOfWork        InferenceUnitOfWorkAdapter
	retrievalClient            RetrievalClient
	queryTransformer           QueryTransformer
	contextPacker              ContextPacker
	reranker                   Reranker
	promptBuilder              PromptBuilder
	generationAdapters         map[string]GenerationAdapter
	toolInvoker                ToolInvoker
	userEventPublisher         UserEventPublisher
	modelServingLoadTrigger    ModelServingLoadTrigger
	modelServingLoadTimeout    time.Duration
	modelServingLoadPoll       time.Duration
	preferenceDatasetWriter    PreferenceDatasetWriter
	promptStrategy             model.PromptStrategy
	queryTransformerTimeout    time.Duration
	rerankCandidateMultiplier  int
	defaultRAGMergeStrategy    model.RAGMergeStrategy
}

type preferenceEvalSetFreeze struct {
	evalSet    *model.LineageEvalSet
	exampleIDs []uuid.UUID
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

func WithAgentSpecRepository(repository AgentSpecRepository) InferenceOption {
	log.Trace("WithAgentSpecRepository")

	return func(u *inferenceUsecase) {
		u.agentSpecRepository = repository
	}
}

func WithCapabilityReportRepository(repository CapabilityReportRepository) InferenceOption {
	log.Trace("WithCapabilityReportRepository")

	return func(u *inferenceUsecase) {
		u.capabilityReportRepository = repository
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

func WithAgentTrajectoryRepository(repository AgentTrajectoryRepository) InferenceOption {
	log.Trace("WithAgentTrajectoryRepository")

	return func(u *inferenceUsecase) {
		u.trajectoryRepository = repository
	}
}

func WithInferenceFeedbackRepository(repository InferenceFeedbackRepository) InferenceOption {
	log.Trace("WithInferenceFeedbackRepository")

	return func(u *inferenceUsecase) {
		u.feedbackRepository = repository
	}
}

func WithLineageEvalSetRepository(repository LineageEvalSetRepository) InferenceOption {
	log.Trace("WithLineageEvalSetRepository")

	return func(u *inferenceUsecase) {
		u.lineageEvalSetRepository = repository
	}
}

func WithInferenceUnitOfWork(unitOfWork InferenceUnitOfWorkAdapter) InferenceOption {
	log.Trace("WithInferenceUnitOfWork")

	return func(u *inferenceUsecase) {
		u.inferenceUnitOfWork = unitOfWork
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

func WithToolInvoker(invoker ToolInvoker) InferenceOption {
	log.Trace("WithToolInvoker")

	return func(u *inferenceUsecase) {
		u.toolInvoker = invoker
	}
}

func WithUserEventPublisher(publisher UserEventPublisher) InferenceOption {
	log.Trace("WithUserEventPublisher")

	return func(u *inferenceUsecase) {
		u.userEventPublisher = publisher
	}
}

func WithModelServingLoadTrigger(trigger ModelServingLoadTrigger, timeout time.Duration, pollInterval time.Duration) InferenceOption {
	log.Trace("WithModelServingLoadTrigger")

	return func(u *inferenceUsecase) {
		u.modelServingLoadTrigger = trigger
		u.modelServingLoadTimeout = timeout
		u.modelServingLoadPoll = pollInterval
	}
}

func NewInferenceUsecase(repository InferenceModelRepository, opts ...InferenceOption) InferenceUsecase {
	log.Trace("NewInferenceUsecase")

	u := &inferenceUsecase{
		modelRepository:    repository,
		userEventPublisher: userevents.NewNoopPublisher(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
}

func (u *inferenceUsecase) RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (record *model.InferenceModel, err error) {
	log.Trace("InferenceUsecase RecordModelUpdated")

	if inferenceModel != nil {
		ctx = contextForActorOrg(ctx, inferenceModel.UserID, inferenceModel.OrgID)
	}
	ctx, span := startInferenceSpan(ctx, "model.record_updated")
	defer endInferenceSpanOnReturn(ctx, span, &err)

	record, err = u.modelRepository.UpsertModel(ctx, inferenceModel, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if err := u.upsertEndpointProjection(ctx, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (u *inferenceUsecase) RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (record *model.InferenceDataset, err error) {
	log.Trace("InferenceUsecase RecordDatasetUpdated")

	if dataset != nil {
		ctx = contextForActorOrg(ctx, dataset.UserID, dataset.OrgID)
	}
	ctx, span := startInferenceSpan(ctx, "dataset.record_updated")
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.datasetRepository.UpsertDataset(ctx, dataset, idempotencyKey)
}

func (u *inferenceUsecase) ReadModel(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (record *model.InferenceModel, err error) {
	log.Trace("InferenceUsecase ReadModel")

	ctx = ctxutil.WithOrgID(ctx, orgID)
	ctx, span := startInferenceSpan(ctx, "model.read",
		attribute.String("org_id", orgID.String()),
		attribute.String("model_id", modelID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.modelRepository.ReadByID(ctx, orgID, modelID)
}

func (u *inferenceUsecase) ListEndpoints(ctx context.Context, orgID uuid.UUID) (out []*model.PublishedEndpoint, err error) {
	log.Trace("InferenceUsecase ListEndpoints")

	ctx = ctxutil.WithOrgID(ctx, orgID)
	ctx, span := startInferenceSpan(ctx, "endpoint.list",
		attribute.String("org_id", orgID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	endpoints, err := u.endpointRepository.ListEndpoints(ctx, orgID)
	if err != nil {
		return nil, err
	}
	out = make([]*model.PublishedEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint != nil && endpoint.IsReady() {
			out = append(out, endpoint)
		}
	}
	return out, nil
}

func (u *inferenceUsecase) PublishEndpoint(ctx context.Context, request model.EndpointPublication) (endpoint *model.PublishedEndpoint, err error) {
	log.Trace("InferenceUsecase PublishEndpoint")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "endpoint.publish",
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("model_id", request.ModelID.String()),
		attribute.Int("dataset_count", len(request.DatasetIDs)),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	inferenceModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, request.ModelID)
	if err != nil {
		return nil, err
	}
	if inferenceModel.OrgID != request.OrgID {
		return nil, domain.ErrModelNotFound
	}
	mode := request.Mode
	agentSpecID := uuid.Nil
	agentSpecHash := ""
	if mode == model.AgentEndpointModeAgent {
		spec, err := u.agentSpecRepository.ReadAgentSpecByHash(ctx, request.OrgID, request.AgentSpecHash)
		if err != nil {
			return nil, err
		}
		if spec.ModelID != request.ModelID {
			return nil, domain.ErrModelMismatch.Extend("agent spec model does not match endpoint model")
		}
		agentSpecID = spec.AgentSpecID
		agentSpecHash = spec.ContentHash
	}
	if err := u.ensureDatasetsExist(ctx, request.OrgID, request.DatasetIDs); err != nil {
		return nil, err
	}
	strategy, err := u.endpointMergeStrategy(request.MergeStrategy)
	if err != nil {
		return nil, err
	}
	if strategy == model.RAGMergeStrategyReranker && u.reranker == nil {
		return nil, domain.ErrModelNotReady.Extend("reranker merge strategy requires a configured reranker")
	}
	displayName := strings.TrimSpace(request.DisplayName)
	if displayName == "" {
		displayName = inferenceModel.Name
	}
	return u.endpointRepository.UpsertEndpoint(ctx, &model.PublishedEndpoint{
		OrgID:           request.OrgID,
		ModelID:         request.ModelID,
		Mode:            mode,
		AgentSpecID:     agentSpecID,
		AgentSpecHash:   agentSpecHash,
		DatasetIDs:      dedupeUUIDs(request.DatasetIDs),
		MergeStrategy:   strategy,
		Status:          model.PublishedEndpointStatusReady,
		DisplayName:     displayName,
		CreatedByUserID: request.UserID,
	})
}

func (u *inferenceUsecase) SetEndpointDatasets(ctx context.Context, request model.EndpointDatasetBinding) (endpoint *model.PublishedEndpoint, err error) {
	log.Trace("InferenceUsecase SetEndpointDatasets")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "endpoint.set_datasets",
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("endpoint_id", request.EndpointID.String()),
		attribute.Int("dataset_count", len(request.DatasetIDs)),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	if err := u.ensureDatasetsExist(ctx, request.OrgID, request.DatasetIDs); err != nil {
		return nil, err
	}
	return u.endpointRepository.SetEndpointDatasets(ctx, request.OrgID, request.EndpointID, dedupeUUIDs(request.DatasetIDs))
}

func (u *inferenceUsecase) SetEndpointMergeStrategy(ctx context.Context, request model.EndpointMergeConfiguration) (endpoint *model.PublishedEndpoint, err error) {
	log.Trace("InferenceUsecase SetEndpointMergeStrategy")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "endpoint.set_merge_strategy",
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("endpoint_id", request.EndpointID.String()),
		attribute.String("merge_strategy", request.MergeStrategy.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	endpoint, err = u.endpointRepository.ReadEndpoint(ctx, request.OrgID, request.EndpointID)
	if err != nil {
		return nil, err
	}
	strategy, err := u.endpointMergeStrategy(request.MergeStrategy)
	if err != nil {
		return nil, err
	}
	if strategy == model.RAGMergeStrategyReranker && u.reranker == nil {
		return nil, domain.ErrModelNotReady.Extend("reranker merge strategy requires a configured reranker")
	}
	endpoint.MergeStrategy = strategy
	return u.endpointRepository.UpsertEndpoint(ctx, endpoint)
}

func (u *inferenceUsecase) GenerateForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.GenerateRequest) (response *model.GenerateResponse, err error) {
	log.Trace("InferenceUsecase GenerateForEndpoint")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "generate.for_endpoint",
		attribute.String("request_id", request.RequestID.String()),
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("endpoint_id", endpointID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	endpoint, err := u.endpointRepository.ReadEndpoint(ctx, request.OrgID, endpointID)
	if err != nil {
		return nil, err
	}
	if !endpoint.IsReady() {
		return nil, domain.ErrModelNotReady.Extend("inference endpoint is not ready")
	}
	request.ModelID = endpoint.ModelID
	if endpoint.Mode == model.AgentEndpointModeAgent {
		return u.generateAgent(ctx, request, endpoint)
	}
	return u.generate(ctx, request, endpoint)
}

func (u *inferenceUsecase) RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (record *model.InferenceFeedback, err error) {
	log.Trace("InferenceUsecase RecordFeedback")

	if feedback != nil {
		ctx = contextForActorOrg(ctx, feedback.UserID, feedback.OrgID)
	}
	ctx, span := startInferenceSpan(ctx, "feedback.record")
	defer endInferenceSpanOnReturn(ctx, span, &err)

	record, err = u.recordFeedback(ctx, feedback, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (u *inferenceUsecase) BuildPreferenceDatasetForEndpoint(ctx context.Context, endpointID uuid.UUID, request model.PreferenceDatasetBuildRequest) (dataset *model.PreferenceDataset, err error) {
	log.Trace("InferenceUsecase BuildPreferenceDatasetForEndpoint")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "preference_dataset.build_for_endpoint",
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("endpoint_id", endpointID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	endpoint, err := u.endpointRepository.ReadEndpoint(ctx, request.OrgID, endpointID)
	if err != nil {
		return nil, err
	}
	if !endpoint.IsReady() {
		return nil, domain.ErrModelNotReady.Extend("inference endpoint is not ready")
	}
	request.EndpointID = endpoint.EndpointID
	request.ModelID = endpoint.ModelID
	request.DatasetIDs = append([]uuid.UUID(nil), endpoint.DatasetIDs...)
	if request.DatasetID == uuid.Nil && len(request.DatasetIDs) == 1 {
		request.DatasetID = request.DatasetIDs[0]
	}
	return u.BuildPreferenceDataset(ctx, request)
}

func (u *inferenceUsecase) ReadPreferenceDataset(ctx context.Context, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (dataset *model.PreferenceDataset, err error) {
	log.Trace("InferenceUsecase ReadPreferenceDataset")

	ctx, span := startInferenceSpan(ctx, "preference_dataset.read",
		attribute.String("org_id", orgID.String()),
		attribute.String("preference_dataset_id", preferenceDatasetID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.feedbackRepository.ReadPreferenceDatasetSnapshot(ctx, orgID, preferenceDatasetID)
}

func (u *inferenceUsecase) ListPreferenceDatasets(ctx context.Context, orgID uuid.UUID, filter model.PreferenceDatasetFilter) (datasets []*model.PreferenceDataset, err error) {
	log.Trace("InferenceUsecase ListPreferenceDatasets")

	ctx, span := startInferenceSpan(ctx, "preference_dataset.list",
		attribute.String("org_id", orgID.String()),
		attribute.String("model_id", filter.ModelID.String()),
		attribute.String("endpoint_id", filter.EndpointID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.feedbackRepository.ListPreferenceDatasetSnapshots(ctx, orgID, filter)
}

func (u *inferenceUsecase) BuildPreferenceDataset(ctx context.Context, request model.PreferenceDatasetBuildRequest) (dataset *model.PreferenceDataset, err error) {
	log.Trace("InferenceUsecase BuildPreferenceDataset")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "preference_dataset.build",
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("model_id", request.ModelID.String()),
		attribute.String("endpoint_id", request.EndpointID.String()),
		attribute.Int("dataset_count", len(request.DatasetIDs)),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	dataset, err = u.feedbackRepository.ReadPreferenceDataset(ctx, request)
	if err != nil {
		return nil, err
	}
	var evalFreeze *preferenceEvalSetFreeze
	if request.OutputURI != "" {
		evalFreeze, err = u.preparePreferenceEvalSet(ctx, dataset)
		if err != nil {
			return nil, err
		}
		dataset.PreferenceDatasetID = preferenceDatasetID(dataset)
		dataset.OutputURI = preferenceDatasetOutputURI(request.OutputURI, dataset)
		if strings.TrimSpace(dataset.EvaluationOutputURI) == "" {
			dataset.EvaluationOutputURI = preferenceDatasetEvaluationOutputURI(request.OutputURI, dataset)
		}
		dataset.IntegrityKey = preferenceDatasetIntegrityKey(dataset)
		if evalFreeze != nil && evalFreeze.evalSet != nil {
			evalFreeze.evalSet.EvalDatasetURI = dataset.EvaluationOutputURI
		}
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
		written.Format = preferenceDatasetFormat(written.Format)
		written.EligibilityPolicy = preferenceDatasetEligibilityPolicy(written.EligibilityPolicy)
		written.MinExamples = request.MinExamples
		written.Limit = request.Limit
		return u.recordPreferenceDatasetSnapshot(ctx, written, request, evalFreeze)
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

func (u *inferenceUsecase) recordPreferenceDatasetSnapshot(ctx context.Context, dataset *model.PreferenceDataset, request model.PreferenceDatasetBuildRequest, evalFreeze *preferenceEvalSetFreeze) (*model.PreferenceDataset, error) {
	log.Trace("InferenceUsecase recordPreferenceDatasetSnapshot")

	var record *model.PreferenceDataset
	err := u.inferenceUnitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		if evalFreeze != nil && evalFreeze.evalSet != nil && u.lineageEvalSetRepository != nil {
			if _, err := u.lineageEvalSetRepository.FreezeEvalSet(ctx, tx, evalFreeze.evalSet, evalFreeze.exampleIDs); err != nil {
				return err
			}
		}
		out, err := u.feedbackRepository.RecordPreferenceDatasetSnapshot(ctx, tx, dataset, request)
		if err != nil {
			return err
		}
		record = out
		return nil
	})
	return record, err
}

func (u *inferenceUsecase) Generate(ctx context.Context, request model.GenerateRequest) (response *model.GenerateResponse, err error) {
	log.Trace("InferenceUsecase Generate")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "generate.raw",
		attribute.String("request_id", request.RequestID.String()),
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("model_id", request.ModelID.String()),
		attribute.String("dataset_id", request.DatasetID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	return u.generate(ctx, request, nil)
}

func (u *inferenceUsecase) generate(ctx context.Context, request model.GenerateRequest, endpoint *model.PublishedEndpoint) (response *model.GenerateResponse, err error) {
	log.Trace("InferenceUsecase generate")

	ctx, span := startInferenceSpan(ctx, "generate.execute",
		attribute.String("request_id", request.RequestID.String()),
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("model_id", request.ModelID.String()),
		attribute.Bool("endpoint_request", endpoint != nil),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

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
		inferenceModel, err = u.ensureServingModelLoaded(ctx, request.OrgID, inferenceModel)
		if err != nil {
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
		generationProtocol = strings.TrimSpace(inferenceModel.ServingProtocol.String())
		generationModel = strings.TrimSpace(inferenceModel.ServingModel)
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
		transformErr := error(nil)
		transformCtx, transformSpan := startInferenceSpan(ctx, "generate.transform_query",
			attribute.String("request_id", request.RequestID.String()),
			attribute.String("model_id", request.ModelID.String()),
		)
		queryTransformCtx := transformCtx
		cancel := func() {}
		if u.queryTransformerTimeout > 0 {
			queryTransformCtx, cancel = context.WithTimeout(transformCtx, u.queryTransformerTimeout)
		}
		transformed, transformErr := u.queryTransformer.TransformQuery(queryTransformCtx, model.QueryTransformRequest{
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
		endInferenceSpanOnReturn(transformCtx, transformSpan, &transformErr)
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
		rerankCtx, rerankSpan := startInferenceSpan(ctx, "generate.rerank",
			attribute.String("request_id", request.RequestID.String()),
			attribute.Int("candidate_count", len(contexts)),
			attribute.Int("top_k", request.TopK),
		)
		contexts, err = u.reranker.Rerank(rerankCtx, retrievalQuery, contexts, request.TopK)
		endInferenceSpanOnReturn(rerankCtx, rerankSpan, &err)
		if err != nil {
			err = fmt.Errorf("%w: %w", domain.ErrRerankFailed, err)
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
	case model.RAGMergeStrategyScoreNormalized:
		_, mergeSpan := startInferenceSpan(ctx, "generate.score_normalized_merge",
			attribute.String("request_id", request.RequestID.String()),
			attribute.Int("candidate_count", len(contexts)),
			attribute.Int("top_k", request.TopK),
		)
		contexts = scoreNormalizedMerge(contexts, request.TopK)
		mergeSpan.End()
	default:
		err = domain.ErrGenerationFailed.Extend(fmt.Sprintf("unsupported rag merge strategy %q", mergeStrategy))
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
	generateCtx, generateSpan := startInferenceSpan(ctx, "generate.call_model",
		attribute.String("request_id", request.RequestID.String()),
		attribute.String("generation_protocol", generationProtocol),
		attribute.String("generation_model", generationModel),
	)
	generationResult, err := generator.Generate(generateCtx, model.GenerationRequest{
		RequestID:             request.RequestID,
		Dataset:               dataset,
		Model:                 inferenceModel,
		Query:                 request.QueryText,
		Prompt:                promptPackage.Prompt,
		PromptStrategyVersion: promptPackage.Strategy.Version,
		Contexts:              promptPackage.Contexts,
	})
	endInferenceSpanOnReturn(generateCtx, generateSpan, &err)
	if err != nil {
		err = fmt.Errorf("%w: %w", domain.ErrGenerationFailed, err)
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProtocol, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	answer := strings.TrimSpace(generationResult.Content)
	if answer == "" {
		err = fmt.Errorf("%w: generation returned an empty response", domain.ErrGenerationFailed)
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
		return datasetIDs, nil
	}
	return []uuid.UUID{request.DatasetID}, nil
}

func (u *inferenceUsecase) ensureServingModelLoaded(ctx context.Context, orgID uuid.UUID, inferenceModel *model.InferenceModel) (loadedModel *model.InferenceModel, err error) {
	log.Trace("InferenceUsecase ensureServingModelLoaded")

	modelID := uuid.Nil
	if inferenceModel != nil {
		modelID = inferenceModel.ModelID
	}
	ctx, span := startInferenceSpan(ctx, "model.ensure_serving_loaded",
		attribute.String("org_id", orgID.String()),
		attribute.String("model_id", modelID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	if inferenceModel.ServingLoadStatus == model.ModelLoadStatusLoaded {
		return inferenceModel, nil
	}
	if inferenceModel.ServingLoadStatus != model.ModelLoadStatusNotLoaded || u.modelServingLoadTrigger == nil {
		return inferenceModel, domain.ErrModelNotReady.Extend("model is not loaded by serving layer")
	}
	if err := u.modelServingLoadTrigger.TriggerModelLoad(ctx, orgID, inferenceModel.ModelID); err != nil {
		return inferenceModel, domain.ErrModelNotReady.Extend(fmt.Sprintf("model serving load trigger failed: %s", err.Error()))
	}
	if u.modelServingLoadTimeout <= 0 || u.modelServingLoadPoll <= 0 {
		return inferenceModel, domain.ErrModelNotReady.Extend("model serving load timing is not configured")
	}
	deadline := time.NewTimer(u.modelServingLoadTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(u.modelServingLoadPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return inferenceModel, ctx.Err()
		case <-deadline.C:
			return inferenceModel, domain.ErrModelNotReady.Extend("model serving load timed out")
		case <-ticker.C:
			latest, err := u.modelRepository.ReadByID(ctx, orgID, inferenceModel.ModelID)
			if err != nil {
				return inferenceModel, err
			}
			inferenceModel = latest
			switch inferenceModel.ServingLoadStatus {
			case model.ModelLoadStatusLoaded:
				return inferenceModel, nil
			case model.ModelLoadStatusFailed:
				return inferenceModel, domain.ErrModelNotReady.Extend("model serving load failed")
			}
		}
	}
}

func (u *inferenceUsecase) readGenerateDatasets(ctx context.Context, orgID uuid.UUID, datasetIDs []uuid.UUID) ([]*model.InferenceDataset, error) {
	log.Trace("InferenceUsecase readGenerateDatasets")

	datasets := make([]*model.InferenceDataset, 0, len(datasetIDs))
	for _, datasetID := range datasetIDs {
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

func (u *inferenceUsecase) retrieveFromDatasets(ctx context.Context, userID uuid.UUID, datasets []*model.InferenceDataset, queryText string, topK int, metadataFilters map[string]string) (contexts []model.RetrievedContext, err error) {
	log.Trace("InferenceUsecase retrieveFromDatasets")

	ctx, span := startInferenceSpan(ctx, "generate.retrieve_contexts",
		attribute.String("user_id", userID.String()),
		attribute.Int("dataset_count", len(datasets)),
		attribute.Int("top_k", topK),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	contexts = make([]model.RetrievedContext, 0, topK*len(datasets))
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
	outputURI = strings.ReplaceAll(outputURI, "{endpoint_id}", dataset.EndpointID.String())
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
		dataset.EndpointID.String(),
		dataset.DatasetID.String(),
		dataset.ModelID.String(),
		dataset.ParentModelKind.String(),
		strings.TrimSpace(dataset.ParentArtifactURI),
		strings.TrimSpace(dataset.ParentArtifactChecksum),
		strings.TrimSpace(dataset.ParentAdapterURI),
		strings.TrimSpace(dataset.ParentBaseModel),
		strings.TrimSpace(dataset.ParentLineageName),
		strings.TrimSpace(dataset.ParentModelName),
		fmt.Sprintf("%d", dataset.ParentModelVersion),
	}
	for _, datasetID := range dedupeUUIDs(dataset.DatasetIDs) {
		parts = append(parts, datasetID.String())
	}
	for _, example := range dataset.Examples {
		parts = append(parts, example.PreferenceExampleID.String())
		parts = append(parts, example.Split)
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, ":")))
}

func preferenceDatasetIntegrityKey(dataset *model.PreferenceDataset) string {
	log.Trace("preferenceDatasetIntegrityKey")

	if dataset == nil {
		return ""
	}
	parts := []string{
		"preference-dataset-integrity-v1",
		dataset.PreferenceDatasetID.String(),
		dataset.OrgID.String(),
		dataset.EndpointID.String(),
		dataset.DatasetID.String(),
		dataset.ModelID.String(),
		dataset.ParentModelKind.String(),
		strings.TrimSpace(dataset.ParentArtifactURI),
		strings.TrimSpace(dataset.ParentArtifactChecksum),
		strings.TrimSpace(dataset.ParentAdapterURI),
		strings.TrimSpace(dataset.ParentBaseModel),
		strings.TrimSpace(dataset.ParentLineageName),
		strings.TrimSpace(dataset.ParentModelName),
		fmt.Sprintf("%d", dataset.ParentModelVersion),
		strings.TrimSpace(dataset.OutputURI),
		strings.TrimSpace(dataset.EvaluationOutputURI),
		preferenceDatasetFormat(dataset.Format),
		preferenceDatasetEligibilityPolicy(dataset.EligibilityPolicy),
	}
	for _, datasetID := range dedupeUUIDs(dataset.DatasetIDs) {
		parts = append(parts, datasetID.String())
	}
	for _, example := range dataset.Examples {
		parts = append(parts,
			example.PreferenceExampleID.String(),
			example.FeedbackID.String(),
			example.RequestID.String(),
			example.UserID.String(),
			example.OrgID.String(),
			example.DatasetID.String(),
			example.ModelID.String(),
			strings.TrimSpace(example.Split),
			example.PromptText,
			example.AcceptedAnswer,
			example.RejectedAnswer,
			fmt.Sprintf("%d", example.Rating),
			strings.TrimSpace(example.FeedbackLabel),
		)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func (u *inferenceUsecase) preparePreferenceEvalSet(ctx context.Context, dataset *model.PreferenceDataset) (*preferenceEvalSetFreeze, error) {
	log.Trace("InferenceUsecase preparePreferenceEvalSet")

	if u.lineageEvalSetRepository == nil || dataset == nil {
		return nil, nil
	}
	lineageName := preferenceDatasetLineageName(dataset)
	active, err := u.lineageEvalSetRepository.ReadActiveEvalSet(ctx, dataset.OrgID, lineageName)
	if err == nil && active != nil {
		dataset.EvaluationOutputURI = strings.TrimSpace(active.EvalDatasetURI)
		dataset.Examples = preferenceDatasetTrainOnlyExamples(dataset.Examples)
		return nil, nil
	}
	if err != nil && !errors.Is(err, domain.ErrEvalSetNotFound) {
		return nil, err
	}
	exampleIDs := preferenceDatasetEvalExampleIDs(dataset)
	if len(exampleIDs) < minimumFrozenEvalExamples {
		return nil, nil
	}
	return &preferenceEvalSetFreeze{
		evalSet: &model.LineageEvalSet{
			OrgID:        dataset.OrgID,
			LineageName:  lineageName,
			Version:      1,
			Checksum:     preferenceEvalSetChecksum(exampleIDs),
			ExampleCount: len(exampleIDs),
			Source:       model.LineageEvalSetSourceFrozenGen0,
			Active:       true,
		},
		exampleIDs: exampleIDs,
	}, nil
}

func preferenceDatasetLineageName(dataset *model.PreferenceDataset) string {
	log.Trace("preferenceDatasetLineageName")

	if dataset == nil {
		return ""
	}
	if name := strings.TrimSpace(dataset.ParentLineageName); name != "" {
		return name
	}
	if name := strings.TrimSpace(dataset.ParentModelName); name != "" {
		return name
	}
	if dataset.ModelID != uuid.Nil {
		return dataset.ModelID.String()
	}
	return strings.TrimSpace(dataset.ParentBaseModel)
}

func preferenceDatasetTrainOnlyExamples(examples []model.PreferenceExample) []model.PreferenceExample {
	log.Trace("preferenceDatasetTrainOnlyExamples")

	out := make([]model.PreferenceExample, len(examples))
	copy(out, examples)
	for i := range out {
		out[i].Split = "TRAIN"
	}
	return out
}

func preferenceDatasetEvalExampleIDs(dataset *model.PreferenceDataset) []uuid.UUID {
	log.Trace("preferenceDatasetEvalExampleIDs")

	if dataset == nil {
		return nil
	}
	evalExamples := dataset.EvaluationExamples()
	ids := make([]uuid.UUID, 0, len(evalExamples))
	for _, example := range evalExamples {
		if example.PreferenceExampleID == uuid.Nil {
			continue
		}
		ids = append(ids, example.PreferenceExampleID)
	}
	return ids
}

func preferenceEvalSetChecksum(exampleIDs []uuid.UUID) string {
	log.Trace("preferenceEvalSetChecksum")

	if len(exampleIDs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(exampleIDs))
	for _, id := range exampleIDs {
		if id == uuid.Nil {
			continue
		}
		parts = append(parts, id.String())
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func (u *inferenceUsecase) upsertEndpointProjection(ctx context.Context, inferenceModel *model.InferenceModel) error {
	log.Trace("InferenceUsecase upsertEndpointProjection")

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
		Mode:            model.AgentEndpointModeRAG,
		DatasetIDs:      []uuid.UUID{inferenceModel.DatasetID},
		MergeStrategy:   strategy,
		Status:          status,
		DisplayName:     inferenceModel.Name,
		CreatedByUserID: inferenceModel.UserID,
	})
	return err
}

func (u *inferenceUsecase) probeAndRecordCapabilityReport(ctx context.Context, inferenceModel *model.InferenceModel) (*model.CapabilityReport, error) {
	log.Trace("InferenceUsecase probeAndRecordCapabilityReport")

	if u.capabilityReportRepository == nil {
		return nil, domain.ErrModelNotReady.Extend("model capability report repository is not configured")
	}
	effectiveBaseID := strings.TrimSpace(inferenceModel.EffectiveBaseID)
	if effectiveBaseID == "" {
		return nil, domain.ErrModelNotReady.Extend("model effective base is required for capability probing")
	}
	report := &model.CapabilityReport{
		EffectiveBaseID:      effectiveBaseID,
		SupportsChat:         u.probeChatCapability(ctx, inferenceModel),
		SupportsToolCalls:    u.probeToolCallCapability(ctx, inferenceModel),
		SupportsSystemPrompt: u.probeSystemPromptCapability(ctx, inferenceModel),
	}
	return u.capabilityReportRepository.RecordCapabilityReport(ctx, report)
}

func (u *inferenceUsecase) probeChatCapability(ctx context.Context, inferenceModel *model.InferenceModel) bool {
	log.Trace("InferenceUsecase probeChatCapability")

	result, err := u.probeGenerationCapability(ctx, inferenceModel, model.GenerationRequest{
		Model: inferenceModel,
		Query: "Respond with ok.",
		Messages: []model.ChatMessage{
			{Role: model.ChatMessageRoleUser, Content: "Respond with ok."},
		},
		Options: model.GenerationOptions{
			Temperature:     0,
			TopP:            1,
			MaxOutputTokens: 8,
		},
	})
	return err == nil && strings.TrimSpace(result.Content) != ""
}

func (u *inferenceUsecase) probeSystemPromptCapability(ctx context.Context, inferenceModel *model.InferenceModel) bool {
	log.Trace("InferenceUsecase probeSystemPromptCapability")

	result, err := u.probeGenerationCapability(ctx, inferenceModel, model.GenerationRequest{
		Model: inferenceModel,
		Query: "Respond with ok.",
		Messages: []model.ChatMessage{
			{Role: model.ChatMessageRoleSystem, Content: "You are a capability probe."},
			{Role: model.ChatMessageRoleUser, Content: "Respond with ok."},
		},
		Options: model.GenerationOptions{
			Temperature:     0,
			TopP:            1,
			MaxOutputTokens: 8,
		},
	})
	return err == nil && strings.TrimSpace(result.Content) != ""
}

func (u *inferenceUsecase) probeToolCallCapability(ctx context.Context, inferenceModel *model.InferenceModel) bool {
	log.Trace("InferenceUsecase probeToolCallCapability")

	result, err := u.probeGenerationCapability(ctx, inferenceModel, model.GenerationRequest{
		Model: inferenceModel,
		Query: "Call the capability_probe tool with an empty JSON object.",
		Messages: []model.ChatMessage{
			{Role: model.ChatMessageRoleUser, Content: "Call the capability_probe tool with an empty JSON object."},
		},
		Tools: []model.ToolSpec{{
			Name:        "capability_probe",
			Description: "Capability probe; returns no user data.",
			Parameters:  json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{}}`),
		}},
		ToolChoice: agentToolChoiceRequired,
		Options: model.GenerationOptions{
			Temperature:     0,
			TopP:            1,
			MaxOutputTokens: 32,
		},
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("tool-call capability probe failed")
		return false
	}
	return len(result.ToolCalls) > 0
}

func (u *inferenceUsecase) probeGenerationCapability(ctx context.Context, inferenceModel *model.InferenceModel, request model.GenerationRequest) (model.GenerationResult, error) {
	log.Trace("InferenceUsecase probeGenerationCapability")

	if inferenceModel == nil || inferenceModel.ServingProtocol == model.ServingProtocolUnknown {
		return model.GenerationResult{}, domain.ErrModelNotReady.Extend("serving protocol is unknown")
	}
	generator := u.generationAdapters[inferenceModel.ServingProtocol.String()]
	if generator == nil {
		return model.GenerationResult{}, domain.ErrModelNotReady.Extend("generation adapter is not configured")
	}
	probeCtx, cancel := context.WithTimeout(ctx, capabilityProbeTimeout)
	defer cancel()
	return generator.Generate(probeCtx, request)
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

func (u *inferenceUsecase) recordInferenceRequest(ctx context.Context, request model.GenerateRequest, dataset *model.InferenceDataset, inferenceModel *model.InferenceModel, contexts []model.RetrievedContext, promptText string, answerText string, startedAt time.Time, generationProtocol string, generationModel string, status model.InferenceRequestStatus, errorMessage string) (err error) {
	log.Trace("InferenceUsecase recordInferenceRequest")

	ctx, span := startInferenceSpan(ctx, "generate.record_audit",
		attribute.String("request_id", request.RequestID.String()),
		attribute.String("status", status.String()),
		attribute.Int("context_count", len(contexts)),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

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
