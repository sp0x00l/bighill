package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type InferenceUsecase interface {
	RecordModelUpdated(ctx context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error)
	RecordDatasetUpdated(ctx context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error)
	ReadModel(ctx context.Context, orgID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error)
	ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error)
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
	generator                 GenerationAdapter
	preferenceDatasetWriter   PreferenceDatasetWriter
	promptStrategy            model.PromptStrategy
	rerankCandidateMultiplier int
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
	request.DatasetID = endpoint.DatasetID
	request.ModelID = endpoint.ModelID
	return u.Generate(ctx, request)
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
	startedAt := time.Now()

	var dataset *model.InferenceDataset
	var inferenceModel *model.InferenceModel
	var contexts []model.RetrievedContext
	var promptText string
	var answerText string

	dataset, err = u.datasetRepository.ReadDataset(ctx, request.OrgID, request.DatasetID)
	if err != nil {
		return nil, err
	}
	inferenceModel, err = u.modelRepository.ReadByID(ctx, request.OrgID, request.ModelID)
	if err != nil {
		return nil, err
	}
	generationProvider := u.generator.Provider()
	generationModel := u.generator.Model()
	if inferenceModel.Status != model.ModelStatusReady {
		err = domain.ErrModelNotReady.Extend("model is not ready")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if inferenceModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		err = domain.ErrModelNotReady.Extend("model is not loaded by serving layer")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if inferenceModel.RequiresDatasetMatch() && inferenceModel.DatasetID != request.DatasetID {
		err = domain.ErrModelMismatch.Extend("model dataset does not match request dataset")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if !dataset.IsRAGReady() {
		err = domain.ErrDatasetNotReady.Extend("dataset embeddings are not materialized")
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}

	retrievalQuery := request.QueryText
	retrievalFilters := copyMetadataFilters(request.MetadataFilters)
	if u.queryTransformer != nil {
		transformCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		transformed, transformErr := u.queryTransformer.TransformQuery(transformCtx, model.QueryTransformRequest{
			RequestID:       request.RequestID,
			UserID:          request.UserID,
			OrgID:           request.OrgID,
			DatasetID:       request.DatasetID,
			ModelID:         request.ModelID,
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

	candidateK := request.TopK
	if u.reranker != nil && u.rerankCandidateMultiplier > 1 {
		candidateK = request.TopK * u.rerankCandidateMultiplier
	}

	contexts, err = u.retrievalClient.SearchEmbeddings(ctx, request.UserID, request.DatasetID, retrievalQuery, candidateK, retrievalFilters)
	if err != nil {
		err = fmt.Errorf("%w: %w", domain.ErrRetrievalFailed, err)
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	if u.reranker != nil {
		contexts, err = u.reranker.Rerank(ctx, retrievalQuery, contexts, request.TopK)
		if err != nil {
			err = fmt.Errorf("%w: %w", domain.ErrRerankFailed, err)
			return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
		}
	}
	contexts, err = u.contextPacker.Pack(ctx, model.ContextPackRequest{
		Query:    retrievalQuery,
		Contexts: contexts,
	})
	if err != nil {
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	promptPackage, err := u.promptBuilder.BuildPrompt(ctx, model.PromptBuildRequest{
		Query:    request.QueryText,
		Dataset:  dataset,
		Model:    inferenceModel,
		Contexts: contexts,
	})
	if err != nil {
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	promptText = promptPackage.Prompt
	answer, err := u.generator.Generate(ctx, model.GenerationRequest{
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
		return nil, errors.Join(err, u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusFailed, err.Error()))
	}
	answerText = answer

	response = &model.GenerateResponse{
		RequestID:             request.RequestID,
		OrgID:                 request.OrgID,
		DatasetID:             request.DatasetID,
		ModelID:               inferenceModel.ModelID,
		QueryText:             request.QueryText,
		Answer:                answer,
		Contexts:              contexts,
		PromptStrategyVersion: promptPackage.Strategy.Version,
		GenerationProvider:    generationProvider,
		GenerationModel:       generationModel,
	}
	if err := u.recordInferenceRequest(ctx, request, dataset, inferenceModel, contexts, promptText, answerText, startedAt, generationProvider, generationModel, model.InferenceRequestStatusCompleted, ""); err != nil {
		return nil, err
	}
	return response, nil
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
	if inferenceModel.OrgID == uuid.Nil || inferenceModel.UserID == uuid.Nil || inferenceModel.DatasetID == uuid.Nil {
		return nil
	}
	status := model.PublishedEndpointStatusDisabled
	if inferenceModel.Status == model.ModelStatusReady && inferenceModel.ServingLoadStatus == model.ModelLoadStatusLoaded {
		status = model.PublishedEndpointStatusReady
	}
	_, err := u.endpointRepository.UpsertEndpoint(ctx, &model.PublishedEndpoint{
		EndpointID:      publishedEndpointID(inferenceModel),
		OrgID:           inferenceModel.OrgID,
		ModelID:         inferenceModel.ModelID,
		DatasetID:       inferenceModel.DatasetID,
		Status:          status,
		DisplayName:     inferenceModel.Name,
		CreatedByUserID: inferenceModel.UserID,
	})
	return err
}

func publishedEndpointID(inferenceModel *model.InferenceModel) uuid.UUID {
	log.Trace("publishedEndpointID")

	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"published-inference-endpoint",
		inferenceModel.OrgID.String(),
		inferenceModel.ModelID.String(),
		inferenceModel.DatasetID.String(),
	}, ":")))
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

func (u *inferenceUsecase) recordInferenceRequest(ctx context.Context, request model.GenerateRequest, dataset *model.InferenceDataset, inferenceModel *model.InferenceModel, contexts []model.RetrievedContext, promptText string, answerText string, startedAt time.Time, generationProvider string, generationModel string, status model.InferenceRequestStatus, errorMessage string) error {
	log.Trace("InferenceUsecase recordInferenceRequest")

	record, err := inferenceRequestRecord(request, dataset, inferenceModel, contexts, promptText, answerText, u.promptStrategy, startedAt, generationProvider, generationModel, status, errorMessage)
	if err != nil {
		return err
	}
	if err := u.requestRepository.RecordInferenceRequest(ctx, record); err != nil {
		return fmt.Errorf("record inference request: %w", err)
	}
	return nil
}

func inferenceRequestRecord(request model.GenerateRequest, dataset *model.InferenceDataset, inferenceModel *model.InferenceModel, contexts []model.RetrievedContext, promptText string, answerText string, strategy model.PromptStrategy, startedAt time.Time, generationProvider string, generationModel string, status model.InferenceRequestStatus, errorMessage string) (*model.InferenceRequest, error) {
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
		GenerationProvider:    generationProvider,
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

	records := make([]model.RetrievedContextAudit, 0, len(contexts))
	for i, retrieved := range contexts {
		if retrieved.EmbeddingRecordID == uuid.Nil {
			return "", fmt.Errorf("retrieved context %d has empty embedding record id", i)
		}
		if retrieved.EmbeddingSnapshotID == uuid.Nil {
			return "", fmt.Errorf("retrieved context %d has empty embedding snapshot id", i)
		}
		records = append(records, model.RetrievedContextAudit{
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
