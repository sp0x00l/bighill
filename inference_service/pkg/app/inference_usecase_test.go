package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	inferencepb "lib/data_contracts_lib/inference"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service app unit test suite")
}

type inferenceModelRepositoryStub struct {
	model          *model.InferenceModel
	upsertedModel  *model.InferenceModel
	idempotencyKey uuid.UUID
	readUserID     uuid.UUID
	readID         uuid.UUID
	err            error
}

func (s *inferenceModelRepositoryStub) UpsertModel(_ context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	s.upsertedModel = inferenceModel
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return inferenceModel, nil
}

func (s *inferenceModelRepositoryStub) ReadByID(_ context.Context, userID uuid.UUID, modelID uuid.UUID) (*model.InferenceModel, error) {
	s.readUserID = userID
	s.readID = modelID
	if s.err != nil {
		return nil, s.err
	}
	return s.model, nil
}

type inferenceDatasetRepositoryStub struct {
	dataset        *model.InferenceDataset
	upserted       *model.InferenceDataset
	idempotencyKey uuid.UUID
	readUserID     uuid.UUID
	readID         uuid.UUID
	err            error
}

func (s *inferenceDatasetRepositoryStub) UpsertDataset(_ context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error) {
	s.upserted = dataset
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return dataset, nil
}

func (s *inferenceDatasetRepositoryStub) ReadDataset(_ context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.InferenceDataset, error) {
	s.readUserID = userID
	s.readID = datasetID
	if s.err != nil {
		return nil, s.err
	}
	return s.dataset, nil
}

type retrievalClientStub struct {
	userID          uuid.UUID
	datasetID       uuid.UUID
	queryText       string
	topK            int
	metadataFilters map[string]string
	contexts        []model.RetrievedContext
	err             error
}

func (s *retrievalClientStub) SearchEmbeddings(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error) {
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	s.metadataFilters = metadataFilters
	if s.err != nil {
		return nil, s.err
	}
	if topK < len(s.contexts) {
		return s.contexts[:topK], nil
	}
	return s.contexts, nil
}

func (s *retrievalClientStub) Close() error {
	return nil
}

type rerankerStub struct {
	query      string
	candidates []model.RetrievedContext
	topK       int
	contexts   []model.RetrievedContext
	err        error
}

func (s *rerankerStub) Rerank(_ context.Context, query string, candidates []model.RetrievedContext, topK int) ([]model.RetrievedContext, error) {
	s.query = query
	s.candidates = candidates
	s.topK = topK
	if s.err != nil {
		return nil, s.err
	}
	return s.contexts, nil
}

type generationAdapterStub struct {
	request model.GenerationRequest
	answer  string
	err     error
}

func (s *generationAdapterStub) Generate(_ context.Context, request model.GenerationRequest) (string, error) {
	s.request = request
	if s.err != nil {
		return "", s.err
	}
	if s.answer != "" {
		return s.answer, nil
	}
	return "generated answer", nil
}

func (s *generationAdapterStub) Provider() string {
	return "stub"
}

func (s *generationAdapterStub) Model() string {
	return "stub-model"
}

type inferenceRequestRepositoryStub struct {
	request *model.InferenceRequest
	err     error
}

func (s *inferenceRequestRepositoryStub) RecordInferenceRequest(_ context.Context, request *model.InferenceRequest) error {
	s.request = request
	return s.err
}

type inferenceFeedbackRepositoryStub struct {
	feedback          *model.InferenceFeedback
	idempotencyKey    uuid.UUID
	preferenceRequest model.PreferenceDatasetExportRequest
	preferenceDataset *model.PreferenceDataset
	recordedSnapshot  *model.PreferenceDataset
	snapshotRequest   model.PreferenceDatasetExportRequest
	err               error
}

func (s *inferenceFeedbackRepositoryStub) RecordFeedback(_ context.Context, _ pgx.Tx, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	s.feedback = feedback
	s.idempotencyKey = idempotencyKey
	return feedback, s.err
}

func (s *inferenceFeedbackRepositoryStub) ReadPreferenceDataset(_ context.Context, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	s.preferenceRequest = request
	if s.preferenceDataset != nil {
		return s.preferenceDataset, s.err
	}
	return &model.PreferenceDataset{RequestID: request.RequestID, UserID: request.UserID, DatasetID: request.DatasetID, ModelID: request.ModelID}, s.err
}

func (s *inferenceFeedbackRepositoryStub) RecordPreferenceDatasetSnapshot(_ context.Context, _ pgx.Tx, dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	s.recordedSnapshot = dataset
	s.snapshotRequest = request
	return dataset, s.err
}

type inferenceUnitOfWorkStub struct {
	messages []msgConn.OutboundMessage
	err      error
}

func preferenceEventBuilder() app.PreferenceDatasetEventBuilder {
	return inferencemessaging.NewPreferenceDatasetEventBuilder("inference")
}

func (s *inferenceUnitOfWorkStub) Do(ctx context.Context, fn shareduow.TxFunc) error {
	if s.err != nil {
		return s.err
	}
	return fn(ctx, nil, func(msg msgConn.OutboundMessage) error {
		s.messages = append(s.messages, msg)
		return nil
	})
}

type preferenceDatasetWriterStub struct {
	dataset *model.PreferenceDataset
	err     error
}

func (s *preferenceDatasetWriterStub) WritePreferenceDataset(_ context.Context, dataset *model.PreferenceDataset) (*model.PreferenceDataset, error) {
	s.dataset = dataset
	if s.err != nil {
		return nil, s.err
	}
	dataset.Exported = true
	return dataset, nil
}

type queryTransformerStub struct {
	request model.QueryTransformRequest
	result  *model.QueryTransformResult
	err     error
}

func (s *queryTransformerStub) TransformQuery(_ context.Context, request model.QueryTransformRequest) (*model.QueryTransformResult, error) {
	s.request = request
	return s.result, s.err
}

var _ = Describe("InferenceUsecase", func() {
	It("records a complete model update", func() {
		repository := &inferenceModelRepositoryStub{}
		uc := app.NewInferenceUsecase(repository)
		idempotencyKey := uuid.New()

		recorded, err := uc.RecordModelUpdated(context.Background(), validInferenceModel(), idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.ModelID).To(Equal(repository.upsertedModel.ModelID))
		Expect(repository.idempotencyKey).To(Equal(idempotencyKey))
	})

	It("reads a model by id", func() {
		expected := validInferenceModel()
		repository := &inferenceModelRepositoryStub{model: expected}
		uc := app.NewInferenceUsecase(repository)

		readModel, err := uc.ReadModel(context.Background(), expected.UserID, expected.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(readModel).To(Equal(expected))
		Expect(repository.readUserID).To(Equal(expected.UserID))
		Expect(repository.readID).To(Equal(expected.ModelID))
	})

	It("records a registry dataset update", func() {
		datasetRepository := &inferenceDatasetRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceDatasetRepository(datasetRepository),
		)
		idempotencyKey := uuid.New()

		recorded, err := uc.RecordDatasetUpdated(context.Background(), validInferenceDataset(), idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.DatasetID).To(Equal(datasetRepository.upserted.DatasetID))
		Expect(datasetRepository.idempotencyKey).To(Equal(idempotencyKey))
	})

	It("records inference feedback through the repository", func() {
		feedbackRepository := &inferenceFeedbackRepositoryStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}, preferenceEventBuilder()),
		)
		idempotencyKey := uuid.New()
		feedback := &model.InferenceFeedback{
			FeedbackID: uuid.New(),
			RequestID:  uuid.New(),
			UserID:     uuid.New(),
			Accepted:   false,
			Rating:     -1,
			Comment:    "not grounded",
		}

		recorded, err := uc.RecordFeedback(context.Background(), feedback, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded).To(Equal(feedback))
		Expect(feedbackRepository.feedback).To(Equal(feedback))
		Expect(feedbackRepository.idempotencyKey).To(Equal(idempotencyKey))
	})

	It("does not export a preference dataset while recording feedback", func() {
		requestID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID: requestID,
			DatasetID: uuid.New(),
			ModelID:   uuid.New(),
			Examples:  []model.PreferenceExample{},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}, preferenceEventBuilder()),
			app.WithPreferenceDatasetWriter(writer),
		)

		_, err := uc.RecordFeedback(context.Background(), &model.InferenceFeedback{
			FeedbackID: uuid.New(),
			RequestID:  requestID,
			UserID:     uuid.New(),
			Accepted:   true,
			Rating:     1,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Consistently(func() *model.PreferenceDataset { return writer.dataset }).Should(BeNil())
		Expect(feedbackRepository.preferenceRequest.RequestID).To(Equal(uuid.Nil))
	})

	It("exports a preference dataset when explicitly requested with enough complete pairs", func() {
		requestID := uuid.New()
		userID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:          requestID,
			UserID:             userID,
			DatasetID:          datasetID,
			ModelID:            modelID,
			ParentAdapterURI:   "s3://models/parent",
			ParentBaseModel:    "mistral-7b",
			ParentModelVersion: 7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				PromptText:          "prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}, preferenceEventBuilder()),
			app.WithPreferenceDatasetWriter(writer),
		)
		dataset, err := uc.ExportPreferenceDataset(context.Background(), model.PreferenceDatasetExportRequest{
			RequestID:   requestID,
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Exported).To(BeTrue())
		Expect(feedbackRepository.preferenceRequest.RequestID).To(Equal(requestID))
		Expect(feedbackRepository.preferenceRequest.UserID).To(Equal(userID))
		Expect(feedbackRepository.preferenceRequest.MinExamples).To(Equal(1))
		Expect(feedbackRepository.preferenceRequest.Limit).To(Equal(100))
		Expect(writer.dataset).NotTo(BeNil())
		Expect(writer.dataset.OutputURI).To(ContainSubstring("s3://local-dev-bucket/preferences/" + datasetID.String() + "/preference_dataset-"))
		Expect(writer.dataset.OutputURI).To(HaveSuffix(".jsonl"))
		Expect(writer.dataset.Exported).To(BeTrue())
	})

	It("records a preference dataset snapshot after export succeeds", func() {
		requestID := uuid.New()
		userID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:          requestID,
			UserID:             userID,
			DatasetID:          datasetID,
			ModelID:            modelID,
			ParentAdapterURI:   "s3://models/parent",
			ParentBaseModel:    "mistral-7b",
			ParentModelVersion: 7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				PromptText:          "prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		writer := &preferenceDatasetWriterStub{}
		unitOfWork := &inferenceUnitOfWorkStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(unitOfWork, preferenceEventBuilder()),
			app.WithPreferenceDatasetWriter(writer),
		)

		_, err := uc.ExportPreferenceDataset(context.Background(), model.PreferenceDatasetExportRequest{
			RequestID:   requestID,
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/{request_id}.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(feedbackRepository.recordedSnapshot).NotTo(BeNil())
		Expect(feedbackRepository.recordedSnapshot.PreferenceDatasetID).NotTo(Equal(uuid.Nil))
		Expect(feedbackRepository.recordedSnapshot.UserID).To(Equal(userID))
		Expect(feedbackRepository.recordedSnapshot.OutputURI).To(ContainSubstring("s3://local-dev-bucket/preferences/" + datasetID.String() + "/" + requestID.String() + "-"))
		Expect(feedbackRepository.recordedSnapshot.OutputURI).To(HaveSuffix(".jsonl"))
		Expect(feedbackRepository.recordedSnapshot.EvaluationOutputURI).To(ContainSubstring("-eval.jsonl"))
		Expect(feedbackRepository.recordedSnapshot.Format).To(Equal("DPO_JSONL"))
		Expect(feedbackRepository.recordedSnapshot.EligibilityPolicy).To(Equal("complete_rejected_pairs_train_eval_split_v1"))
		Expect(feedbackRepository.snapshotRequest.MinExamples).To(Equal(1))
		Expect(feedbackRepository.snapshotRequest.UserID).To(Equal(userID))
		Expect(feedbackRepository.snapshotRequest.Limit).To(Equal(100))
		Expect(unitOfWork.messages).To(HaveLen(1))
		Expect(unitOfWork.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypePreferenceDatasetReady))
		var event inferencepb.PreferenceDatasetReadyEvent
		Expect(proto.Unmarshal(unitOfWork.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.PreferenceDatasetId).To(Equal(feedbackRepository.recordedSnapshot.PreferenceDatasetID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OutputUri).To(Equal(feedbackRepository.recordedSnapshot.OutputURI))
		Expect(event.ParentAdapterUri).To(Equal("s3://models/parent"))
	})

	It("does not write a preference dataset before the configured threshold is met", func() {
		requestID := uuid.New()
		userID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID: requestID,
			UserID:    userID,
			DatasetID: uuid.New(),
			ModelID:   uuid.New(),
			Examples:  []model.PreferenceExample{},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}, preferenceEventBuilder()),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.ExportPreferenceDataset(context.Background(), model.PreferenceDatasetExportRequest{
			RequestID:   requestID,
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ExampleCount()).To(Equal(0))
		Expect(writer.dataset).To(BeNil())
	})

	It("uses total eligible examples for the preference export threshold", func() {
		requestID := uuid.New()
		userID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackRepository := &inferenceFeedbackRepositoryStub{preferenceDataset: &model.PreferenceDataset{
			RequestID:          requestID,
			UserID:             userID,
			DatasetID:          datasetID,
			ModelID:            modelID,
			ParentAdapterURI:   "s3://models/parent",
			ParentBaseModel:    "mistral-7b",
			ParentModelVersion: 7,
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				RequestID:           requestID,
				UserID:              userID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "EVAL",
				PromptText:          "prompt",
				AcceptedAnswer:      "chosen",
				RejectedAnswer:      "rejected",
			}},
		}}
		writer := &preferenceDatasetWriterStub{}
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceFeedbackRepository(feedbackRepository),
			app.WithInferenceUnitOfWork(&inferenceUnitOfWorkStub{}, preferenceEventBuilder()),
			app.WithPreferenceDatasetWriter(writer),
		)

		dataset, err := uc.ExportPreferenceDataset(context.Background(), model.PreferenceDatasetExportRequest{
			RequestID:   requestID,
			UserID:      userID,
			OutputURI:   "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			MinExamples: 1,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ExampleCount()).To(Equal(1))
		Expect(dataset.TrainingExampleCount()).To(Equal(0))
		Expect(writer.dataset).NotTo(BeNil())
		Expect(writer.dataset.ExampleCount()).To(Equal(1))
	})

	It("generates from retrieved RAG contexts", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		modelRepository := &inferenceModelRepositoryStub{model: inferenceModel}
		datasetRepository := &inferenceDatasetRepositoryStub{dataset: dataset}
		requestRepository := &inferenceRequestRepositoryStub{}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          2,
			SourceText:          "retrieved context",
			Similarity:          0.87,
		}}}
		generator := &generationAdapterStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			modelRepository,
			app.WithInferenceDatasetRepository(datasetRepository),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapter(generator),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)
		requestID := uuid.New()

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID:       requestID,
			UserID:          dataset.UserID,
			DatasetID:       dataset.DatasetID,
			ModelID:         inferenceModel.ModelID,
			QueryText:       "what happened?",
			TopK:            8,
			MetadataFilters: map[string]string{"source": "manual"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(datasetRepository.readUserID).To(Equal(dataset.UserID))
		Expect(datasetRepository.readID).To(Equal(dataset.DatasetID))
		Expect(modelRepository.readUserID).To(Equal(dataset.UserID))
		Expect(modelRepository.readID).To(Equal(inferenceModel.ModelID))
		Expect(retrieval.userID).To(Equal(dataset.UserID))
		Expect(retrieval.datasetID).To(Equal(dataset.DatasetID))
		Expect(retrieval.queryText).To(Equal("what happened?"))
		Expect(retrieval.topK).To(Equal(8))
		Expect(retrieval.metadataFilters).To(Equal(map[string]string{"source": "manual"}))
		Expect(generator.request.Dataset).To(Equal(dataset))
		Expect(generator.request.Model).To(Equal(inferenceModel))
		Expect(generator.request.RequestID).To(Equal(requestID))
		Expect(generator.request.Prompt).To(ContainSubstring("Retrieved context"))
		Expect(generator.request.PromptStrategyVersion).To(Equal("test-rag-prompt-v1"))
		Expect(response.Answer).To(Equal("generated answer"))
		Expect(response.RequestID).To(Equal(requestID))
		Expect(response.PromptStrategyVersion).To(Equal("test-rag-prompt-v1"))
		Expect(response.GenerationProvider).To(Equal("stub"))
		Expect(response.GenerationModel).To(Equal("stub-model"))
		Expect(response.Contexts).To(HaveLen(1))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.UserID).To(Equal(dataset.UserID))
		Expect(requestRepository.request.RequestID).To(Equal(requestID))
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
		Expect(requestRepository.request.GenerationProvider).To(Equal("stub"))
		Expect(requestRepository.request.PromptText).To(ContainSubstring("Retrieved context"))
		Expect(requestRepository.request.PromptText).To(ContainSubstring("retrieved context"))
		Expect(requestRepository.request.AnswerText).To(Equal("generated answer"))
		Expect(requestRepository.request.RetrievedContexts).NotTo(BeEmpty())
		var auditedContexts []model.RetrievedContextAudit
		Expect(json.Unmarshal([]byte(requestRepository.request.RetrievedContexts), &auditedContexts)).To(Succeed())
		Expect(auditedContexts).To(HaveLen(1))
		Expect(auditedContexts[0].SourceText).To(Equal("retrieved context"))
		Expect(auditedContexts[0].EmbeddingSnapshotID).To(Equal(dataset.EmbeddingSnapshotID.String()))
	})

	It("uses query transformer output for retrieval without changing the generated question", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "retrieved context",
			Similarity:          0.91,
		}}}
		transformer := &queryTransformerStub{result: &model.QueryTransformResult{
			QueryText:       "semantic query",
			MetadataFilters: map[string]string{"section": "risk", "source": "generated"},
		}}
		generator := &generationAdapterStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(retrieval),
			app.WithQueryTransformer(transformer),
			app.WithGenerationAdapter(generator),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID:       uuid.New(),
			UserID:          dataset.UserID,
			DatasetID:       dataset.DatasetID,
			ModelID:         inferenceModel.ModelID,
			QueryText:       "original question",
			TopK:            3,
			MetadataFilters: map[string]string{"source": "manual"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(transformer.request.QueryText).To(Equal("original question"))
		Expect(transformer.request.UserID).To(Equal(dataset.UserID))
		Expect(retrieval.userID).To(Equal(dataset.UserID))
		Expect(retrieval.queryText).To(Equal("semantic query"))
		Expect(retrieval.metadataFilters).To(Equal(map[string]string{"section": "risk", "source": "generated"}))
		Expect(generator.request.Query).To(Equal("original question"))
		Expect(response.QueryText).To(Equal("original question"))
	})

	It("falls back to raw retrieval when query transformation fails", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "retrieved context",
			Similarity:          0.91,
		}}}
		transformer := &queryTransformerStub{err: errors.New("planner unavailable")}
		generator := &generationAdapterStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(retrieval),
			app.WithQueryTransformer(transformer),
			app.WithGenerationAdapter(generator),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "original question",
			TopK:      3,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(retrieval.queryText).To(Equal("original question"))
		Expect(response.QueryText).To(Equal("original question"))
	})

	DescribeTable("reranking",
		func(rerankerEnabled bool, expectedRetrievalTopK int, expectedResponseChunks []int) {
			dataset := validInferenceDataset()
			inferenceModel := validInferenceModel()
			inferenceModel.UserID = dataset.UserID
			inferenceModel.DatasetID = dataset.DatasetID
			retrieved := []model.RetrievedContext{
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 1, SourceText: "first", Similarity: 0.70},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 2, SourceText: "second", Similarity: 0.68},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 3, SourceText: "third", Similarity: 0.65},
				{EmbeddingRecordID: uuid.New(), EmbeddingSnapshotID: dataset.EmbeddingSnapshotID, ChunkIndex: 4, SourceText: "fourth", Similarity: 0.60},
			}
			retrieval := &retrievalClientStub{contexts: retrieved}
			reranker := &rerankerStub{contexts: []model.RetrievedContext{
				withRerankScore(retrieved[2], 0.99),
				withRerankScore(retrieved[0], 0.90),
			}}
			promptStrategy := testPromptStrategy()
			options := []app.InferenceOption{
				app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
				app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
				app.WithRetrievalClient(retrieval),
				app.WithGenerationAdapter(&generationAdapterStub{}),
				app.WithPromptStrategy(promptStrategy),
				app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
				app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
			}
			if rerankerEnabled {
				options = append(options, app.WithReranker(reranker), app.WithRerankCandidateMultiplier(3))
			}
			uc := app.NewInferenceUsecase(&inferenceModelRepositoryStub{model: inferenceModel}, options...)

			response, err := uc.Generate(context.Background(), model.GenerateRequest{
				RequestID: uuid.New(),
				UserID:    dataset.UserID,
				DatasetID: dataset.DatasetID,
				ModelID:   inferenceModel.ModelID,
				QueryText: "query",
				TopK:      2,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(retrieval.topK).To(Equal(expectedRetrievalTopK))
			if rerankerEnabled {
				Expect(reranker.query).To(Equal("query"))
				Expect(reranker.topK).To(Equal(2))
				Expect(reranker.candidates).To(Equal(retrieved))
			}
			Expect(response.Contexts).To(HaveLen(len(expectedResponseChunks)))
			for i, chunkIndex := range expectedResponseChunks {
				Expect(response.Contexts[i].ChunkIndex).To(Equal(chunkIndex))
			}
			if rerankerEnabled {
				Expect(response.Contexts[0].RerankScore).To(Equal(0.99))
				Expect(response.Contexts[0].Similarity).To(Equal(0.65))
			}
		},
		Entry("uses request topK when reranker is not configured", false, 2, []int{1, 2}),
		Entry("over-fetches, reranks, then packs when reranker is configured", true, 6, []int{3, 1}),
	)

	It("rejects generation before embeddings are ready", func() {
		dataset := validInferenceDataset()
		dataset.ProcessingState = model.DatasetProcessingFeatureMaterialized
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		inferenceModel.DatasetID = dataset.DatasetID
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(&inferenceRequestRepositoryStub{}),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapter(&generationAdapterStub{}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrDatasetNotReady)).To(BeTrue())
	})

	It("rejects generation when the model is not ready", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		inferenceModel.Status = model.ModelStatusFailed
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapter(&generationAdapterStub{}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("rejects generation when the ready model is not loaded by the serving layer", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		inferenceModel.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapter(&generationAdapterStub{}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("rejects generation when the model belongs to a different dataset", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		requestRepository := &inferenceRequestRepositoryStub{}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapter(&generationAdapterStub{}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrModelMismatch)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusFailed))
	})

	It("allows a base model to generate over any requested dataset", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.ModelKind = model.ModelKindBase
		inferenceModel.DatasetID = uuid.Nil
		inferenceModel.TrainingRunID = uuid.Nil
		inferenceModel.AdapterURI = ""
		requestRepository := &inferenceRequestRepositoryStub{}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "base model context",
			Similarity:          0.91,
		}}}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapter(&generationAdapterStub{}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.Answer).To(Equal("generated answer"))
		Expect(retrieval.datasetID).To(Equal(dataset.DatasetID))
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
	})

	It("returns audit recording errors for otherwise successful generations", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.UserID = dataset.UserID
		inferenceModel.DatasetID = dataset.DatasetID
		auditErr := errors.New("audit table unavailable")
		requestRepository := &inferenceRequestRepositoryStub{err: auditErr}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          1,
			SourceText:          "retrieved context",
			Similarity:          0.92,
		}}}
		promptStrategy := testPromptStrategy()
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{model: inferenceModel},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithInferenceRequestRepository(requestRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapter(&generationAdapterStub{}),
			app.WithPromptStrategy(promptStrategy),
			app.WithContextPacker(app.NewContextWindowPacker(promptStrategy)),
			app.WithPromptBuilder(app.NewDefaultPromptBuilder(promptStrategy)),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			RequestID: uuid.New(),
			UserID:    dataset.UserID,
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: "query",
			TopK:      4,
		})

		Expect(response).To(BeNil())
		Expect(errors.Is(err, auditErr)).To(BeTrue())
		Expect(requestRepository.request).NotTo(BeNil())
		Expect(requestRepository.request.Status).To(Equal(model.InferenceRequestStatusCompleted))
		Expect(requestRepository.request.PromptText).To(ContainSubstring("retrieved context"))
		Expect(requestRepository.request.AnswerText).To(Equal("generated answer"))
	})
})

func testPromptStrategy() model.PromptStrategy {
	return model.PromptStrategy{
		Version:          "test-rag-prompt-v1",
		SystemPrompt:     "Use context only.",
		MaxContextTokens: 200,
		MaxContextChunks: 4,
	}
}

func withRerankScore(context model.RetrievedContext, score float64) model.RetrievedContext {
	context.RerankScore = score
	return context
}

func validInferenceModel() *model.InferenceModel {
	return &model.InferenceModel{
		ModelID:           uuid.New(),
		UserID:            uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		ModelKind:         model.ModelKindFineTuned,
		Source:            model.ModelSourceTraining,
		SourceMetadata:    "{}",
		Name:              "sentence-transformer",
		ModelVersion:      1,
		BaseModel:         "base-model",
		ArtifactLocation:  "s3://local-dev-bucket/models/model-1",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "checksum",
		ArtifactSizeBytes: 10,
		AdapterURI:        "s3://local-dev-bucket/models/model-1",
		ServingTarget:     "vllm-local",
		ServingModel:      "sentence-transformer-v1",
		ServingLoadStatus: model.ModelLoadStatusLoaded,
		MetricsMetadata:   "{}",
		Status:            model.ModelStatusReady,
	}
}

func validInferenceDataset() *model.InferenceDataset {
	return &model.InferenceDataset{
		DatasetID:                uuid.New(),
		UserID:                   uuid.New(),
		DatasetVersion:           4,
		ProcessingState:          model.DatasetProcessingEmbeddingsMaterialized,
		StorageLocation:          "s3://local-dev-bucket/features/dataset.parquet",
		TableNamespace:           "features",
		TableName:                "movies",
		TableFormat:              "PARQUET",
		CatalogProvider:          "LOCAL",
		ProcessingProfile:        "TEXT_RAG_PROCESSING_PROFILE",
		SchemaVersion:            2,
		SchemaMetadata:           "{}",
		RawSnapshotID:            uuid.New(),
		FeatureSnapshotID:        uuid.New(),
		EmbeddingSnapshotID:      uuid.New(),
		VectorStore:              "pgvector",
		CollectionName:           "movies",
		EmbeddingDimensions:      384,
		EmbeddingCount:           12,
		EmbeddingStrategyVersion: "rag-v1",
		EmbeddingChunkerName:     "go-token-window",
		EmbeddingChunkerVersion:  "v1",
		EmbeddingChunkSize:       384,
		EmbeddingChunkOverlap:    64,
		EmbeddingProvider:        "ollama",
		EmbeddingModel:           "bge-small-en-v1.5",
	}
}
