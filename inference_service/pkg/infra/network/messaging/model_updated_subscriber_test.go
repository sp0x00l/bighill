package messaging_test

import (
	"context"
	"testing"

	"inference_service/pkg/domain/model"
	inferencemessaging "inference_service/pkg/infra/network/messaging"

	datasetpb "lib/data_contracts_lib/data_registry"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service messaging unit test suite")
}

type recordingInferenceUsecase struct {
	model          *model.InferenceModel
	dataset        *model.InferenceDataset
	idempotencyKey uuid.UUID
	err            error
}

func (u *recordingInferenceUsecase) RecordModelUpdated(_ context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	u.model = inferenceModel
	u.idempotencyKey = idempotencyKey
	return inferenceModel, u.err
}

func (u *recordingInferenceUsecase) RecordDatasetUpdated(_ context.Context, dataset *model.InferenceDataset, idempotencyKey uuid.UUID) (*model.InferenceDataset, error) {
	u.dataset = dataset
	u.idempotencyKey = idempotencyKey
	return dataset, u.err
}

func (u *recordingInferenceUsecase) ReadModel(context.Context, uuid.UUID, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (u *recordingInferenceUsecase) ListEndpoints(context.Context, uuid.UUID) ([]*model.PublishedEndpoint, error) {
	return nil, nil
}

func (u *recordingInferenceUsecase) GenerateForEndpoint(context.Context, uuid.UUID, model.GenerateRequest) (*model.GenerateResponse, error) {
	return nil, nil
}

func (u *recordingInferenceUsecase) Generate(context.Context, model.GenerateRequest) (*model.GenerateResponse, error) {
	return nil, nil
}

func (u *recordingInferenceUsecase) RecordFeedback(context.Context, *model.InferenceFeedback, uuid.UUID) (*model.InferenceFeedback, error) {
	return nil, nil
}

func (u *recordingInferenceUsecase) ExportPreferenceDataset(context.Context, model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	return nil, nil
}

var _ = Describe("InferenceTopics", func() {
	It("subscribes to registry topics", func() {
		Expect(inferencemessaging.InferenceTopics{
			ModelRegistry: "model_registry",
			DataRegistry:  "data_registry",
		}.List()).To(Equal([]string{"model_registry", "data_registry"}))
	})
})

var _ = Describe("ModelUpdatedEventListener", func() {
	It("exposes the model updated message type", func() {
		listener := inferencemessaging.NewModelUpdatedEventListener(&recordingInferenceUsecase{})

		Expect(listener.MsgType()).To(Equal(sharedmessaging.MsgTypeModelUpdated))
		Expect(listener.NewMessage()).To(Equal(&modelregistrypb.ModelUpdatedEvent{}))
	})

	It("maps a model updated event into the inference use case", func() {
		modelID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewModelUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:           modelID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			ModelKind:         "FINE_TUNED",
			Source:            "TRAINING",
			SourceMetadata:    "{}",
			Name:              "movie-ranker",
			ModelVersion:      2,
			BaseModel:         "base-model",
			ArtifactLocation:  "s3://local-dev-bucket/models/model-1",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "checksum",
			ArtifactSizeBytes: 42,
			AdapterUri:        "s3://local-dev-bucket/models/model-1",
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v2",
			ServingProtocol:   "OPENAI_CHAT_COMPLETIONS",
			ServingLoadStatus: "LOADED",
			MetricsMetadata:   `{"accuracy":0.9}`,
			Status:            "READY",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.model.ModelID).To(Equal(modelID))
		Expect(uc.model.UserID).To(Equal(userID))
		Expect(uc.model.OrgID).To(Equal(orgID))
		Expect(uc.model.TrainingRunID).To(Equal(trainingRunID))
		Expect(uc.model.DatasetID).To(Equal(datasetID))
		Expect(uc.model.ModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(uc.model.Source).To(Equal(model.ModelSourceTraining))
		Expect(uc.model.Status).To(Equal(model.ModelStatusReady))
		Expect(uc.model.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(uc.model.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		Expect(uc.model.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/model-1"))
		Expect(uc.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("maps an ingested base model update without training or dataset ids", func() {
		modelID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewModelUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:           modelID.String(),
			ModelKind:         "BASE",
			Source:            "UPLOAD",
			SourceUri:         "s3://local-dev-bucket/models/base-model",
			SourceMetadata:    `{"upload_id":"u1"}`,
			Name:              "uploaded-base",
			ModelVersion:      1,
			BaseModel:         "s3://local-dev-bucket/models/base-model",
			ArtifactLocation:  "s3://local-dev-bucket/models/base-model",
			ArtifactFormat:    "HF_FULL_WEIGHTS",
			ArtifactChecksum:  "checksum",
			ArtifactSizeBytes: 42,
			ServingTarget:     "vllm-local",
			ServingModel:      "uploaded-base-v1",
			ServingProtocol:   "OPENAI_CHAT_COMPLETIONS",
			ServingLoadStatus: "LOADED",
			Status:            "READY",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.model.ModelID).To(Equal(modelID))
		Expect(uc.model.UserID).To(Equal(uuid.Nil))
		Expect(uc.model.OrgID).To(Equal(uuid.Nil))
		Expect(uc.model.TrainingRunID).To(Equal(uuid.Nil))
		Expect(uc.model.DatasetID).To(Equal(uuid.Nil))
		Expect(uc.model.ModelKind).To(Equal(model.ModelKindBase))
		Expect(uc.model.Source).To(Equal(model.ModelSourceUpload))
		Expect(uc.model.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		Expect(uc.model.SourceURI).To(Equal("s3://local-dev-bucket/models/base-model"))
		Expect(uc.model.SourceMetadata).To(MatchJSON(`{"upload_id":"u1"}`))
		Expect(uc.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("returns non-retryable errors for missing model metadata", func() {
		modelID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewModelUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:       modelID.String(),
			UserId:        uuid.NewString(),
			OrgId:         uuid.NewString(),
			TrainingRunId: uuid.NewString(),
			DatasetId:     uuid.NewString(),
			BaseModel:     "base-model",
			Status:        "PENDING",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("returns non-retryable errors for invalid wire payloads", func() {
		modelID := uuid.New()
		listener := inferencemessaging.NewModelUpdatedEventListener(&recordingInferenceUsecase{})

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:       uuid.NewString(),
			TrainingRunId: uuid.NewString(),
			DatasetId:     uuid.NewString(),
			Status:        "READY",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})
})

var _ = Describe("DatasetUpdatedEventListener", func() {
	It("exposes the dataset updated message type", func() {
		listener := inferencemessaging.NewDatasetUpdatedEventListener(&recordingInferenceUsecase{})

		Expect(listener.MsgType()).To(Equal(sharedmessaging.MsgTypeDatasetUpdated))
		Expect(listener.NewMessage()).To(Equal(&datasetpb.DatasetUpdatedEvent{}))
	})

	It("maps a dataset updated event into the inference use case", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		rawSnapshotID := uuid.New()
		featureSnapshotID := uuid.New()
		embeddingSnapshotID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewDatasetUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:                datasetID.String(),
			UserId:                   userID.String(),
			OrgId:                    orgID.String(),
			DatasetVersion:           5,
			ProcessingState:          "EMBEDDINGS_MATERIALIZED",
			StorageLocation:          "s3://lakehouse/features/movies.parquet",
			TableNamespace:           "features",
			TableName:                "movies",
			TableFormat:              "PARQUET",
			CatalogProvider:          "LOCAL",
			SchemaVersion:            2,
			SchemaMetadata:           `{"columns":[]}`,
			RawSnapshotId:            rawSnapshotID.String(),
			FeatureSnapshotId:        featureSnapshotID.String(),
			EmbeddingSnapshotId:      embeddingSnapshotID.String(),
			VectorStore:              "pgvector",
			CollectionName:           "movies",
			EmbeddingDimensions:      384,
			EmbeddingCount:           9,
			ProcessingProfile:        "TEXT_RAG_PROCESSING_PROFILE",
			EmbeddingStrategyVersion: "rag-v1",
			EmbeddingChunkerName:     "go-token-window",
			EmbeddingChunkerVersion:  "v1",
			EmbeddingChunkSize:       384,
			EmbeddingChunkOverlap:    64,
			EmbeddingProvider:        "ollama",
			EmbeddingModel:           "bge-small-en-v1.5",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.dataset.DatasetID).To(Equal(datasetID))
		Expect(uc.dataset.UserID).To(Equal(userID))
		Expect(uc.dataset.OrgID).To(Equal(orgID))
		Expect(uc.dataset.DatasetVersion).To(Equal(5))
		Expect(uc.dataset.ProcessingState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
		Expect(uc.dataset.RawSnapshotID).To(Equal(rawSnapshotID))
		Expect(uc.dataset.FeatureSnapshotID).To(Equal(featureSnapshotID))
		Expect(uc.dataset.EmbeddingSnapshotID).To(Equal(embeddingSnapshotID))
		Expect(uc.dataset.EmbeddingDimensions).To(Equal(384))
		Expect(uc.dataset.EmbeddingCount).To(Equal(int64(9)))
		Expect(uc.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("returns non-retryable errors for missing dataset metadata", func() {
		datasetID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewDatasetUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:       datasetID.String(),
			UserId:          uuid.NewString(),
			OrgId:           uuid.NewString(),
			ProcessingState: "PENDING",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("returns non-retryable errors for invalid dataset payloads", func() {
		datasetID := uuid.New()
		listener := inferencemessaging.NewDatasetUpdatedEventListener(&recordingInferenceUsecase{})

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:       datasetID.String(),
			UserId:          uuid.NewString(),
			OrgId:           uuid.NewString(),
			ProcessingState: "EMBEDDINGS_MATERIALIZED",
			RawSnapshotId:   "not-a-uuid",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})
})
