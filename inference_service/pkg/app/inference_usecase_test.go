package app_test

import (
	"context"
	"errors"
	"testing"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service app unit test suite")
}

type inferenceModelRepositoryStub struct {
	model          *model.InferenceModel
	upsertedModel  *model.InferenceModel
	idempotencyKey uuid.UUID
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

func (s *inferenceModelRepositoryStub) ReadByID(_ context.Context, modelID uuid.UUID) (*model.InferenceModel, error) {
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

func (s *inferenceDatasetRepositoryStub) ReadDataset(_ context.Context, datasetID uuid.UUID) (*model.InferenceDataset, error) {
	s.readID = datasetID
	if s.err != nil {
		return nil, s.err
	}
	return s.dataset, nil
}

type retrievalClientStub struct {
	datasetID uuid.UUID
	queryText string
	topK      int
	contexts  []model.RetrievedContext
	err       error
}

func (s *retrievalClientStub) SearchEmbeddings(_ context.Context, datasetID uuid.UUID, queryText string, topK int) ([]model.RetrievedContext, error) {
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	if s.err != nil {
		return nil, s.err
	}
	return s.contexts, nil
}

func (s *retrievalClientStub) Close() error {
	return nil
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

		readModel, err := uc.ReadModel(context.Background(), expected.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(readModel).To(Equal(expected))
		Expect(repository.readID).To(Equal(expected.ModelID))
	})

	It("rejects missing model identity", func() {
		uc := app.NewInferenceUsecase(&inferenceModelRepositoryStub{})

		_, err := uc.RecordModelUpdated(context.Background(), &model.InferenceModel{}, uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects ready models without artifact locations", func() {
		uc := app.NewInferenceUsecase(&inferenceModelRepositoryStub{})
		inferenceModel := validInferenceModel()
		inferenceModel.ArtifactLocation = ""

		_, err := uc.RecordModelUpdated(context.Background(), inferenceModel, uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
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

	It("generates from retrieved RAG contexts", func() {
		dataset := validInferenceDataset()
		inferenceModel := validInferenceModel()
		inferenceModel.DatasetID = dataset.DatasetID
		modelRepository := &inferenceModelRepositoryStub{model: inferenceModel}
		datasetRepository := &inferenceDatasetRepositoryStub{dataset: dataset}
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID:   uuid.New(),
			EmbeddingSnapshotID: dataset.EmbeddingSnapshotID,
			ChunkIndex:          2,
			SourceText:          "retrieved context",
			Similarity:          0.87,
		}}}
		generator := &generationAdapterStub{}
		uc := app.NewInferenceUsecase(
			modelRepository,
			app.WithInferenceDatasetRepository(datasetRepository),
			app.WithRetrievalClient(retrieval),
			app.WithGenerationAdapter(generator),
		)

		response, err := uc.Generate(context.Background(), model.GenerateRequest{
			DatasetID: dataset.DatasetID,
			ModelID:   inferenceModel.ModelID,
			QueryText: " what happened? ",
			TopK:      8,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(datasetRepository.readID).To(Equal(dataset.DatasetID))
		Expect(modelRepository.readID).To(Equal(inferenceModel.ModelID))
		Expect(retrieval.datasetID).To(Equal(dataset.DatasetID))
		Expect(retrieval.queryText).To(Equal("what happened?"))
		Expect(retrieval.topK).To(Equal(8))
		Expect(generator.request.Dataset).To(Equal(dataset))
		Expect(generator.request.Model).To(Equal(inferenceModel))
		Expect(response.Answer).To(Equal("generated answer"))
		Expect(response.Contexts).To(HaveLen(1))
	})

	It("rejects generation before embeddings are ready", func() {
		dataset := validInferenceDataset()
		dataset.ProcessingState = model.DatasetProcessingFeatureMaterialized
		uc := app.NewInferenceUsecase(
			&inferenceModelRepositoryStub{},
			app.WithInferenceDatasetRepository(&inferenceDatasetRepositoryStub{dataset: dataset}),
			app.WithRetrievalClient(&retrievalClientStub{}),
			app.WithGenerationAdapter(&generationAdapterStub{}),
		)

		_, err := uc.Generate(context.Background(), model.GenerateRequest{
			DatasetID: dataset.DatasetID,
			QueryText: "query",
		})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrDatasetNotReady)).To(BeTrue())
	})
})

func validInferenceModel() *model.InferenceModel {
	return &model.InferenceModel{
		ModelID:           uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		Name:              "sentence-transformer",
		ModelVersion:      1,
		BaseModel:         "base-model",
		ArtifactLocation:  "s3://local-dev-bucket/models/model-1",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "checksum",
		ArtifactSizeBytes: 10,
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
		ProcessingProfile:        "RAG",
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
