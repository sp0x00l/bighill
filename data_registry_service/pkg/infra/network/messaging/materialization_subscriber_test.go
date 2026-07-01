package messaging_test

import (
	"context"
	"testing"

	"data_registry_service/pkg/domain/model"
	registrymessaging "data_registry_service/pkg/infra/network/messaging"
	featurepb "lib/data_contracts_lib/feature_materializer"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry messaging test suite")
}

type materializationUsecaseStub struct {
	advancedDatasetID uuid.UUID
	advancedUserID    uuid.UUID
	advancedState     model.ProcessingState
	recordedDataset   *model.Dataset
	recordedState     model.ProcessingState
	err               error
}

func (s *materializationUsecaseStub) AdvanceDatasetProcessingState(_ context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, error) {
	s.advancedDatasetID = datasetID
	s.advancedUserID = userID
	s.advancedState = state
	return &model.Dataset{ID: datasetID, UserID: userID, ProcessingState: state}, s.err
}

func (s *materializationUsecaseStub) RecordDatasetMaterialization(_ context.Context, dataset *model.Dataset, state model.ProcessingState) (*model.Dataset, error) {
	s.recordedDataset = dataset
	s.recordedState = state
	return dataset, s.err
}

var _ = Describe("Materialization event listeners", func() {
	It("advances state when a raw snapshot is ready", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		rawSnapshotID := uuid.New()
		uc := &materializationUsecaseStub{}
		listener := registrymessaging.NewRawSnapshotReadyEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &featurepb.RawSnapshotReadyEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			RawSnapshotId:     rawSnapshotID.String(),
			StorageLocation:   "s3://local-dev-bucket/lakehouse/raw/data.parquet",
			TableNamespace:    "raw",
			TableName:         "movies_raw",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
			ProcessingProfile: "TEXT_RAG",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.recordedDataset.ID).To(Equal(datasetID))
		Expect(uc.recordedDataset.UserID).To(Equal(userID))
		Expect(uc.recordedDataset.RawSnapshotID).To(Equal(rawSnapshotID))
		Expect(uc.recordedDataset.TableName).To(Equal("movies_raw"))
		Expect(uc.recordedDataset.ProcessingProfile).To(Equal(model.TextRAGProfile))
		Expect(uc.recordedState).To(Equal(model.DatasetProcessingRawMaterialized))
	})

	It("records table metadata when a feature snapshot is ready", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		featureSnapshotID := uuid.New()
		rawSnapshotID := uuid.New()
		uc := &materializationUsecaseStub{}
		listener := registrymessaging.NewFeatureSnapshotReadyEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &featurepb.FeatureSnapshotReadyEvent{
			FeatureSnapshotId: featureSnapshotID.String(),
			RawSnapshotId:     rawSnapshotID.String(),
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			StorageLocation:   "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			SchemaVersion:     2,
			SchemaMetadata:    `{"columns":["title"]}`,
			ProcessingProfile: "TEXT_RAG",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.recordedDataset.ID).To(Equal(datasetID))
		Expect(uc.recordedDataset.UserID).To(Equal(userID))
		Expect(uc.recordedDataset.TableNamespace).To(Equal("features"))
		Expect(uc.recordedDataset.TableName).To(Equal("movies"))
		Expect(uc.recordedDataset.TableFormat).To(Equal(model.Parquet))
		Expect(uc.recordedDataset.ProcessingProfile).To(Equal(model.TextRAGProfile))
		Expect(uc.recordedDataset.RawSnapshotID).To(Equal(rawSnapshotID))
		Expect(uc.recordedDataset.FeatureSnapshotID).To(Equal(featureSnapshotID))
		Expect(uc.recordedState).To(Equal(model.DatasetProcessingFeatureMaterialized))
	})

	It("advances state when embeddings are ready", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		featureSnapshotID := uuid.New()
		embeddingSnapshotID := uuid.New()
		uc := &materializationUsecaseStub{}
		listener := registrymessaging.NewEmbeddingSnapshotReadyEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &featurepb.EmbeddingSnapshotReadyEvent{
			EmbeddingSnapshotId: embeddingSnapshotID.String(),
			FeatureSnapshotId:   featureSnapshotID.String(),
			DatasetId:           datasetID.String(),
			UserId:              userID.String(),
			VectorStore:         "pgvector",
			CollectionName:      "movies",
			EmbeddingDimensions: 384,
			EmbeddingCount:      2,
			StrategyVersion:     "rag-v1",
			ChunkerName:         "go-token-window",
			ChunkerVersion:      "v1",
			ChunkSize:           384,
			ChunkOverlap:        64,
			EmbeddingProvider:   "ollama",
			EmbeddingModel:      "bge-small-en-v1.5",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.recordedDataset.ID).To(Equal(datasetID))
		Expect(uc.recordedDataset.UserID).To(Equal(userID))
		Expect(uc.recordedDataset.FeatureSnapshotID).To(Equal(featureSnapshotID))
		Expect(uc.recordedDataset.EmbeddingSnapshotID).To(Equal(embeddingSnapshotID))
		Expect(uc.recordedDataset.VectorStore).To(Equal("pgvector"))
		Expect(uc.recordedDataset.EmbeddingStrategyVersion).To(Equal("rag-v1"))
		Expect(uc.recordedDataset.EmbeddingProvider).To(Equal("ollama"))
		Expect(uc.recordedDataset.EmbeddingModel).To(Equal("bge-small-en-v1.5"))
		Expect(uc.recordedState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
	})

	It("returns non-retryable errors for invalid feature-ready payloads", func() {
		datasetID := uuid.New()
		listener := registrymessaging.NewFeatureSnapshotReadyEventListener(&materializationUsecaseStub{})

		err := listener.Handle(context.Background(), datasetID, &featurepb.FeatureSnapshotReadyEvent{
			DatasetId:       datasetID.String(),
			UserId:          uuid.NewString(),
			StorageLocation: "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     "NOT_A_FORMAT",
			CatalogProvider: "LOCAL",
		})

		Expect(err).To(HaveOccurred())
		Expect(msgConn.IsNonRetryable(err)).To(BeTrue())
	})

	It("lists all subscribed materialization topics", func() {
		topics := registrymessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		}

		Expect(topics.List()).To(Equal([]string{"feature_materializer"}))
	})
})
