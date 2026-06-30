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
		uc := &materializationUsecaseStub{}
		listener := registrymessaging.NewRawSnapshotReadyEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &featurepb.RawSnapshotReadyEvent{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			RawSnapshotId:   uuid.NewString(),
			StorageLocation: "s3://local-dev-bucket/lakehouse/raw/data.parquet",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.advancedDatasetID).To(Equal(datasetID))
		Expect(uc.advancedUserID).To(Equal(userID))
		Expect(uc.advancedState).To(Equal(model.DatasetProcessingRawMaterialized))
	})

	It("records table metadata when a feature snapshot is ready", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &materializationUsecaseStub{}
		listener := registrymessaging.NewFeatureSnapshotReadyEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &featurepb.FeatureSnapshotReadyEvent{
			FeatureSnapshotId: uuid.NewString(),
			RawSnapshotId:     uuid.NewString(),
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			StorageLocation:   "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			SchemaVersion:     2,
			SchemaMetadata:    `{"columns":["title"]}`,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.recordedDataset.ID).To(Equal(datasetID))
		Expect(uc.recordedDataset.UserID).To(Equal(userID))
		Expect(uc.recordedDataset.TableNamespace).To(Equal("features"))
		Expect(uc.recordedDataset.TableName).To(Equal("movies"))
		Expect(uc.recordedDataset.TableFormat).To(Equal(model.Parquet))
		Expect(uc.recordedState).To(Equal(model.DatasetProcessingFeatureMaterialized))
	})

	It("advances state when embeddings are ready", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &materializationUsecaseStub{}
		listener := registrymessaging.NewEmbeddingSnapshotReadyEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &featurepb.EmbeddingSnapshotReadyEvent{
			EmbeddingSnapshotId: uuid.NewString(),
			FeatureSnapshotId:   uuid.NewString(),
			DatasetId:           datasetID.String(),
			UserId:              userID.String(),
			VectorStore:         "pgvector",
			CollectionName:      "movies",
			EmbeddingDimensions: 384,
			EmbeddingCount:      2,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.advancedDatasetID).To(Equal(datasetID))
		Expect(uc.advancedUserID).To(Equal(userID))
		Expect(uc.advancedState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
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
