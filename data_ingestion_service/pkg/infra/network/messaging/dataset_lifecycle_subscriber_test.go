package messaging_test

import (
	"context"
	"testing"

	"data_ingestion_service/pkg/domain/model"
	ingestionmessaging "data_ingestion_service/pkg/infra/network/messaging"
	datasetpb "lib/data_contracts_lib/dataset"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data ingestion messaging test suite")
}

type datasetLifecycleUsecaseStub struct {
	addDataset      *model.Dataset
	updateDataset   *model.Dataset
	deleteDatasetID uuid.UUID
	deleteUserID    uuid.UUID
	err             error
}

func (s *datasetLifecycleUsecaseStub) AddDataset(_ context.Context, dataset *model.Dataset) error {
	s.addDataset = dataset
	return s.err
}

func (s *datasetLifecycleUsecaseStub) UpdateDataset(_ context.Context, dataset *model.Dataset) error {
	s.updateDataset = dataset
	return s.err
}

func (s *datasetLifecycleUsecaseStub) DeleteDataset(_ context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	s.deleteDatasetID = datasetID
	s.deleteUserID = userID
	return s.err
}

var _ = Describe("Dataset lifecycle event listeners", func() {
	It("adds datasets from dataset-created events", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &datasetLifecycleUsecaseStub{}
		listener := ingestionmessaging.NewDatasetCreatedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetCreatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.addDataset.DatasetID).To(Equal(datasetID))
		Expect(uc.addDataset.UserID).To(Equal(userID))
		Expect(uc.addDataset.TableNamespace).To(Equal("features"))
		Expect(uc.addDataset.ProcessingProfile).To(Equal("TEXT_RAG"))
		Expect(listener.MsgType()).To(Equal(shared.MsgTypeDatasetCreated))
	})

	It("updates datasets from dataset-updated events", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &datasetLifecycleUsecaseStub{}
		listener := ingestionmessaging.NewDatasetUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
			SchemaVersion:     2,
			SchemaMetadata:    `{"columns":["title"]}`,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.updateDataset.DatasetID).To(Equal(datasetID))
		Expect(uc.updateDataset.UserID).To(Equal(userID))
		Expect(uc.updateDataset.SchemaVersion).To(Equal(2))
		Expect(uc.updateDataset.SchemaMetadata).To(Equal(`{"columns":["title"]}`))
		Expect(listener.MsgType()).To(Equal(shared.MsgTypeDatasetUpdated))
	})

	It("deletes datasets from dataset-deleted events", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		uc := &datasetLifecycleUsecaseStub{}
		listener := ingestionmessaging.NewDatasetDeletedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetDeletedEvent{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.deleteDatasetID).To(Equal(datasetID))
		Expect(uc.deleteUserID).To(Equal(userID))
		Expect(listener.MsgType()).To(Equal(shared.MsgTypeDatasetDeleted))
	})

	It("classifies mismatched resource keys as non-retryable", func() {
		listener := ingestionmessaging.NewDatasetCreatedEventListener(&datasetLifecycleUsecaseStub{})

		err := listener.Handle(context.Background(), uuid.New(), &datasetpb.DatasetCreatedEvent{
			DatasetId:         uuid.NewString(),
			UserId:            uuid.NewString(),
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})

	It("classifies missing materialization metadata as non-retryable", func() {
		datasetID := uuid.New()
		listener := ingestionmessaging.NewDatasetCreatedEventListener(&datasetLifecycleUsecaseStub{})

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetCreatedEvent{
			DatasetId: datasetID.String(),
			UserId:    uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})
})
