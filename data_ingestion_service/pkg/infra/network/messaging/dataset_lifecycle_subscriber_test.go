package messaging_test

import (
	"context"
	"testing"

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
	addDatasetID    uuid.UUID
	addUserID       uuid.UUID
	deleteDatasetID uuid.UUID
	deleteUserID    uuid.UUID
	err             error
}

func (s *datasetLifecycleUsecaseStub) AddDataset(_ context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	s.addDatasetID = datasetID
	s.addUserID = userID
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
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.addDatasetID).To(Equal(datasetID))
		Expect(uc.addUserID).To(Equal(userID))
		Expect(listener.MsgType()).To(Equal(shared.MsgTypeDatasetCreated))
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
			DatasetId: uuid.NewString(),
			UserId:    uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})
})
