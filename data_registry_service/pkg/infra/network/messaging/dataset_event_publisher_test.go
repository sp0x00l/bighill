package messaging_test

import (
	"context"

	"data_registry_service/pkg/domain/model"
	registrymessaging "data_registry_service/pkg/infra/network/messaging"
	datasetpb "lib/data_contracts_lib/dataset"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type registryPublishClientStub struct {
	topic   string
	message shared.Message
	payload proto.Message
}

func (s *registryPublishClientStub) Publish(_ context.Context, topic string, message shared.Message, payload proto.Message) error {
	s.topic = topic
	s.message = message
	s.payload = payload
	return nil
}

func (s *registryPublishClientStub) Close() {}

var _ = Describe("DatasetEventPublisher", func() {
	It("publishes dataset-created events to the data registry topic", func() {
		client := &registryPublishClientStub{}
		publisher := registrymessaging.NewDatasetEventPublisher(client, "data_registry")
		dataset := &model.Dataset{ID: uuid.New(), UserID: uuid.New()}

		Expect(publisher.PublishDatasetCreated(context.Background(), dataset)).To(Succeed())

		Expect(client.topic).To(Equal("data_registry"))
		Expect(client.message.ResourceKey).To(Equal(dataset.ID))
		Expect(client.message.MsgType).To(Equal(shared.MsgTypeDatasetCreated))
		event, ok := client.payload.(*datasetpb.DatasetCreatedEvent)
		Expect(ok).To(BeTrue())
		Expect(event.DatasetId).To(Equal(dataset.ID.String()))
		Expect(event.UserId).To(Equal(dataset.UserID.String()))
	})

	It("publishes dataset-deleted events to the data registry topic", func() {
		client := &registryPublishClientStub{}
		publisher := registrymessaging.NewDatasetEventPublisher(client, "data_registry")
		datasetID := uuid.New()
		userID := uuid.New()

		Expect(publisher.PublishDatasetDeleted(context.Background(), datasetID, userID)).To(Succeed())

		Expect(client.topic).To(Equal("data_registry"))
		Expect(client.message.ResourceKey).To(Equal(datasetID))
		Expect(client.message.MsgType).To(Equal(shared.MsgTypeDatasetDeleted))
		event, ok := client.payload.(*datasetpb.DatasetDeletedEvent)
		Expect(ok).To(BeTrue())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
	})
})
