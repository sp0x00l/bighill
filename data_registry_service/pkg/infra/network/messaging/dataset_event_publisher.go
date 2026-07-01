package messaging

import (
	"context"
	"fmt"

	"data_registry_service/pkg/domain/model"
	datasetpb "lib/data_contracts_lib/dataset"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type DatasetEventPublisher struct {
	publisher shared.Publisher
	topic     string
}

func NewDatasetEventPublisher(publisher shared.Publisher, topic string) *DatasetEventPublisher {
	log.Trace("NewDatasetEventPublisher")

	return &DatasetEventPublisher{
		publisher: publisher,
		topic:     topic,
	}
}

func (p *DatasetEventPublisher) PublishDatasetCreated(ctx context.Context, dataset *model.Dataset) error {
	log.Trace("DatasetEventPublisher PublishDatasetCreated")

	if dataset == nil || dataset.ID == uuid.Nil || dataset.UserID == uuid.Nil {
		return fmt.Errorf("dataset id and user id are required")
	}
	return p.publish(ctx, dataset.ID, shared.MsgTypeDatasetCreated, &datasetpb.DatasetCreatedEvent{
		DatasetId: dataset.ID.String(),
		UserId:    dataset.UserID.String(),
	})
}

func (p *DatasetEventPublisher) PublishDatasetDeleted(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	log.Trace("DatasetEventPublisher PublishDatasetDeleted")

	if datasetID == uuid.Nil || userID == uuid.Nil {
		return fmt.Errorf("dataset id and user id are required")
	}
	return p.publish(ctx, datasetID, shared.MsgTypeDatasetDeleted, &datasetpb.DatasetDeletedEvent{
		DatasetId: datasetID.String(),
		UserId:    userID.String(),
	})
}

func (p *DatasetEventPublisher) publish(ctx context.Context, resourceKey uuid.UUID, msgType shared.MsgType, payload proto.Message) error {
	log.Trace("DatasetEventPublisher publish")

	if p == nil || p.publisher == nil {
		return nil
	}
	if p.topic == "" {
		return fmt.Errorf("data registry topic is required")
	}
	return p.publisher.Publish(ctx, p.topic, shared.Message{
		ResourceKey: resourceKey,
		MsgType:     msgType,
	}, payload)
}
