package messaging

import (
	"context"
	"errors"
	"fmt"

	"data_ingestion_service/pkg/domain"
	datasetpb "lib/data_contracts_lib/dataset"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type DatasetLifecycleListener interface {
	AddDataset(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) error
	DeleteDataset(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) error
}

type DatasetLifecycleSubscriber struct {
	subscriber msgConn.Subscriber
	listener   DatasetLifecycleListener
	topics     []string
}

func NewDatasetLifecycleSubscriber(subscriber msgConn.Subscriber, listener DatasetLifecycleListener, topics []string) *DatasetLifecycleSubscriber {
	log.Trace("NewDatasetLifecycleSubscriber")

	return &DatasetLifecycleSubscriber{
		subscriber: subscriber,
		listener:   listener,
		topics:     topics,
	}
}

func (s *DatasetLifecycleSubscriber) Start(ctx context.Context) error {
	log.Trace("DatasetLifecycleSubscriber Start")

	msgConn.AddListener(s.subscriber, NewDatasetCreatedEventListener(s.listener))
	msgConn.AddListener(s.subscriber, NewDatasetDeletedEventListener(s.listener))
	return s.subscriber.Subscribe(ctx, s.topics)
}

type datasetCreatedEventListener struct {
	listener DatasetLifecycleListener
}

func NewDatasetCreatedEventListener(listener DatasetLifecycleListener) *datasetCreatedEventListener {
	log.Trace("NewDatasetCreatedEventListener")

	return &datasetCreatedEventListener{listener: listener}
}

func (l *datasetCreatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetCreatedEventListener MsgType")

	return msgConn.MsgTypeDatasetCreated
}

func (l *datasetCreatedEventListener) NewMessage() *datasetpb.DatasetCreatedEvent {
	log.Trace("datasetCreatedEventListener NewMessage")

	return &datasetpb.DatasetCreatedEvent{}
}

func (l *datasetCreatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, event *datasetpb.DatasetCreatedEvent) error {
	log.Trace("datasetCreatedEventListener Handle")

	datasetID, userID, err := datasetLifecycleIDs(resourceKey, event.GetDatasetId(), event.GetUserId())
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	if err := l.listener.AddDataset(ctx, datasetID, userID); err != nil {
		if errors.Is(err, domain.ErrResourceAlreadyExists) {
			return msgConn.AlreadyProcessed(err)
		}
		return err
	}
	return nil
}

type datasetDeletedEventListener struct {
	listener DatasetLifecycleListener
}

func NewDatasetDeletedEventListener(listener DatasetLifecycleListener) *datasetDeletedEventListener {
	log.Trace("NewDatasetDeletedEventListener")

	return &datasetDeletedEventListener{listener: listener}
}

func (l *datasetDeletedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetDeletedEventListener MsgType")

	return msgConn.MsgTypeDatasetDeleted
}

func (l *datasetDeletedEventListener) NewMessage() *datasetpb.DatasetDeletedEvent {
	log.Trace("datasetDeletedEventListener NewMessage")

	return &datasetpb.DatasetDeletedEvent{}
}

func (l *datasetDeletedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, event *datasetpb.DatasetDeletedEvent) error {
	log.Trace("datasetDeletedEventListener Handle")

	datasetID, userID, err := datasetLifecycleIDs(resourceKey, event.GetDatasetId(), event.GetUserId())
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	if err := l.listener.DeleteDataset(ctx, datasetID, userID); err != nil {
		if errors.Is(err, domain.ErrResourceNotFound) {
			return msgConn.AlreadyProcessed(err)
		}
		return err
	}
	return nil
}

func datasetLifecycleIDs(resourceKey uuid.UUID, datasetIDRaw string, userIDRaw string) (uuid.UUID, uuid.UUID, error) {
	log.Trace("datasetLifecycleIDs")

	datasetID, err := msgConn.ParseUUID("dataset_id", datasetIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return uuid.Nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", userIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return datasetID, userID, nil
}
