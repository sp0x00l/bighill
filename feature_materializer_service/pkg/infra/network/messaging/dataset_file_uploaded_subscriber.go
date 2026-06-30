package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	datasetpb "lib/data_contracts_lib/dataset"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type DatasetFileUploadedListener interface {
	MaterializeRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
}

type DatasetFileUploadedSubscriber interface {
	Start(ctx context.Context) error
}

type datasetFileUploadedSubscriber struct {
	subscriber msgConn.Subscriber
	listener   DatasetFileUploadedListener
	topics     []string
}

func NewDatasetFileUploadedSubscriber(subscriber msgConn.Subscriber, listener DatasetFileUploadedListener, topics []string) DatasetFileUploadedSubscriber {
	log.Trace("NewDatasetFileUploadedSubscriber")

	configureErrorPolicy(subscriber)
	return &datasetFileUploadedSubscriber{
		subscriber: subscriber,
		listener:   listener,
		topics:     topics,
	}
}

func (s *datasetFileUploadedSubscriber) Start(ctx context.Context) error {
	log.Trace("DatasetFileUploadedSubscriber Start")

	msgConn.AddListener(s.subscriber, &datasetFileUploadedEventListener{listener: s.listener})
	return s.subscriber.Subscribe(ctx, s.topics)
}

type datasetFileUploadedEventListener struct {
	listener  DatasetFileUploadedListener
	publisher MaterializationEventPublisher
}

func NewDatasetFileUploadedEventListener(listener DatasetFileUploadedListener) *datasetFileUploadedEventListener {
	log.Trace("NewDatasetFileUploadedEventListener")

	return &datasetFileUploadedEventListener{
		listener: listener,
	}
}

func NewDatasetFileUploadedEventListenerWithPublisher(listener DatasetFileUploadedListener, publisher MaterializationEventPublisher) *datasetFileUploadedEventListener {
	log.Trace("NewDatasetFileUploadedEventListenerWithPublisher")

	return &datasetFileUploadedEventListener{
		listener:  listener,
		publisher: publisher,
	}
}

func (l *datasetFileUploadedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeDatasetFileUploaded
}

func (l *datasetFileUploadedEventListener) NewMessage() *datasetpb.DatasetFileUploadedEvent {
	return &datasetpb.DatasetFileUploadedEvent{}
}

func (l *datasetFileUploadedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *datasetpb.DatasetFileUploadedEvent) error {
	log.Trace("datasetFileUploadedEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("dataset file uploaded listener is nil"))
	}
	datasetFile, idempotencyKey, err := eventToDatasetFile(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	rawSnapshot, err := l.listener.MaterializeRawSnapshot(ctx, datasetFile, idempotencyKey)
	if err != nil {
		existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err)
		if !ok {
			return err
		}
		rawSnapshot = existing
	}
	if rawSnapshot == nil {
		return msgConn.NonRetryable(fmt.Errorf("raw snapshot materializer returned nil"))
	}
	if err := l.publishNext(ctx, rawSnapshot); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("publish raw snapshot materialization events: %w", err)
	}
	return nil
}

func (l *datasetFileUploadedEventListener) publishNext(ctx context.Context, rawSnapshot *model.RawSnapshot) error {
	log.Trace("datasetFileUploadedEventListener publishNext")

	if l.publisher == nil {
		return nil
	}
	if err := l.publisher.PublishRawSnapshotReady(ctx, rawSnapshot); err != nil {
		return err
	}
	return l.publisher.PublishFeatureSnapshotBuildRequested(ctx, rawSnapshot, featureSnapshotIdempotencyKey(rawSnapshot.RawSnapshotID))
}

func eventToDatasetFile(resourceKey uuid.UUID, payload *datasetpb.DatasetFileUploadedEvent) (*model.DatasetFile, uuid.UUID, error) {
	if resourceKey == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("dataset file uploaded payload is required")
	}

	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}

	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, uuid.Nil, err
	}

	storageLocation := strings.TrimSpace(payload.GetStorageLocation())
	if storageLocation == "" {
		return nil, uuid.Nil, fmt.Errorf("storage location is required")
	}
	contentType := strings.TrimSpace(payload.GetContentType())
	if contentType == "" {
		return nil, uuid.Nil, fmt.Errorf("content type is required")
	}
	fileExtension := strings.TrimSpace(payload.GetFileExtension())
	if fileExtension == "" {
		return nil, uuid.Nil, fmt.Errorf("file extension is required")
	}

	datasetFile := &model.DatasetFile{
		DatasetID:       datasetID,
		UserID:          userID,
		StorageLocation: storageLocation,
		ContentType:     contentType,
		FileExtension:   fileExtension,
		TableNamespace:  withDefault(payload.GetTableNamespace(), "default"),
		TableName:       withDefault(payload.GetTableName(), "dataset_"+strings.ReplaceAll(datasetID.String(), "-", "")),
		TableFormat:     withDefault(payload.GetTableFormat(), "PARQUET"),
		CatalogProvider: withDefault(payload.GetCatalogProvider(), "LOCAL"),
	}
	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(datasetID.String()+":"+storageLocation))
	return datasetFile, idempotencyKey, nil
}

func withDefault(value, defaultValue string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue
	}
	return value
}
