package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	datasetpb "lib/data_contracts_lib/data_registry"
	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type DatasetLifecycleListener interface {
	AddDataset(ctx context.Context, dataset *model.Dataset) error
	UpdateDataset(ctx context.Context, dataset *model.Dataset) error
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
	msgConn.AddListener(s.subscriber, NewDatasetUpdatedEventListener(s.listener))
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

	dataset, err := datasetFromCreatedEvent(resourceKey, event)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.listener.AddDataset(ctx, dataset)
}

type datasetUpdatedEventListener struct {
	listener DatasetLifecycleListener
}

func NewDatasetUpdatedEventListener(listener DatasetLifecycleListener) *datasetUpdatedEventListener {
	log.Trace("NewDatasetUpdatedEventListener")

	return &datasetUpdatedEventListener{listener: listener}
}

func (l *datasetUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetUpdatedEventListener MsgType")

	return msgConn.MsgTypeDatasetUpdated
}

func (l *datasetUpdatedEventListener) NewMessage() *datasetpb.DatasetUpdatedEvent {
	log.Trace("datasetUpdatedEventListener NewMessage")

	return &datasetpb.DatasetUpdatedEvent{}
}

func (l *datasetUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, event *datasetpb.DatasetUpdatedEvent) error {
	log.Trace("datasetUpdatedEventListener Handle")

	dataset, err := datasetFromUpdatedEvent(resourceKey, event)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	return l.listener.UpdateDataset(ctx, dataset)
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

	datasetID, userID, orgID, err := datasetLifecycleIDs(resourceKey, event.GetDatasetId(), event.GetUserId(), event.GetOrgId())
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	ctx = ctxutil.WithActorOrg(ctx, userID, orgID)
	if err := l.listener.DeleteDataset(ctx, datasetID, userID); err != nil {
		if errors.Is(err, domain.ErrResourceNotFound) {
			return msgConn.AlreadyProcessed(err)
		}
		return err
	}
	return nil
}

func datasetFromCreatedEvent(resourceKey uuid.UUID, event *datasetpb.DatasetCreatedEvent) (*model.Dataset, error) {
	log.Trace("datasetFromCreatedEvent")

	if event == nil {
		return nil, fmt.Errorf("dataset created payload is required")
	}
	datasetID, userID, orgID, err := datasetLifecycleIDs(resourceKey, event.GetDatasetId(), event.GetUserId(), event.GetOrgId())
	if err != nil {
		return nil, err
	}
	return datasetFromLifecycleFields(
		datasetID,
		userID,
		orgID,
		event.GetStorageLocation(),
		event.GetTableNamespace(),
		event.GetTableName(),
		event.GetTableFormat(),
		event.GetCatalogProvider(),
		event.GetProcessingProfile(),
		int(event.GetSchemaVersion()),
		event.GetSchemaMetadata(),
	)
}

func datasetFromUpdatedEvent(resourceKey uuid.UUID, event *datasetpb.DatasetUpdatedEvent) (*model.Dataset, error) {
	log.Trace("datasetFromUpdatedEvent")

	if event == nil {
		return nil, fmt.Errorf("dataset updated payload is required")
	}
	datasetID, userID, orgID, err := datasetLifecycleIDs(resourceKey, event.GetDatasetId(), event.GetUserId(), event.GetOrgId())
	if err != nil {
		return nil, err
	}
	return datasetFromLifecycleFields(
		datasetID,
		userID,
		orgID,
		event.GetStorageLocation(),
		event.GetTableNamespace(),
		event.GetTableName(),
		event.GetTableFormat(),
		event.GetCatalogProvider(),
		event.GetProcessingProfile(),
		int(event.GetSchemaVersion()),
		event.GetSchemaMetadata(),
	)
}

func datasetFromLifecycleFields(
	datasetID uuid.UUID,
	userID uuid.UUID,
	orgID uuid.UUID,
	storageLocation string,
	tableNamespace string,
	tableName string,
	tableFormat string,
	catalogProvider string,
	processingProfile string,
	schemaVersion int,
	schemaMetadata string,
) (*model.Dataset, error) {
	log.Trace("datasetFromLifecycleFields")

	tableNamespace, err := requiredLifecycleString("table namespace", tableNamespace)
	if err != nil {
		return nil, err
	}
	tableName, err = requiredLifecycleString("table name", tableName)
	if err != nil {
		return nil, err
	}
	tableFormat, err = requiredLifecycleString("table format", tableFormat)
	if err != nil {
		return nil, err
	}
	catalogProvider, err = requiredLifecycleString("catalog provider", catalogProvider)
	if err != nil {
		return nil, err
	}
	processingProfile, err = requiredLifecycleString("processing profile", processingProfile)
	if err != nil {
		return nil, err
	}
	schemaMetadata = strings.TrimSpace(schemaMetadata)
	if schemaMetadata == "" {
		return nil, fmt.Errorf("schema metadata is required")
	}
	return &model.Dataset{
		DatasetID:         datasetID,
		UserID:            userID,
		OrgID:             orgID,
		StorageLocation:   strings.TrimSpace(storageLocation),
		TableNamespace:    tableNamespace,
		TableName:         tableName,
		TableFormat:       tableFormat,
		CatalogProvider:   catalogProvider,
		ProcessingProfile: processingProfile,
		SchemaVersion:     schemaVersion,
		SchemaMetadata:    schemaMetadata,
	}, nil
}

func requiredLifecycleString(fieldName string, value string) (string, error) {
	log.Trace("requiredLifecycleString")

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	return value, nil
}

func datasetLifecycleIDs(resourceKey uuid.UUID, datasetIDRaw string, userIDRaw string, orgIDRaw string) (uuid.UUID, uuid.UUID, uuid.UUID, error) {
	log.Trace("datasetLifecycleIDs")

	datasetID, err := msgConn.ParseUUID("dataset_id", datasetIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", userIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, err
	}
	orgID, err := msgConn.ParseUUID("org_id", orgIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, err
	}
	return datasetID, userID, orgID, nil
}
