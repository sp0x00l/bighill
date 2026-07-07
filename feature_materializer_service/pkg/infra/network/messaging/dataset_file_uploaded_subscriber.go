package messaging

import (
	"context"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain/model"
	dataregistrypb "lib/data_contracts_lib/data_registry"
	ingestionpb "lib/data_contracts_lib/ingestion"
	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const parquetContentType = "application/vnd.apache.parquet"

type DatasetFileUploadedListener interface {
	StartMaterializationWorkflow(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) error
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
	msgConn.AddListener(s.subscriber, &datasetCreatedEventListener{listener: s.listener})
	msgConn.AddListener(s.subscriber, &datasetUpdatedEventListener{listener: s.listener})
	return s.subscriber.Subscribe(ctx, s.topics)
}

type datasetFileUploadedEventListener struct {
	listener DatasetFileUploadedListener
}

func NewDatasetFileUploadedEventListener(listener DatasetFileUploadedListener) *datasetFileUploadedEventListener {
	log.Trace("NewDatasetFileUploadedEventListener")

	return &datasetFileUploadedEventListener{
		listener: listener,
	}
}

func (l *datasetFileUploadedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetFileUploadedEventListener MsgType")

	return msgConn.MsgTypeDatasetFileUploaded
}

func (l *datasetFileUploadedEventListener) NewMessage() *ingestionpb.DatasetFileUploadedEvent {
	log.Trace("datasetFileUploadedEventListener NewMessage")

	return &ingestionpb.DatasetFileUploadedEvent{}
}

func (l *datasetFileUploadedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *ingestionpb.DatasetFileUploadedEvent) error {
	log.Trace("datasetFileUploadedEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("dataset file uploaded listener is nil"))
	}
	datasetFile, idempotencyKey, err := eventToDatasetFile(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	ctx = ctxutil.WithActorOrg(ctx, datasetFile.UserID, datasetFile.OrgID)
	return l.listener.StartMaterializationWorkflow(ctx, datasetFile, idempotencyKey)
}

func eventToDatasetFile(resourceKey uuid.UUID, payload *ingestionpb.DatasetFileUploadedEvent) (*model.DatasetFile, uuid.UUID, error) {
	log.Trace("eventToDatasetFile")

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
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
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
	tableNamespace, err := requiredDatasetFileUploadedString("table namespace", payload.GetTableNamespace())
	if err != nil {
		return nil, uuid.Nil, err
	}
	tableName, err := requiredDatasetFileUploadedString("table name", payload.GetTableName())
	if err != nil {
		return nil, uuid.Nil, err
	}
	tableFormat, err := requiredDatasetFileUploadedString("table format", payload.GetTableFormat())
	if err != nil {
		return nil, uuid.Nil, err
	}
	catalogProvider, err := requiredDatasetFileUploadedString("catalog provider", payload.GetCatalogProvider())
	if err != nil {
		return nil, uuid.Nil, err
	}
	processingProfile, err := model.ToProcessingProfile(payload.GetProcessingProfile())
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("processing profile is invalid: %w", err)
	}

	datasetFile := &model.DatasetFile{
		DatasetID:         datasetID,
		UserID:            userID,
		OrgID:             orgID,
		StorageLocation:   storageLocation,
		ContentType:       contentType,
		FileExtension:     fileExtension,
		TableNamespace:    tableNamespace,
		TableName:         tableName,
		TableFormat:       tableFormat,
		CatalogProvider:   catalogProvider,
		ProcessingProfile: processingProfile,
	}
	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(datasetID.String()+":"+storageLocation))
	return datasetFile, idempotencyKey, nil
}

type datasetCreatedEventListener struct {
	listener DatasetFileUploadedListener
}

func NewDatasetCreatedEventListener(listener DatasetFileUploadedListener) *datasetCreatedEventListener {
	log.Trace("NewDatasetCreatedEventListener")

	return &datasetCreatedEventListener{listener: listener}
}

func (l *datasetCreatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetCreatedEventListener MsgType")

	return msgConn.MsgTypeDatasetCreated
}

func (l *datasetCreatedEventListener) NewMessage() *dataregistrypb.DatasetCreatedEvent {
	log.Trace("datasetCreatedEventListener NewMessage")

	return &dataregistrypb.DatasetCreatedEvent{}
}

func (l *datasetCreatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *dataregistrypb.DatasetCreatedEvent) error {
	log.Trace("datasetCreatedEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("dataset created listener is nil"))
	}
	datasetFile, idempotencyKey, ok, err := datasetCreatedToSourceDatasetFile(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	if !ok {
		return nil
	}
	ctx = ctxutil.WithActorOrg(ctx, datasetFile.UserID, datasetFile.OrgID)
	return l.listener.StartMaterializationWorkflow(ctx, datasetFile, idempotencyKey)
}

type datasetUpdatedEventListener struct {
	listener DatasetFileUploadedListener
}

func NewDatasetUpdatedEventListener(listener DatasetFileUploadedListener) *datasetUpdatedEventListener {
	log.Trace("NewDatasetUpdatedEventListener")

	return &datasetUpdatedEventListener{listener: listener}
}

func (l *datasetUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetUpdatedEventListener MsgType")

	return msgConn.MsgTypeDatasetUpdated
}

func (l *datasetUpdatedEventListener) NewMessage() *dataregistrypb.DatasetUpdatedEvent {
	log.Trace("datasetUpdatedEventListener NewMessage")

	return &dataregistrypb.DatasetUpdatedEvent{}
}

func (l *datasetUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *dataregistrypb.DatasetUpdatedEvent) error {
	log.Trace("datasetUpdatedEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("dataset updated listener is nil"))
	}
	datasetFile, idempotencyKey, ok, err := datasetUpdatedToSourceDatasetFile(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	if !ok {
		return nil
	}
	ctx = ctxutil.WithActorOrg(ctx, datasetFile.UserID, datasetFile.OrgID)
	return l.listener.StartMaterializationWorkflow(ctx, datasetFile, idempotencyKey)
}

func datasetCreatedToSourceDatasetFile(resourceKey uuid.UUID, payload *dataregistrypb.DatasetCreatedEvent) (*model.DatasetFile, uuid.UUID, bool, error) {
	log.Trace("datasetCreatedToSourceDatasetFile")

	if payload == nil {
		return nil, uuid.Nil, false, fmt.Errorf("dataset created payload is required")
	}
	return sourceDatasetFileFromRegistryEvent(sourceDatasetRegistryEvent{
		ResourceKey:       resourceKey,
		DatasetID:         payload.GetDatasetId(),
		UserID:            payload.GetUserId(),
		OrgID:             payload.GetOrgId(),
		DatasetVersion:    payload.GetDatasetVersion(),
		SourceType:        payload.GetSourceType(),
		SourceConnectorID: payload.GetSourceConnectorId(),
		SourceQuery:       payload.GetSourceQuery(),
		SourceDatabase:    payload.GetSourceDatabase(),
		SourceCollection:  payload.GetSourceCollection(),
		TableNamespace:    payload.GetTableNamespace(),
		TableName:         payload.GetTableName(),
		TableFormat:       payload.GetTableFormat(),
		CatalogProvider:   payload.GetCatalogProvider(),
		ProcessingProfile: payload.GetProcessingProfile(),
	})
}

func datasetUpdatedToSourceDatasetFile(resourceKey uuid.UUID, payload *dataregistrypb.DatasetUpdatedEvent) (*model.DatasetFile, uuid.UUID, bool, error) {
	log.Trace("datasetUpdatedToSourceDatasetFile")

	if payload == nil {
		return nil, uuid.Nil, false, fmt.Errorf("dataset updated payload is required")
	}
	return sourceDatasetFileFromRegistryEvent(sourceDatasetRegistryEvent{
		ResourceKey:       resourceKey,
		DatasetID:         payload.GetDatasetId(),
		UserID:            payload.GetUserId(),
		OrgID:             payload.GetOrgId(),
		DatasetVersion:    payload.GetDatasetVersion(),
		SourceType:        payload.GetSourceType(),
		SourceConnectorID: payload.GetSourceConnectorId(),
		SourceQuery:       payload.GetSourceQuery(),
		SourceDatabase:    payload.GetSourceDatabase(),
		SourceCollection:  payload.GetSourceCollection(),
		TableNamespace:    payload.GetTableNamespace(),
		TableName:         payload.GetTableName(),
		TableFormat:       payload.GetTableFormat(),
		CatalogProvider:   payload.GetCatalogProvider(),
		ProcessingProfile: payload.GetProcessingProfile(),
	})
}

type sourceDatasetRegistryEvent struct {
	ResourceKey       uuid.UUID
	DatasetID         string
	UserID            string
	OrgID             string
	DatasetVersion    int32
	SourceType        string
	SourceConnectorID string
	SourceQuery       string
	SourceDatabase    string
	SourceCollection  string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile string
}

func sourceDatasetFileFromRegistryEvent(event sourceDatasetRegistryEvent) (*model.DatasetFile, uuid.UUID, bool, error) {
	log.Trace("sourceDatasetFileFromRegistryEvent")

	sourceConnectorID := strings.TrimSpace(event.SourceConnectorID)
	if sourceConnectorID == "" {
		return nil, uuid.Nil, false, nil
	}
	sourceType := strings.ToLower(strings.TrimSpace(event.SourceType))
	if sourceType == "" {
		return nil, uuid.Nil, false, fmt.Errorf("source type is required")
	}
	if event.ResourceKey == uuid.Nil {
		return nil, uuid.Nil, false, fmt.Errorf("resource key is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", event.DatasetID)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	if datasetID != event.ResourceKey {
		return nil, uuid.Nil, false, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, event.ResourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", event.UserID)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	orgID, err := msgConn.ParseUUID("org_id", event.OrgID)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	connectorID, err := msgConn.ParseUUID("source_connector_id", sourceConnectorID)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	tableNamespace, err := requiredDatasetFileUploadedString("table namespace", event.TableNamespace)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	tableName, err := requiredDatasetFileUploadedString("table name", event.TableName)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	tableFormat, err := requiredDatasetFileUploadedString("table format", event.TableFormat)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	catalogProvider, err := requiredDatasetFileUploadedString("catalog provider", event.CatalogProvider)
	if err != nil {
		return nil, uuid.Nil, false, err
	}
	processingProfile, err := model.ToProcessingProfile(event.ProcessingProfile)
	if err != nil {
		return nil, uuid.Nil, false, fmt.Errorf("processing profile is invalid: %w", err)
	}

	datasetFile := &model.DatasetFile{
		DatasetID:         datasetID,
		UserID:            userID,
		OrgID:             orgID,
		SourceType:        sourceType,
		SourceConnectorID: connectorID,
		SourceQuery:       strings.TrimSpace(event.SourceQuery),
		SourceDatabase:    strings.TrimSpace(event.SourceDatabase),
		SourceCollection:  strings.TrimSpace(event.SourceCollection),
		ContentType:       parquetContentType,
		FileExtension:     "parquet",
		TableNamespace:    tableNamespace,
		TableName:         tableName,
		TableFormat:       tableFormat,
		CatalogProvider:   catalogProvider,
		ProcessingProfile: processingProfile,
	}
	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		datasetID.String(),
		fmt.Sprintf("%d", event.DatasetVersion),
		sourceConnectorID,
		datasetFile.SourceType,
		datasetFile.SourceQuery,
		datasetFile.SourceDatabase,
		datasetFile.SourceCollection,
	}, ":")))
	return datasetFile, idempotencyKey, true, nil
}

func requiredDatasetFileUploadedString(fieldName string, value string) (string, error) {
	log.Trace("requiredDatasetFileUploadedString")

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	return value, nil
}
