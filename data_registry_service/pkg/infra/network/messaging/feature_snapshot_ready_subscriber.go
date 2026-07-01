package messaging

import (
	"context"
	"fmt"
	"strings"

	"data_registry_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type FeatureSnapshotReadyListener interface {
	RecordDatasetMaterialization(ctx context.Context, dataset *model.Dataset, state model.ProcessingState) (*model.Dataset, error)
}

type featureSnapshotReadyEventListener struct {
	listener FeatureSnapshotReadyListener
}

func NewFeatureSnapshotReadyEventListener(listener FeatureSnapshotReadyListener) *featureSnapshotReadyEventListener {
	log.Trace("NewFeatureSnapshotReadyEventListener")

	return &featureSnapshotReadyEventListener{
		listener: listener,
	}
}

func (l *featureSnapshotReadyEventListener) MsgType() msgConn.MsgType {
	log.Trace("featureSnapshotReadyEventListener MsgType")

	return msgConn.MsgTypeFeatureSnapshotReady
}

func (l *featureSnapshotReadyEventListener) NewMessage() *featurepb.FeatureSnapshotReadyEvent {
	log.Trace("featureSnapshotReadyEventListener NewMessage")

	return &featurepb.FeatureSnapshotReadyEvent{}
}

func (l *featureSnapshotReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.FeatureSnapshotReadyEvent) error {
	log.Trace("featureSnapshotReadyEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("feature snapshot ready listener is nil"))
	}
	dataset, err := featureSnapshotReadyToDataset(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.listener.RecordDatasetMaterialization(ctx, dataset, model.DatasetProcessingFeatureMaterialized)
	return err
}

func featureSnapshotReadyToDataset(resourceKey uuid.UUID, payload *featurepb.FeatureSnapshotReadyEvent) (*model.Dataset, error) {
	log.Trace("featureSnapshotReadyToDataset")

	if resourceKey == uuid.Nil {
		return nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, fmt.Errorf("feature snapshot ready payload is required")
	}

	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, err
	}
	if datasetID != resourceKey {
		return nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, err
	}
	location := strings.TrimSpace(payload.GetStorageLocation())
	if location == "" {
		return nil, fmt.Errorf("storage location is required")
	}
	tableNamespace := strings.TrimSpace(payload.GetTableNamespace())
	if tableNamespace == "" {
		return nil, fmt.Errorf("table namespace is required")
	}
	tableName := strings.TrimSpace(payload.GetTableName())
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}
	tableFormat, err := model.ToTableFormat(strings.TrimSpace(payload.GetTableFormat()))
	if err != nil {
		return nil, fmt.Errorf("table format is invalid: %w", err)
	}
	catalogProvider, err := model.ToCatalogProvider(strings.TrimSpace(payload.GetCatalogProvider()))
	if err != nil {
		return nil, fmt.Errorf("catalog provider is invalid: %w", err)
	}
	schemaVersion := int(payload.GetSchemaVersion())
	if schemaVersion <= 0 {
		schemaVersion = 1
	}
	schemaMetadata := strings.TrimSpace(payload.GetSchemaMetadata())
	if schemaMetadata == "" {
		schemaMetadata = "{}"
	}

	return &model.Dataset{
		ID:              datasetID,
		UserID:          userID,
		Location:        location,
		TableNamespace:  tableNamespace,
		TableName:       tableName,
		TableFormat:     tableFormat,
		CatalogProvider: catalogProvider,
		SchemaVersion:   schemaVersion,
		SchemaMetadata:  schemaMetadata,
	}, nil
}
