package messaging

import (
	"context"
	"fmt"
	"strings"

	"data_registry_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type RawSnapshotReadyListener interface {
	RecordDatasetMaterialization(ctx context.Context, dataset *model.Dataset, state model.ProcessingState, eventSeq int64) (*model.Dataset, error)
}

type rawSnapshotReadyEventListener struct {
	listener RawSnapshotReadyListener
}

func NewRawSnapshotReadyEventListener(listener RawSnapshotReadyListener) *rawSnapshotReadyEventListener {
	log.Trace("NewRawSnapshotReadyEventListener")

	return &rawSnapshotReadyEventListener{
		listener: listener,
	}
}

func (l *rawSnapshotReadyEventListener) MsgType() msgConn.MsgType {
	log.Trace("rawSnapshotReadyEventListener MsgType")

	return msgConn.MsgTypeRawSnapshotReady
}

func (l *rawSnapshotReadyEventListener) NewMessage() *featurepb.RawSnapshotReadyEvent {
	log.Trace("rawSnapshotReadyEventListener NewMessage")

	return &featurepb.RawSnapshotReadyEvent{}
}

func (l *rawSnapshotReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.RawSnapshotReadyEvent) error {
	log.Trace("rawSnapshotReadyEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("raw snapshot ready listener is nil"))
	}
	dataset, err := rawSnapshotReadyToDataset(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	ctx = ctxutil.WithActorOrg(ctx, dataset.UserID, dataset.OrgID)
	_, err = l.listener.RecordDatasetMaterialization(ctx, dataset, model.DatasetProcessingRawMaterialized, payload.GetMaterializationEventSeq())
	return err
}

func rawSnapshotReadyToDataset(resourceKey uuid.UUID, payload *featurepb.RawSnapshotReadyEvent) (*model.Dataset, error) {
	log.Trace("rawSnapshotReadyToDataset")

	if resourceKey == uuid.Nil {
		return nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, fmt.Errorf("raw snapshot ready payload is required")
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
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return nil, err
	}
	rawSnapshotID, err := msgConn.ParseUUID("raw_snapshot_id", payload.GetRawSnapshotId())
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
	processingProfile, err := model.ToProcessingProfile(strings.TrimSpace(payload.GetProcessingProfile()))
	if err != nil {
		return nil, fmt.Errorf("processing profile is invalid: %w", err)
	}
	schemaVersion := int(payload.GetSchemaVersion())
	if schemaVersion <= 0 {
		return nil, fmt.Errorf("schema version is required")
	}
	schemaMetadata := strings.TrimSpace(payload.GetSchemaMetadata())
	if schemaMetadata == "" {
		return nil, fmt.Errorf("schema metadata is required")
	}
	return &model.Dataset{
		ID:                datasetID,
		UserID:            userID,
		OrgID:             orgID,
		Location:          location,
		TableNamespace:    tableNamespace,
		TableName:         tableName,
		TableFormat:       tableFormat,
		CatalogProvider:   catalogProvider,
		ProcessingProfile: processingProfile,
		SchemaVersion:     schemaVersion,
		SchemaMetadata:    schemaMetadata,
		RawSnapshotID:     rawSnapshotID,
	}, nil
}
