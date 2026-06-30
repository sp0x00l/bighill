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

type RawSnapshotReadyListener interface {
	AdvanceDatasetProcessingState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, error)
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
	return msgConn.MsgTypeRawSnapshotReady
}

func (l *rawSnapshotReadyEventListener) NewMessage() *featurepb.RawSnapshotReadyEvent {
	return &featurepb.RawSnapshotReadyEvent{}
}

func (l *rawSnapshotReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.RawSnapshotReadyEvent) error {
	log.Trace("rawSnapshotReadyEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("raw snapshot ready listener is nil"))
	}
	datasetID, userID, err := rawSnapshotReadyToIDs(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.listener.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)
	return err
}

func rawSnapshotReadyToIDs(resourceKey uuid.UUID, payload *featurepb.RawSnapshotReadyEvent) (uuid.UUID, uuid.UUID, error) {
	if resourceKey == uuid.Nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("raw snapshot ready payload is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return uuid.Nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if strings.TrimSpace(payload.GetStorageLocation()) == "" {
		return uuid.Nil, uuid.Nil, fmt.Errorf("storage location is required")
	}
	return datasetID, userID, nil
}
