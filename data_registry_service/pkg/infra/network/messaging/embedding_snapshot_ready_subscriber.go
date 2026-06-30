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

type EmbeddingSnapshotReadyListener interface {
	AdvanceDatasetProcessingState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, error)
}

type embeddingSnapshotReadyEventListener struct {
	listener EmbeddingSnapshotReadyListener
}

func NewEmbeddingSnapshotReadyEventListener(listener EmbeddingSnapshotReadyListener) *embeddingSnapshotReadyEventListener {
	log.Trace("NewEmbeddingSnapshotReadyEventListener")

	return &embeddingSnapshotReadyEventListener{
		listener: listener,
	}
}

func (l *embeddingSnapshotReadyEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeEmbeddingSnapshotReady
}

func (l *embeddingSnapshotReadyEventListener) NewMessage() *featurepb.EmbeddingSnapshotReadyEvent {
	return &featurepb.EmbeddingSnapshotReadyEvent{}
}

func (l *embeddingSnapshotReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.EmbeddingSnapshotReadyEvent) error {
	log.Trace("embeddingSnapshotReadyEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("embedding snapshot ready listener is nil"))
	}
	datasetID, userID, err := embeddingSnapshotReadyToIDs(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.listener.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingEmbeddingsMaterialized)
	return err
}

func embeddingSnapshotReadyToIDs(resourceKey uuid.UUID, payload *featurepb.EmbeddingSnapshotReadyEvent) (uuid.UUID, uuid.UUID, error) {
	if resourceKey == uuid.Nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("embedding snapshot ready payload is required")
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
	if strings.TrimSpace(payload.GetVectorStore()) == "" {
		return uuid.Nil, uuid.Nil, fmt.Errorf("vector store is required")
	}
	if strings.TrimSpace(payload.GetCollectionName()) == "" {
		return uuid.Nil, uuid.Nil, fmt.Errorf("collection name is required")
	}
	return datasetID, userID, nil
}
