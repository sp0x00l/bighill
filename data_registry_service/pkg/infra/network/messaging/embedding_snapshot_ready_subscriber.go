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
	RecordDatasetMaterialization(ctx context.Context, dataset *model.Dataset, state model.ProcessingState) (*model.Dataset, error)
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
	log.Trace("embeddingSnapshotReadyEventListener MsgType")

	return msgConn.MsgTypeEmbeddingSnapshotReady
}

func (l *embeddingSnapshotReadyEventListener) NewMessage() *featurepb.EmbeddingSnapshotReadyEvent {
	log.Trace("embeddingSnapshotReadyEventListener NewMessage")

	return &featurepb.EmbeddingSnapshotReadyEvent{}
}

func (l *embeddingSnapshotReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.EmbeddingSnapshotReadyEvent) error {
	log.Trace("embeddingSnapshotReadyEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("embedding snapshot ready listener is nil"))
	}
	dataset, err := embeddingSnapshotReadyToDataset(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.listener.RecordDatasetMaterialization(ctx, dataset, model.DatasetProcessingEmbeddingsMaterialized)
	return err
}

func embeddingSnapshotReadyToDataset(resourceKey uuid.UUID, payload *featurepb.EmbeddingSnapshotReadyEvent) (*model.Dataset, error) {
	log.Trace("embeddingSnapshotReadyToDataset")

	if resourceKey == uuid.Nil {
		return nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, fmt.Errorf("embedding snapshot ready payload is required")
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
	embeddingSnapshotID, err := msgConn.ParseUUID("embedding_snapshot_id", payload.GetEmbeddingSnapshotId())
	if err != nil {
		return nil, err
	}
	featureSnapshotID, err := msgConn.ParseUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return nil, err
	}
	vectorStore := strings.TrimSpace(payload.GetVectorStore())
	if vectorStore == "" {
		return nil, fmt.Errorf("vector store is required")
	}
	collectionName := strings.TrimSpace(payload.GetCollectionName())
	if collectionName == "" {
		return nil, fmt.Errorf("collection name is required")
	}
	return &model.Dataset{
		ID:                       datasetID,
		UserID:                   userID,
		OrgID:                    orgID,
		FeatureSnapshotID:        featureSnapshotID,
		EmbeddingSnapshotID:      embeddingSnapshotID,
		VectorStore:              vectorStore,
		CollectionName:           collectionName,
		EmbeddingDimensions:      int(payload.GetEmbeddingDimensions()),
		EmbeddingCount:           payload.GetEmbeddingCount(),
		EmbeddingStrategyVersion: strings.TrimSpace(payload.GetStrategyVersion()),
		EmbeddingChunkerName:     strings.TrimSpace(payload.GetChunkerName()),
		EmbeddingChunkerVersion:  strings.TrimSpace(payload.GetChunkerVersion()),
		EmbeddingChunkSize:       int(payload.GetChunkSize()),
		EmbeddingChunkOverlap:    int(payload.GetChunkOverlap()),
		EmbeddingProvider:        strings.TrimSpace(payload.GetEmbeddingProvider()),
		EmbeddingModel:           strings.TrimSpace(payload.GetEmbeddingModel()),
	}, nil
}
