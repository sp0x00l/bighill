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

type GraphSnapshotReadyListener interface {
	RecordDatasetMaterialization(ctx context.Context, dataset *model.Dataset, state model.ProcessingState, eventSeq int64) (*model.Dataset, error)
}

type graphSnapshotReadyEventListener struct {
	listener GraphSnapshotReadyListener
}

func NewGraphSnapshotReadyEventListener(listener GraphSnapshotReadyListener) *graphSnapshotReadyEventListener {
	log.Trace("NewGraphSnapshotReadyEventListener")

	return &graphSnapshotReadyEventListener{
		listener: listener,
	}
}

func (l *graphSnapshotReadyEventListener) MsgType() msgConn.MsgType {
	log.Trace("graphSnapshotReadyEventListener MsgType")

	return msgConn.MsgTypeGraphSnapshotReady
}

func (l *graphSnapshotReadyEventListener) NewMessage() *featurepb.GraphSnapshotReadyEvent {
	log.Trace("graphSnapshotReadyEventListener NewMessage")

	return &featurepb.GraphSnapshotReadyEvent{}
}

func (l *graphSnapshotReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.GraphSnapshotReadyEvent) error {
	log.Trace("graphSnapshotReadyEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("graph snapshot ready listener is nil"))
	}
	dataset, err := graphSnapshotReadyToDataset(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	ctx = ctxutil.WithActorOrg(ctx, dataset.UserID, dataset.OrgID)
	_, err = l.listener.RecordDatasetMaterialization(ctx, dataset, model.DatasetProcessingGraphMaterialized, payload.GetMaterializationEventSeq())
	return err
}

func graphSnapshotReadyToDataset(resourceKey uuid.UUID, payload *featurepb.GraphSnapshotReadyEvent) (*model.Dataset, error) {
	log.Trace("graphSnapshotReadyToDataset")

	if resourceKey == uuid.Nil {
		return nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, fmt.Errorf("graph snapshot ready payload is required")
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
	featureSnapshotID, err := msgConn.ParseUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return nil, err
	}
	embeddingSnapshotID, err := msgConn.ParseUUID("embedding_snapshot_id", payload.GetEmbeddingSnapshotId())
	if err != nil {
		return nil, err
	}
	graphSnapshotID, err := msgConn.ParseUUID("graph_snapshot_id", payload.GetGraphSnapshotId())
	if err != nil {
		return nil, err
	}
	provenanceHash := strings.TrimSpace(payload.GetProvenanceHash())
	if provenanceHash == "" {
		return nil, fmt.Errorf("graph provenance hash is required")
	}
	if payload.GetChunkCount() <= 0 {
		return nil, fmt.Errorf("graph chunk count is required")
	}
	if payload.GetChunksProcessed() != payload.GetChunkCount() {
		return nil, fmt.Errorf("graph chunks processed must equal chunk count")
	}
	if strings.TrimSpace(payload.GetExtractionModel()) == "" {
		return nil, fmt.Errorf("graph extraction model is required")
	}
	if strings.TrimSpace(payload.GetExtractionPromptVersion()) == "" {
		return nil, fmt.Errorf("graph extraction prompt version is required")
	}
	if strings.TrimSpace(payload.GetExtractionSchemaVersion()) == "" {
		return nil, fmt.Errorf("graph extraction schema version is required")
	}
	return &model.Dataset{
		ID:                  datasetID,
		UserID:              userID,
		OrgID:               orgID,
		FeatureSnapshotID:   featureSnapshotID,
		EmbeddingSnapshotID: embeddingSnapshotID,
		GraphSnapshotID:     graphSnapshotID,
		GraphProvenanceHash: provenanceHash,
		GraphNodeCount:      payload.GetEntityCount(),
		GraphEdgeCount:      payload.GetEdgeCount(),
	}, nil
}
