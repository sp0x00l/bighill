package messaging

import (
	"fmt"

	"feature_materializer_service/pkg/domain/model"

	featurepb "lib/data_contracts_lib/feature_materializer"
	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type snapshotEventBuilder struct {
	topics MaterializationTopics
}

func NewSnapshotEventBuilder(topics MaterializationTopics) *snapshotEventBuilder {
	log.Trace("NewSnapshotEventBuilder")

	return &snapshotEventBuilder{topics: topics}
}

func (b *snapshotEventBuilder) RawSnapshotReadyMessage(rawSnapshot *model.RawSnapshot) (msgConn.OutboundMessage, error) {
	log.Trace("snapshotEventBuilder RawSnapshotReadyMessage")

	if rawSnapshot == nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("raw snapshot is required")
	}
	if rawSnapshot.MaterializationEventSeq <= 0 {
		return msgConn.OutboundMessage{}, fmt.Errorf("raw snapshot materialization event sequence is required")
	}
	payload, err := marshalSnapshotEvent(&featurepb.RawSnapshotReadyEvent{
		DatasetId:               rawSnapshot.DatasetID.String(),
		MaterializationEventSeq: rawSnapshot.MaterializationEventSeq,
		RawSnapshotId:           rawSnapshot.RawSnapshotID.String(),
		UserId:                  rawSnapshot.UserID.String(),
		OrgId:                   rawSnapshot.OrgID.String(),
		StorageLocation:         rawSnapshot.StorageLocation,
		TableNamespace:          rawSnapshot.TableNamespace,
		TableName:               rawSnapshot.TableName,
		TableFormat:             rawSnapshot.TableFormat,
		CatalogProvider:         rawSnapshot.CatalogProvider,
		SchemaVersion:           int32(rawSnapshot.SchemaVersion),
		SchemaMetadata:          rawSnapshot.SchemaMetadata,
		ProcessingProfile:       rawSnapshot.ProcessingProfile.String(),
	})
	if err != nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("marshal raw snapshot ready event: %w", err)
	}
	return msgConn.OutboundMessage{
		Topic: b.topics.FeatureMaterializer,
		Message: msgConn.Message{
			ResourceKey: rawSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeRawSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "raw_snapshot_ready:" + rawSnapshot.RawSnapshotID.String(),
	}, nil
}

func (b *snapshotEventBuilder) FeatureSnapshotReadyMessage(featureSnapshot *model.FeatureSnapshot) (msgConn.OutboundMessage, error) {
	log.Trace("snapshotEventBuilder FeatureSnapshotReadyMessage")

	if featureSnapshot == nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("feature snapshot is required")
	}
	if featureSnapshot.MaterializationEventSeq <= 0 {
		return msgConn.OutboundMessage{}, fmt.Errorf("feature snapshot materialization event sequence is required")
	}
	payload, err := marshalSnapshotEvent(&featurepb.FeatureSnapshotReadyEvent{
		DatasetId:               featureSnapshot.DatasetID.String(),
		MaterializationEventSeq: featureSnapshot.MaterializationEventSeq,
		RawSnapshotId:           featureSnapshot.RawSnapshotID.String(),
		FeatureSnapshotId:       featureSnapshot.FeatureSnapshotID.String(),
		UserId:                  featureSnapshot.UserID.String(),
		OrgId:                   featureSnapshot.OrgID.String(),
		StorageLocation:         featureSnapshot.StorageLocation,
		TableNamespace:          featureSnapshot.TableNamespace,
		TableName:               featureSnapshot.TableName,
		TableFormat:             featureSnapshot.TableFormat,
		CatalogProvider:         featureSnapshot.CatalogProvider,
		SchemaVersion:           int32(featureSnapshot.SchemaVersion),
		SchemaMetadata:          featureSnapshot.SchemaMetadata,
		ProcessingProfile:       featureSnapshot.ProcessingProfile.String(),
	})
	if err != nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("marshal feature snapshot ready event: %w", err)
	}
	return msgConn.OutboundMessage{
		Topic: b.topics.FeatureMaterializer,
		Message: msgConn.Message{
			ResourceKey: featureSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeFeatureSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "feature_snapshot_ready:" + featureSnapshot.FeatureSnapshotID.String(),
	}, nil
}

func (b *snapshotEventBuilder) EmbeddingSnapshotReadyMessage(embeddingSnapshot *model.EmbeddingSnapshot) (msgConn.OutboundMessage, error) {
	log.Trace("snapshotEventBuilder EmbeddingSnapshotReadyMessage")

	if embeddingSnapshot == nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("embedding snapshot is required")
	}
	if embeddingSnapshot.MaterializationEventSeq <= 0 {
		return msgConn.OutboundMessage{}, fmt.Errorf("embedding snapshot materialization event sequence is required")
	}
	payload, err := marshalSnapshotEvent(&featurepb.EmbeddingSnapshotReadyEvent{
		DatasetId:               embeddingSnapshot.DatasetID.String(),
		MaterializationEventSeq: embeddingSnapshot.MaterializationEventSeq,
		FeatureSnapshotId:       embeddingSnapshot.FeatureSnapshotID.String(),
		EmbeddingSnapshotId:     embeddingSnapshot.EmbeddingSnapshotID.String(),
		UserId:                  embeddingSnapshot.UserID.String(),
		OrgId:                   embeddingSnapshot.OrgID.String(),
		VectorStore:             embeddingSnapshot.VectorStore,
		CollectionName:          embeddingSnapshot.CollectionName,
		EmbeddingDimensions:     int32(embeddingSnapshot.EmbeddingDimensions),
		EmbeddingCount:          embeddingSnapshot.EmbeddingCount,
		StrategyVersion:         embeddingSnapshot.StrategyVersion,
		ChunkerName:             embeddingSnapshot.ChunkerName,
		ChunkerVersion:          embeddingSnapshot.ChunkerVersion,
		ChunkSize:               int32(embeddingSnapshot.ChunkSize),
		ChunkOverlap:            int32(embeddingSnapshot.ChunkOverlap),
		EmbeddingProvider:       embeddingSnapshot.EmbeddingProvider,
		EmbeddingModel:          embeddingSnapshot.EmbeddingModel,
	})
	if err != nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("marshal embedding snapshot ready event: %w", err)
	}
	return msgConn.OutboundMessage{
		Topic: b.topics.FeatureMaterializer,
		Message: msgConn.Message{
			ResourceKey: embeddingSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeEmbeddingSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "embedding_snapshot_ready:" + embeddingSnapshot.EmbeddingSnapshotID.String(),
	}, nil
}

func (b *snapshotEventBuilder) GraphSnapshotReadyMessage(graphSnapshot *model.GraphSnapshot) (msgConn.OutboundMessage, error) {
	log.Trace("snapshotEventBuilder GraphSnapshotReadyMessage")

	if graphSnapshot == nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("graph snapshot is required")
	}
	if graphSnapshot.MaterializationEventSeq <= 0 {
		return msgConn.OutboundMessage{}, fmt.Errorf("graph snapshot materialization event sequence is required")
	}
	payload, err := marshalSnapshotEvent(&featurepb.GraphSnapshotReadyEvent{
		DatasetId:               graphSnapshot.DatasetID.String(),
		MaterializationEventSeq: graphSnapshot.MaterializationEventSeq,
		FeatureSnapshotId:       graphSnapshot.FeatureSnapshotID.String(),
		EmbeddingSnapshotId:     graphSnapshot.EmbeddingSnapshotID.String(),
		GraphSnapshotId:         graphSnapshot.GraphSnapshotID.String(),
		UserId:                  graphSnapshot.UserID.String(),
		OrgId:                   graphSnapshot.OrgID.String(),
		ProvenanceHash:          graphSnapshot.ProvenanceHash,
		ExtractionModel:         graphSnapshot.ExtractionModel,
		ExtractionPromptVersion: graphSnapshot.ExtractionPromptVersion,
		ExtractionSchemaVersion: graphSnapshot.ExtractionSchemaVersion,
		ChunkCount:              graphSnapshot.ChunkCount,
		ChunksProcessed:         graphSnapshot.ChunksProcessed,
		EntityCount:             graphSnapshot.EntityCount,
		EdgeCount:               graphSnapshot.EdgeCount,
	})
	if err != nil {
		return msgConn.OutboundMessage{}, fmt.Errorf("marshal graph snapshot ready event: %w", err)
	}
	return msgConn.OutboundMessage{
		Topic: b.topics.FeatureMaterializer,
		Message: msgConn.Message{
			ResourceKey: graphSnapshot.DatasetID,
			MsgType:     msgConn.MsgTypeGraphSnapshotReady,
			Payload:     payload,
		},
		DispatchKey: "graph_snapshot_ready:" + graphSnapshot.GraphSnapshotID.String(),
	}, nil
}

func marshalSnapshotEvent(payload proto.Message) ([]byte, error) {
	log.Trace("marshalSnapshotEvent")

	out, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return out, nil
}
