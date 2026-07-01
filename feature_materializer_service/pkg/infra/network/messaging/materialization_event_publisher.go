package messaging

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type MaterializationEventPublisher interface {
	PublishRawSnapshotReady(ctx context.Context, rawSnapshot *model.RawSnapshot) error
	PublishFeatureSnapshotReady(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error
	PublishEmbeddingSnapshotReady(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot) error
}

type MaterializationTopics struct {
	FeatureMaterializer string
}

type materializationEventPublisher struct {
	publisher msgConn.Publisher
	topics    MaterializationTopics
}

func NewMaterializationEventPublisher(publisher msgConn.Publisher, topics MaterializationTopics) MaterializationEventPublisher {
	log.Trace("NewMaterializationEventPublisher")

	return &materializationEventPublisher{
		publisher: publisher,
		topics:    topics,
	}
}

func (p *materializationEventPublisher) PublishRawSnapshotReady(ctx context.Context, rawSnapshot *model.RawSnapshot) error {
	log.Trace("materializationEventPublisher PublishRawSnapshotReady")

	if rawSnapshot == nil {
		return msgConn.NonRetryable(fmt.Errorf("raw snapshot is required"))
	}
	return p.publish(ctx, rawSnapshot.DatasetID, msgConn.MsgTypeRawSnapshotReady, &featurepb.RawSnapshotReadyEvent{
		RawSnapshotId:     rawSnapshot.RawSnapshotID.String(),
		DatasetId:         rawSnapshot.DatasetID.String(),
		UserId:            rawSnapshot.UserID.String(),
		StorageLocation:   rawSnapshot.StorageLocation,
		TableNamespace:    rawSnapshot.TableNamespace,
		TableName:         rawSnapshot.TableName,
		TableFormat:       rawSnapshot.TableFormat,
		CatalogProvider:   rawSnapshot.CatalogProvider,
		SchemaVersion:     int32(rawSnapshot.SchemaVersion),
		SchemaMetadata:    rawSnapshot.SchemaMetadata,
		ProcessingProfile: rawSnapshot.ProcessingProfile.String(),
	})
}

func (p *materializationEventPublisher) PublishFeatureSnapshotReady(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error {
	log.Trace("materializationEventPublisher PublishFeatureSnapshotReady")

	if featureSnapshot == nil {
		return msgConn.NonRetryable(fmt.Errorf("feature snapshot is required"))
	}
	return p.publish(ctx, featureSnapshot.DatasetID, msgConn.MsgTypeFeatureSnapshotReady, &featurepb.FeatureSnapshotReadyEvent{
		FeatureSnapshotId: featureSnapshot.FeatureSnapshotID.String(),
		RawSnapshotId:     featureSnapshot.RawSnapshotID.String(),
		DatasetId:         featureSnapshot.DatasetID.String(),
		UserId:            featureSnapshot.UserID.String(),
		StorageLocation:   featureSnapshot.StorageLocation,
		TableNamespace:    featureSnapshot.TableNamespace,
		TableName:         featureSnapshot.TableName,
		TableFormat:       featureSnapshot.TableFormat,
		CatalogProvider:   featureSnapshot.CatalogProvider,
		SchemaVersion:     int32(featureSnapshot.SchemaVersion),
		SchemaMetadata:    featureSnapshot.SchemaMetadata,
		ProcessingProfile: featureSnapshot.ProcessingProfile.String(),
	})
}

func (p *materializationEventPublisher) PublishEmbeddingSnapshotReady(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot) error {
	log.Trace("materializationEventPublisher PublishEmbeddingSnapshotReady")

	if embeddingSnapshot == nil {
		return msgConn.NonRetryable(fmt.Errorf("embedding snapshot is required"))
	}
	return p.publish(ctx, embeddingSnapshot.DatasetID, msgConn.MsgTypeEmbeddingSnapshotReady, &featurepb.EmbeddingSnapshotReadyEvent{
		EmbeddingSnapshotId: embeddingSnapshot.EmbeddingSnapshotID.String(),
		FeatureSnapshotId:   embeddingSnapshot.FeatureSnapshotID.String(),
		DatasetId:           embeddingSnapshot.DatasetID.String(),
		UserId:              embeddingSnapshot.UserID.String(),
		VectorStore:         embeddingSnapshot.VectorStore,
		CollectionName:      embeddingSnapshot.CollectionName,
		EmbeddingDimensions: int32(embeddingSnapshot.EmbeddingDimensions),
		EmbeddingCount:      embeddingSnapshot.EmbeddingCount,
	})
}

func (p *materializationEventPublisher) publish(ctx context.Context, resourceKey uuid.UUID, msgType msgConn.MsgType, payload proto.Message) error {
	log.Trace("materializationEventPublisher publish")

	if p == nil || p.publisher == nil {
		return nil
	}
	topic := p.topics.FeatureMaterializer
	if topic == "" {
		return msgConn.NonRetryable(fmt.Errorf("topic is required for %s", msgType.String()))
	}
	return p.publisher.Publish(ctx, topic, msgConn.Message{
		ResourceKey: resourceKey,
		MsgType:     msgType,
	}, payload)
}
