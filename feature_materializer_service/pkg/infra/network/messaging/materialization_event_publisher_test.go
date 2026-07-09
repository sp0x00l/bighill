package messaging_test

import (
	"context"

	"feature_materializer_service/pkg/domain/model"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	featurepb "lib/data_contracts_lib/feature_materializer"
	sharedMessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type recordingSharedPublisher struct {
	topic   string
	message sharedMessaging.Message
	payload proto.Message
}

func (p *recordingSharedPublisher) Publish(_ context.Context, topic string, message sharedMessaging.Message, payload proto.Message) error {
	p.topic = topic
	p.message = message
	p.payload = payload
	return nil
}

func (p *recordingSharedPublisher) Close() {}

var _ = Describe("MaterializationEventPublisher", func() {
	It("publishes raw snapshot ready facts through the shared publisher", func() {
		datasetID := uuid.New()
		sharedPublisher := &recordingSharedPublisher{}
		publisher := featuremessaging.NewMaterializationEventPublisher(sharedPublisher, featuremessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})
		rawSnapshot := validMessagingRawSnapshot(datasetID)

		err := publisher.PublishRawSnapshotReady(context.Background(), rawSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(sharedPublisher.topic).To(Equal("feature_materializer"))
		Expect(sharedPublisher.message.ResourceKey).To(Equal(datasetID))
		Expect(sharedPublisher.message.MsgType).To(Equal(sharedMessaging.MsgTypeRawSnapshotReady))
		event, ok := sharedPublisher.payload.(*featurepb.RawSnapshotReadyEvent)
		Expect(ok).To(BeTrue())
		Expect(event.MaterializationEventSeq).To(Equal(rawSnapshot.MaterializationEventSeq))
		Expect(event.OrgId).To(Equal(rawSnapshot.OrgID.String()))
		Expect(event.ProcessingProfile).To(Equal(model.ProcessingProfileTextRAG.String()))
	})

	It("publishes feature snapshot ready facts through the shared publisher", func() {
		datasetID := uuid.New()
		sharedPublisher := &recordingSharedPublisher{}
		publisher := featuremessaging.NewMaterializationEventPublisher(sharedPublisher, featuremessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})
		featureSnapshot := validMessagingFeatureSnapshot(uuid.New())
		featureSnapshot.DatasetID = datasetID

		err := publisher.PublishFeatureSnapshotReady(context.Background(), featureSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(sharedPublisher.message.ResourceKey).To(Equal(datasetID))
		Expect(sharedPublisher.message.MsgType).To(Equal(sharedMessaging.MsgTypeFeatureSnapshotReady))
		event, ok := sharedPublisher.payload.(*featurepb.FeatureSnapshotReadyEvent)
		Expect(ok).To(BeTrue())
		Expect(event.MaterializationEventSeq).To(Equal(featureSnapshot.MaterializationEventSeq))
		Expect(event.OrgId).To(Equal(featureSnapshot.OrgID.String()))
		Expect(event.ProcessingProfile).To(Equal(model.ProcessingProfileTextRAG.String()))
	})

	It("publishes embedding snapshot ready facts through the shared publisher", func() {
		datasetID := uuid.New()
		sharedPublisher := &recordingSharedPublisher{}
		publisher := featuremessaging.NewMaterializationEventPublisher(sharedPublisher, featuremessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})
		embeddingSnapshot := validMessagingEmbeddingSnapshot(uuid.New())
		embeddingSnapshot.DatasetID = datasetID

		err := publisher.PublishEmbeddingSnapshotReady(context.Background(), embeddingSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(sharedPublisher.message.ResourceKey).To(Equal(datasetID))
		Expect(sharedPublisher.message.MsgType).To(Equal(sharedMessaging.MsgTypeEmbeddingSnapshotReady))
		event, ok := sharedPublisher.payload.(*featurepb.EmbeddingSnapshotReadyEvent)
		Expect(ok).To(BeTrue())
		Expect(event.MaterializationEventSeq).To(Equal(embeddingSnapshot.MaterializationEventSeq))
		Expect(event.OrgId).To(Equal(embeddingSnapshot.OrgID.String()))
	})

	It("rejects ready facts without an assigned materialization event sequence", func() {
		sharedPublisher := &recordingSharedPublisher{}
		publisher := featuremessaging.NewMaterializationEventPublisher(sharedPublisher, featuremessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})
		rawSnapshot := validMessagingRawSnapshot(uuid.New())
		rawSnapshot.MaterializationEventSeq = 0

		err := publisher.PublishRawSnapshotReady(context.Background(), rawSnapshot)

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
		Expect(sharedPublisher.payload).To(BeNil())
	})
})

func validMessagingRawSnapshot(datasetID uuid.UUID) *model.RawSnapshot {
	return &model.RawSnapshot{
		RawSnapshotID:           uuid.New(),
		DatasetID:               datasetID,
		UserID:                  uuid.New(),
		OrgID:                   uuid.New(),
		MaterializationEventSeq: 11,
		StorageLocation:         "s3://local-dev-bucket/lakehouse/raw/data.parquet",
		ContentType:             "text/csv",
		FileExtension:           "csv",
		TableNamespace:          "features",
		TableName:               "movies",
		TableFormat:             "PARQUET",
		CatalogProvider:         "LOCAL",
		ProcessingProfile:       model.ProcessingProfileTextRAG,
		SchemaVersion:           1,
		SchemaMetadata:          "{}",
		Status:                  model.SnapshotStatusReady,
	}
}

func validMessagingFeatureSnapshot(rawSnapshotID uuid.UUID) *model.FeatureSnapshot {
	return &model.FeatureSnapshot{
		FeatureSnapshotID:       uuid.New(),
		RawSnapshotID:           rawSnapshotID,
		DatasetID:               uuid.New(),
		UserID:                  uuid.New(),
		OrgID:                   uuid.New(),
		MaterializationEventSeq: 12,
		StorageLocation:         "s3://local-dev-bucket/lakehouse/features/data.parquet",
		TableNamespace:          "features",
		TableName:               "movies",
		TableFormat:             "PARQUET",
		CatalogProvider:         "LOCAL",
		ProcessingProfile:       model.ProcessingProfileTextRAG,
		SchemaVersion:           1,
		SchemaMetadata:          "{}",
		Status:                  model.SnapshotStatusReady,
	}
}

func validMessagingEmbeddingSnapshot(featureSnapshotID uuid.UUID) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID:     uuid.New(),
		FeatureSnapshotID:       featureSnapshotID,
		DatasetID:               uuid.New(),
		UserID:                  uuid.New(),
		OrgID:                   uuid.New(),
		MaterializationEventSeq: 13,
		VectorStore:             "pgvector",
		CollectionName:          "movies",
		EmbeddingDimensions:     384,
		EmbeddingCount:          2,
		StrategyVersion:         "rag-v1",
		ChunkerName:             "go-token-window",
		ChunkerVersion:          "v1",
		ChunkSize:               384,
		ChunkOverlap:            64,
		EmbeddingProvider:       "ollama",
		EmbeddingModel:          model.DefaultEmbeddingModel,
		Status:                  model.SnapshotStatusReady,
	}
}
