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

		err := publisher.PublishRawSnapshotReady(context.Background(), validMessagingRawSnapshot(datasetID))

		Expect(err).NotTo(HaveOccurred())
		Expect(sharedPublisher.topic).To(Equal("feature_materializer"))
		Expect(sharedPublisher.message.ResourceKey).To(Equal(datasetID))
		Expect(sharedPublisher.message.MsgType).To(Equal(sharedMessaging.MsgTypeRawSnapshotReady))
		Expect(sharedPublisher.payload).To(BeAssignableToTypeOf(&featurepb.RawSnapshotReadyEvent{}))
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
		Expect(sharedPublisher.payload).To(BeAssignableToTypeOf(&featurepb.FeatureSnapshotReadyEvent{}))
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
		Expect(sharedPublisher.payload).To(BeAssignableToTypeOf(&featurepb.EmbeddingSnapshotReadyEvent{}))
	})
})

func validMessagingRawSnapshot(datasetID uuid.UUID) *model.RawSnapshot {
	return &model.RawSnapshot{
		RawSnapshotID:   uuid.New(),
		DatasetID:       datasetID,
		UserID:          uuid.New(),
		StorageLocation: "s3://local-dev-bucket/lakehouse/raw/data.parquet",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "features",
		TableName:       "movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
		SchemaVersion:   1,
		SchemaMetadata:  "{}",
		Status:          model.SnapshotStatusReady,
	}
}

func validMessagingFeatureSnapshot(rawSnapshotID uuid.UUID) *model.FeatureSnapshot {
	return &model.FeatureSnapshot{
		FeatureSnapshotID: uuid.New(),
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         uuid.New(),
		UserID:            uuid.New(),
		StorageLocation:   "s3://local-dev-bucket/lakehouse/features/data.parquet",
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		SchemaVersion:     1,
		SchemaMetadata:    "{}",
		Status:            model.SnapshotStatusReady,
	}
}

func validMessagingEmbeddingSnapshot(featureSnapshotID uuid.UUID) *model.EmbeddingSnapshot {
	return &model.EmbeddingSnapshot{
		EmbeddingSnapshotID: uuid.New(),
		FeatureSnapshotID:   featureSnapshotID,
		DatasetID:           uuid.New(),
		UserID:              uuid.New(),
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		EmbeddingDimensions: 384,
		EmbeddingCount:      2,
		Status:              model.SnapshotStatusReady,
	}
}
