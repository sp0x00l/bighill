package messaging_test

import (
	"feature_materializer_service/pkg/infra/network/messaging"
	featurepb "lib/data_contracts_lib/feature_materializer"
	sharedMessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("SnapshotEventBuilder", func() {
	It("builds raw snapshot ready outbound messages", func() {
		datasetID := uuid.New()
		rawSnapshot := validMessagingRawSnapshot(datasetID)
		builder := messaging.NewSnapshotEventBuilder(messaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})

		message, err := builder.RawSnapshotReadyMessage(rawSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(message.Topic).To(Equal("feature_materializer"))
		Expect(message.DispatchKey).To(Equal("raw_snapshot_ready:" + rawSnapshot.RawSnapshotID.String()))
		Expect(message.Message.ResourceKey).To(Equal(datasetID))
		Expect(message.Message.MsgType).To(Equal(sharedMessaging.MsgTypeRawSnapshotReady))

		var event featurepb.RawSnapshotReadyEvent
		Expect(proto.Unmarshal(message.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.RawSnapshotId).To(Equal(rawSnapshot.RawSnapshotID.String()))
		Expect(event.MaterializationEventSeq).To(Equal(rawSnapshot.MaterializationEventSeq))
		Expect(event.ProcessingProfile).To(Equal(rawSnapshot.ProcessingProfile.String()))
	})

	It("builds feature snapshot ready outbound messages", func() {
		rawSnapshotID := uuid.New()
		featureSnapshot := validMessagingFeatureSnapshot(rawSnapshotID)
		builder := messaging.NewSnapshotEventBuilder(messaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})

		message, err := builder.FeatureSnapshotReadyMessage(featureSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(message.Topic).To(Equal("feature_materializer"))
		Expect(message.DispatchKey).To(Equal("feature_snapshot_ready:" + featureSnapshot.FeatureSnapshotID.String()))
		Expect(message.Message.ResourceKey).To(Equal(featureSnapshot.DatasetID))
		Expect(message.Message.MsgType).To(Equal(sharedMessaging.MsgTypeFeatureSnapshotReady))

		var event featurepb.FeatureSnapshotReadyEvent
		Expect(proto.Unmarshal(message.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(featureSnapshot.DatasetID.String()))
		Expect(event.RawSnapshotId).To(Equal(rawSnapshotID.String()))
		Expect(event.FeatureSnapshotId).To(Equal(featureSnapshot.FeatureSnapshotID.String()))
		Expect(event.MaterializationEventSeq).To(Equal(featureSnapshot.MaterializationEventSeq))
		Expect(event.ProcessingProfile).To(Equal(featureSnapshot.ProcessingProfile.String()))
	})

	It("builds embedding snapshot ready outbound messages", func() {
		featureSnapshotID := uuid.New()
		embeddingSnapshot := validMessagingEmbeddingSnapshot(featureSnapshotID)
		builder := messaging.NewSnapshotEventBuilder(messaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})

		message, err := builder.EmbeddingSnapshotReadyMessage(embeddingSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(message.Topic).To(Equal("feature_materializer"))
		Expect(message.DispatchKey).To(Equal("embedding_snapshot_ready:" + embeddingSnapshot.EmbeddingSnapshotID.String()))
		Expect(message.Message.ResourceKey).To(Equal(embeddingSnapshot.DatasetID))
		Expect(message.Message.MsgType).To(Equal(sharedMessaging.MsgTypeEmbeddingSnapshotReady))

		var event featurepb.EmbeddingSnapshotReadyEvent
		Expect(proto.Unmarshal(message.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(embeddingSnapshot.DatasetID.String()))
		Expect(event.FeatureSnapshotId).To(Equal(featureSnapshotID.String()))
		Expect(event.EmbeddingSnapshotId).To(Equal(embeddingSnapshot.EmbeddingSnapshotID.String()))
		Expect(event.MaterializationEventSeq).To(Equal(embeddingSnapshot.MaterializationEventSeq))
		Expect(event.VectorStore).To(Equal(embeddingSnapshot.VectorStore))
		Expect(event.EmbeddingProvider).To(Equal(embeddingSnapshot.EmbeddingProvider))
	})

	It("rejects missing snapshots", func() {
		builder := messaging.NewSnapshotEventBuilder(messaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})

		_, rawErr := builder.RawSnapshotReadyMessage(nil)
		_, featureErr := builder.FeatureSnapshotReadyMessage(nil)
		_, embeddingErr := builder.EmbeddingSnapshotReadyMessage(nil)

		Expect(rawErr).To(MatchError(ContainSubstring("raw snapshot is required")))
		Expect(featureErr).To(MatchError(ContainSubstring("feature snapshot is required")))
		Expect(embeddingErr).To(MatchError(ContainSubstring("embedding snapshot is required")))
	})

	It("rejects snapshots without materialization sequence numbers", func() {
		builder := messaging.NewSnapshotEventBuilder(messaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		})
		rawSnapshot := validMessagingRawSnapshot(uuid.New())
		featureSnapshot := validMessagingFeatureSnapshot(uuid.New())
		embeddingSnapshot := validMessagingEmbeddingSnapshot(uuid.New())
		rawSnapshot.MaterializationEventSeq = 0
		featureSnapshot.MaterializationEventSeq = 0
		embeddingSnapshot.MaterializationEventSeq = 0

		_, rawErr := builder.RawSnapshotReadyMessage(rawSnapshot)
		_, featureErr := builder.FeatureSnapshotReadyMessage(featureSnapshot)
		_, embeddingErr := builder.EmbeddingSnapshotReadyMessage(embeddingSnapshot)

		Expect(rawErr).To(MatchError(ContainSubstring("raw snapshot materialization event sequence is required")))
		Expect(featureErr).To(MatchError(ContainSubstring("feature snapshot materialization event sequence is required")))
		Expect(embeddingErr).To(MatchError(ContainSubstring("embedding snapshot materialization event sequence is required")))
	})
})
