package messaging_test

import (
	"context"
	"errors"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	datasetpb "lib/data_contracts_lib/dataset"
	featurepb "lib/data_contracts_lib/feature_materializer"
	sharedMessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type recordingMaterializationPublisher struct {
	rawReady       *model.RawSnapshot
	featureRequest *model.RawSnapshot
	featureReady   *model.FeatureSnapshot
	embeddingReq   *model.FeatureSnapshot
	embeddingReady *model.EmbeddingSnapshot
}

func (p *recordingMaterializationPublisher) PublishRawSnapshotReady(_ context.Context, rawSnapshot *model.RawSnapshot) error {
	p.rawReady = rawSnapshot
	return nil
}

func (p *recordingMaterializationPublisher) PublishFeatureSnapshotBuildRequested(_ context.Context, rawSnapshot *model.RawSnapshot, _ uuid.UUID) error {
	p.featureRequest = rawSnapshot
	return nil
}

func (p *recordingMaterializationPublisher) PublishFeatureSnapshotReady(_ context.Context, featureSnapshot *model.FeatureSnapshot) error {
	p.featureReady = featureSnapshot
	return nil
}

func (p *recordingMaterializationPublisher) PublishEmbeddingMaterializationRequested(_ context.Context, featureSnapshot *model.FeatureSnapshot, _ uuid.UUID) error {
	p.embeddingReq = featureSnapshot
	return nil
}

func (p *recordingMaterializationPublisher) PublishEmbeddingSnapshotReady(_ context.Context, embeddingSnapshot *model.EmbeddingSnapshot) error {
	p.embeddingReady = embeddingSnapshot
	return nil
}

type featureSnapshotUsecaseStub struct {
	rawSnapshotID   uuid.UUID
	idempotencyKey  uuid.UUID
	featureSnapshot *model.FeatureSnapshot
	err             error
}

func (s *featureSnapshotUsecaseStub) BuildFeatureSnapshot(_ context.Context, rawSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	s.rawSnapshotID = rawSnapshotID
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return s.featureSnapshot, nil
}

type embeddingUsecaseStub struct {
	featureSnapshotID uuid.UUID
	idempotencyKey    uuid.UUID
	embeddingSnapshot *model.EmbeddingSnapshot
	err               error
}

func (s *embeddingUsecaseStub) MaterializeEmbeddings(_ context.Context, featureSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error) {
	s.featureSnapshotID = featureSnapshotID
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return s.embeddingSnapshot, nil
}

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

var _ = Describe("Materialization event flow", func() {
	It("publishes raw-ready and feature-build-requested after dataset upload", func() {
		datasetID := uuid.New()
		rawSnapshot := validMessagingRawSnapshot(datasetID)
		ucWithSnapshot := &rawSnapshotUsecaseReturning{snapshot: rawSnapshot}
		publisher := &recordingMaterializationPublisher{}
		listener := featuremessaging.NewDatasetFileUploadedEventListenerWithPublisher(ucWithSnapshot, publisher)

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetFileUploadedEvent{
			DatasetId:       datasetID.String(),
			UserId:          rawSnapshot.UserID.String(),
			StorageLocation: "s3://local-dev-bucket/raw/file.csv",
			ContentType:     "text/csv",
			FileExtension:   "csv",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.rawReady.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		Expect(publisher.featureRequest.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
	})

	It("publishes downstream events when feature build succeeds", func() {
		rawSnapshotID := uuid.New()
		idempotencyKey := uuid.New()
		featureSnapshot := validMessagingFeatureSnapshot(rawSnapshotID)
		uc := &featureSnapshotUsecaseStub{featureSnapshot: featureSnapshot}
		publisher := &recordingMaterializationPublisher{}
		listener := featuremessaging.NewFeatureSnapshotBuildRequestedEventListener(uc, publisher)

		err := listener.Handle(context.Background(), featureSnapshot.DatasetID, &featurepb.FeatureSnapshotBuildRequestedEvent{
			RawSnapshotId:  rawSnapshotID.String(),
			IdempotencyKey: idempotencyKey.String(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.rawSnapshotID).To(Equal(rawSnapshotID))
		Expect(uc.idempotencyKey).To(Equal(idempotencyKey))
		Expect(publisher.featureReady.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(publisher.embeddingReq.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
	})

	It("publishes embedding-ready when embedding materialization succeeds", func() {
		featureSnapshotID := uuid.New()
		idempotencyKey := uuid.New()
		embeddingSnapshot := validMessagingEmbeddingSnapshot(featureSnapshotID)
		uc := &embeddingUsecaseStub{embeddingSnapshot: embeddingSnapshot}
		publisher := &recordingMaterializationPublisher{}
		listener := featuremessaging.NewEmbeddingMaterializationRequestedEventListener(uc, publisher)

		err := listener.Handle(context.Background(), embeddingSnapshot.DatasetID, &featurepb.EmbeddingMaterializationRequestedEvent{
			FeatureSnapshotId: featureSnapshotID.String(),
			IdempotencyKey:    idempotencyKey.String(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.featureSnapshotID).To(Equal(featureSnapshotID))
		Expect(uc.idempotencyKey).To(Equal(idempotencyKey))
		Expect(publisher.embeddingReady.EmbeddingSnapshotID).To(Equal(embeddingSnapshot.EmbeddingSnapshotID))
	})

	It("re-publishes feature events for idempotent feature replay", func() {
		featureSnapshot := validMessagingFeatureSnapshot(uuid.New())
		uc := &featureSnapshotUsecaseStub{err: &domain.FeatureSnapshotAlreadyBuiltError{Record: featureSnapshot}}
		publisher := &recordingMaterializationPublisher{}
		listener := featuremessaging.NewFeatureSnapshotBuildRequestedEventListener(uc, publisher)

		err := listener.Handle(context.Background(), featureSnapshot.DatasetID, &featurepb.FeatureSnapshotBuildRequestedEvent{
			RawSnapshotId:  featureSnapshot.RawSnapshotID.String(),
			IdempotencyKey: uuid.NewString(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.featureReady.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
	})

	It("classifies invalid downstream event payloads as non-retryable", func() {
		listener := featuremessaging.NewEmbeddingMaterializationRequestedEventListener(&embeddingUsecaseStub{}, nil)

		err := listener.Handle(context.Background(), uuid.New(), &featurepb.EmbeddingMaterializationRequestedEvent{
			FeatureSnapshotId: "invalid",
			IdempotencyKey:    uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedMessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("publishes typed envelopes through the shared publisher", func() {
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

	It("returns transient usecase errors from downstream listeners", func() {
		expectedErr := errors.New("temporary failure")
		listener := featuremessaging.NewFeatureSnapshotBuildRequestedEventListener(&featureSnapshotUsecaseStub{err: expectedErr}, nil)

		err := listener.Handle(context.Background(), uuid.New(), &featurepb.FeatureSnapshotBuildRequestedEvent{
			RawSnapshotId:  uuid.NewString(),
			IdempotencyKey: uuid.NewString(),
		})

		Expect(errors.Is(err, expectedErr)).To(BeTrue())
	})
})

type rawSnapshotUsecaseReturning struct {
	snapshot *model.RawSnapshot
	err      error
}

func (u *rawSnapshotUsecaseReturning) MaterializeRawSnapshot(context.Context, *model.DatasetFile, uuid.UUID) (*model.RawSnapshot, error) {
	if u.err != nil {
		return nil, u.err
	}
	return u.snapshot, nil
}

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
