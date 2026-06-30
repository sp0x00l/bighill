package messaging

import (
	"context"

	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type MaterializationSubscriber struct {
	subscriber                  msgConn.Subscriber
	rawSnapshotUsecase          DatasetFileUploadedListener
	featureSnapshotUsecase      FeatureSnapshotBuildRequestedListener
	embeddingMaterializeUsecase EmbeddingMaterializationRequestedListener
	publisher                   MaterializationEventPublisher
	topics                      []string
}

func NewMaterializationSubscriber(
	subscriber msgConn.Subscriber,
	rawSnapshotUsecase DatasetFileUploadedListener,
	featureSnapshotUsecase FeatureSnapshotBuildRequestedListener,
	embeddingMaterializeUsecase EmbeddingMaterializationRequestedListener,
	publisher MaterializationEventPublisher,
	topics []string,
) *MaterializationSubscriber {
	log.Trace("NewMaterializationSubscriber")

	configureErrorPolicy(subscriber)
	return &MaterializationSubscriber{
		subscriber:                  subscriber,
		rawSnapshotUsecase:          rawSnapshotUsecase,
		featureSnapshotUsecase:      featureSnapshotUsecase,
		embeddingMaterializeUsecase: embeddingMaterializeUsecase,
		publisher:                   publisher,
		topics:                      topics,
	}
}

func (s *MaterializationSubscriber) Start(ctx context.Context) error {
	log.Trace("MaterializationSubscriber Start")

	msgConn.AddListener(s.subscriber, NewDatasetFileUploadedEventListenerWithPublisher(s.rawSnapshotUsecase, s.publisher))
	msgConn.AddListener(s.subscriber, NewFeatureSnapshotBuildRequestedEventListener(s.featureSnapshotUsecase, s.publisher))
	msgConn.AddListener(s.subscriber, NewEmbeddingMaterializationRequestedEventListener(s.embeddingMaterializeUsecase, s.publisher))
	return s.subscriber.Subscribe(ctx, s.topics)
}
