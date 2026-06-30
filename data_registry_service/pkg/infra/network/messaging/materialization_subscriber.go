package messaging

import (
	"context"

	usecase "data_registry_service/pkg/app"
	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type MaterializationTopics struct {
	FeatureMaterializer string
}

func (t MaterializationTopics) List() []string {
	return []string{t.FeatureMaterializer}
}

type MaterializationSubscriber struct {
	subscriber msgConn.Subscriber
	usecase    usecase.DatasetUsecase
	topics     MaterializationTopics
}

func NewMaterializationSubscriber(subscriber msgConn.Subscriber, usecase usecase.DatasetUsecase, topics MaterializationTopics) *MaterializationSubscriber {
	log.Trace("NewMaterializationSubscriber")

	configureErrorPolicy(subscriber)
	return &MaterializationSubscriber{
		subscriber: subscriber,
		usecase:    usecase,
		topics:     topics,
	}
}

func (s *MaterializationSubscriber) Start(ctx context.Context) error {
	log.Trace("MaterializationSubscriber Start")

	msgConn.AddListener(s.subscriber, NewRawSnapshotReadyEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewFeatureSnapshotReadyEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewEmbeddingSnapshotReadyEventListener(s.usecase))
	return s.subscriber.Subscribe(ctx, s.topics.List())
}
