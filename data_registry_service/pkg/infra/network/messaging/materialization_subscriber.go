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
	log.Trace("MaterializationTopics List")

	return []string{t.FeatureMaterializer}
}

type MaterializationSubscriber struct {
	subscriber msgConn.Subscriber
	usecase    usecase.DatasetUsecase
	topics     MaterializationTopics
}

func NewMaterializationSubscriber(subscriber msgConn.Subscriber, usecase usecase.DatasetUsecase, topics MaterializationTopics) *MaterializationSubscriber {
	log.Trace("NewMaterializationSubscriber")

	ConfigureSubscriberErrorPolicy(subscriber)
	return &MaterializationSubscriber{
		subscriber: subscriber,
		usecase:    usecase,
		topics:     topics,
	}
}

func ConfigureMaterializationSubscriber(subscriber msgConn.Subscriber, usecase usecase.DatasetUsecase) {
	log.Trace("ConfigureMaterializationSubscriber")

	ConfigureSubscriberErrorPolicy(subscriber)
	msgConn.AddListener(subscriber, NewRawSnapshotReadyEventListener(usecase))
	msgConn.AddListener(subscriber, NewFeatureSnapshotReadyEventListener(usecase))
	msgConn.AddListener(subscriber, NewEmbeddingSnapshotReadyEventListener(usecase))
	msgConn.AddListener(subscriber, NewGraphSnapshotReadyEventListener(usecase))
}

func (s *MaterializationSubscriber) Start(ctx context.Context) error {
	log.Trace("MaterializationSubscriber Start")

	ConfigureMaterializationSubscriber(s.subscriber, s.usecase)
	return s.subscriber.Subscribe(ctx, s.topics.List())
}
