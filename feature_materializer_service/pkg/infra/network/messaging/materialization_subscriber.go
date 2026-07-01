package messaging

import (
	"context"

	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type MaterializationSubscriber struct {
	subscriber msgConn.Subscriber
	listener   DatasetFileUploadedListener
	topics     []string
}

func NewMaterializationSubscriber(
	subscriber msgConn.Subscriber,
	listener DatasetFileUploadedListener,
	topics []string,
) *MaterializationSubscriber {
	log.Trace("NewMaterializationSubscriber")

	configureErrorPolicy(subscriber)
	return &MaterializationSubscriber{
		subscriber: subscriber,
		listener:   listener,
		topics:     topics,
	}
}

func (s *MaterializationSubscriber) Start(ctx context.Context) error {
	log.Trace("MaterializationSubscriber Start")

	msgConn.AddListener(s.subscriber, NewDatasetFileUploadedEventListener(s.listener))
	return s.subscriber.Subscribe(ctx, s.topics)
}
