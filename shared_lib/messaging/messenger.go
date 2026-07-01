package messaging

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
)

type Messenger interface {
	Subscriber(ctx context.Context) (Subscriber, error)
	Publisher(ctx context.Context) (Publisher, error)
	Close(ctx context.Context) error
}

type MessengerConfig struct {
	Brokers         string
	GroupID         string
	DlqURL          string
	AutoOffsetReset string
}

type messenger struct {
	sub              Subscriber
	pub              Publisher
	brokers          string
	groupID          string
	dlqURL           string
	autoOffsetReset  string
	subscriberCancel context.CancelFunc
}

func exponentialBackOffFactory() *backoff.ExponentialBackOff {
	expBackoff := backoff.NewExponentialBackOff(
		backoff.WithMaxInterval(maxBackoffSeconds*time.Second),
		backoff.WithMaxElapsedTime(maxElapsedBackoffSeconds*time.Second),
	)
	return expBackoff
}

func NewMessenger(cfg MessengerConfig, cancel context.CancelFunc) Messenger {
	log.Trace("Messenger NewMessenger")

	return &messenger{
		brokers:          cfg.Brokers,
		groupID:          cfg.GroupID,
		dlqURL:           cfg.DlqURL,
		autoOffsetReset:  cfg.AutoOffsetReset,
		subscriberCancel: cancel,
	}
}

func (m *messenger) Subscriber(ctx context.Context) (Subscriber, error) {
	log.Trace("messenger Subscriber")

	if m.sub == nil {
		dlq := NewDLQ(ctx, m.dlqURL)
		var opts []SubscriberOption
		if m.autoOffsetReset != "" {
			opts = append(opts, WithAutoOffsetReset(m.autoOffsetReset))
		}
		sub, err := NewSubscriber(m.brokers, m.groupID, dlq, exponentialBackOffFactory, DefaultNumShards, DefaultChannelBuffer, opts...)
		if err != nil {
			return nil, err
		}

		m.sub = sub
	}
	return m.sub, nil
}

func (m *messenger) Publisher(ctx context.Context) (Publisher, error) {
	log.Trace("messenger Publisher")

	if m.pub == nil {
		pub, err := NewPublisher(m.brokers)
		if err != nil {
			return nil, err
		}
		m.pub = pub
	}
	return m.pub, nil
}

func (m messenger) Close(ctx context.Context) error {
	log.Trace("messenger Close")

	if m.pub != nil {
		m.pub.Close()
	}

	if m.subscriberCancel != nil {
		// cancel the context to stop the subscriber loop
		m.subscriberCancel()
	}
	return nil
}
