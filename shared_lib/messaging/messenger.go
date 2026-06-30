package messaging

import (
	"context"
	"fmt"

	"time"

	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
)

type Messenger interface {
	Subscriber(ctx context.Context) (Subscriber, error)
	Publisher(ctx context.Context) (Publisher, error)
	OutboxRelay(ctx context.Context, cfg OutboxRelayConfig) (*OutboxRelay, error)
	Close(ctx context.Context) error
}

type MessengerConfig struct {
	Brokers         string
	GroupID         string
	DlqURL          string
	OutboxURL       string
	AutoOffsetReset string
}

type messenger struct {
	sub              Subscriber
	pub              Publisher
	brokers          string
	groupID          string
	dlqURL           string
	outboxURL        string
	autoOffsetReset  string
	outbox           OutboxWriter
	publishSignal    chan struct{}
	subscriberCancel context.CancelFunc
}

type signalOutbox struct {
	base   OutboxWriter
	signal chan<- struct{}
}

func newSignalOutbox(base OutboxWriter, signal chan<- struct{}) OutboxWriter {
	return &signalOutbox{
		base:   base,
		signal: signal,
	}
}

func (s *signalOutbox) WriteMessage(ctx context.Context, message OutboxMessage) error {
	log.Trace("signalOutbox WriteMessage")

	if err := s.base.WriteMessage(ctx, message); err != nil {
		return err
	}
	if s.signal != nil && !isNoopOutbox(s.base) {
		select {
		case s.signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *signalOutbox) Enqueue(ctx context.Context, msg OutboundMessage) error {
	log.Trace("signalOutbox Enqueue")

	if outbox, ok := s.base.(Outbox); ok {
		if err := outbox.Enqueue(ctx, msg); err != nil {
			return err
		}
	} else {
		outboxMessage, err := outboxMessageFromOutbound(ctx, msg)
		if err != nil {
			return err
		}
		if err := s.base.WriteMessage(ctx, outboxMessage); err != nil {
			return err
		}
	}
	if s.signal != nil && !isNoopOutbox(s.base) {
		select {
		case s.signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *signalOutbox) ReadPending(ctx context.Context, batchSize int32) ([]OutboxPendingMessage, error) {
	log.Trace("signalOutbox ReadPending")

	relay, ok := s.base.(RelayOutbox)
	if !ok {
		return nil, fmt.Errorf("configured outbox backend does not support relay operations")
	}
	return relay.ReadPending(ctx, batchSize)
}

func (s *signalOutbox) MarkSent(ctx context.Context, pending OutboxPendingMessage) error {
	log.Trace("signalOutbox MarkSent")

	relay, ok := s.base.(RelayOutbox)
	if !ok {
		return fmt.Errorf("configured outbox backend does not support relay operations")
	}
	return relay.MarkSent(ctx, pending)
}

func (s *signalOutbox) MarkFailed(ctx context.Context, pending OutboxPendingMessage, reason string, nextAttemptAt time.Time) error {
	log.Trace("signalOutbox MarkFailed")

	relay, ok := s.base.(RelayOutbox)
	if !ok {
		return fmt.Errorf("configured outbox backend does not support relay operations")
	}
	return relay.MarkFailed(ctx, pending, reason, nextAttemptAt)
}

func (s *signalOutbox) ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration) {
	if configurable, ok := s.base.(interface {
		ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration)
	}); ok {
		configurable.ConfigureRelayIdentity(ownerID, leaseDuration)
	}
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
		outboxURL:        cfg.OutboxURL,
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
		if m.publishSignal == nil {
			m.publishSignal = make(chan struct{}, 1)
		}
		baseOutbox := NewOutbox(ctx, m.outboxURL)
		outbox := newSignalOutbox(baseOutbox, m.publishSignal)
		pub, err := NewPublisher(m.brokers, WithOutbox(outbox))
		if err != nil {
			return nil, err
		}
		m.outbox = outbox
		m.pub = pub
	}
	return m.pub, nil
}

func (m *messenger) OutboxRelay(ctx context.Context, cfg OutboxRelayConfig) (*OutboxRelay, error) {
	log.Trace("messenger OutboxRelay")

	pub, err := m.Publisher(ctx)
	if err != nil {
		return nil, err
	}
	relayOutbox, ok := m.outbox.(RelayOutbox)
	if !ok {
		return nil, fmt.Errorf("configured outbox backend does not support relay operations")
	}
	if cfg.Signal == nil && m.publishSignal != nil {
		cfg.Signal = m.publishSignal
	}
	relayPublisher, ok := pub.(RelayPublisher)
	if !ok {
		return nil, fmt.Errorf("configured publisher does not support relay publishing")
	}
	return NewOutboxRelay(relayOutbox, relayPublisher, cfg), nil
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
