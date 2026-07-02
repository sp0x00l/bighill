package messaging

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type signaledOutbox struct {
	base   OutboxWriter
	signal chan<- struct{}
}

func NewSignaledOutbox(base OutboxWriter, signal chan<- struct{}) OutboxWriter {
	log.Trace("NewSignaledOutbox")

	return &signaledOutbox{
		base:   base,
		signal: signal,
	}
}

func NotifyOutboxSignal(signal chan<- struct{}) {
	log.Trace("NotifyOutboxSignal")

	if signal == nil {
		return
	}
	select {
	case signal <- struct{}{}:
	default:
	}
}

func (s *signaledOutbox) WriteMessage(ctx context.Context, message OutboxMessage) error {
	log.Trace("signaledOutbox WriteMessage")

	if s.base == nil {
		return fmt.Errorf("signaled outbox requires a base outbox")
	}
	if err := s.base.WriteMessage(ctx, message); err != nil {
		return err
	}
	NotifyOutboxSignal(s.signal)
	return nil
}

func (s *signaledOutbox) Enqueue(ctx context.Context, msg OutboundMessage) error {
	log.Trace("signaledOutbox Enqueue")

	if s.base == nil {
		return fmt.Errorf("signaled outbox requires a base outbox")
	}
	if outbox, ok := s.base.(Outbox); ok {
		if err := outbox.Enqueue(ctx, msg); err != nil {
			return err
		}
		NotifyOutboxSignal(s.signal)
		return nil
	}

	outboxMessage, err := outboxMessageFromOutbound(ctx, msg)
	if err != nil {
		return err
	}
	if err := s.base.WriteMessage(ctx, outboxMessage); err != nil {
		return err
	}
	NotifyOutboxSignal(s.signal)
	return nil
}

func (s *signaledOutbox) ReadPending(ctx context.Context, batchSize int32) ([]OutboxPendingMessage, error) {
	log.Trace("signaledOutbox ReadPending")

	relay, ok := s.base.(RelayOutbox)
	if !ok {
		return nil, fmt.Errorf("configured outbox backend does not support relay operations")
	}
	return relay.ReadPending(ctx, batchSize)
}

func (s *signaledOutbox) MarkSent(ctx context.Context, pending OutboxPendingMessage) error {
	log.Trace("signaledOutbox MarkSent")

	relay, ok := s.base.(RelayOutbox)
	if !ok {
		return fmt.Errorf("configured outbox backend does not support relay operations")
	}
	return relay.MarkSent(ctx, pending)
}

func (s *signaledOutbox) MarkFailed(ctx context.Context, pending OutboxPendingMessage, reason string, nextAttemptAt time.Time) error {
	log.Trace("signaledOutbox MarkFailed")

	relay, ok := s.base.(RelayOutbox)
	if !ok {
		return fmt.Errorf("configured outbox backend does not support relay operations")
	}
	return relay.MarkFailed(ctx, pending, reason, nextAttemptAt)
}

func (s *signaledOutbox) ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration) {
	log.Trace("signaledOutbox ConfigureRelayIdentity")

	if configurable, ok := s.base.(interface {
		ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration)
	}); ok {
		configurable.ConfigureRelayIdentity(ownerID, leaseDuration)
	}
}
