package messaging

import (
	"context"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

// OutboundMessage is the producer-side message shape shared by the two outbox
// ports. Message.Payload is the canonical inner protobuf payload; transport
// backplanes serialize the full message envelope before publish.
type OutboundMessage struct {
	Topic       string
	Message     Message
	Headers     []kafka.Header
	DispatchKey string
}

// Outbox is the at-least-once, non-transactional publish handoff.
//
// Contract:
//   - Enqueue has no transactional binding to business state.
//   - Ordering across messages is not guaranteed.
//   - Callers must only enqueue after the business operation has succeeded,
//     and only when crash-after-commit-before-enqueue loss is acceptable or
//     recoverable out of band.
//   - Consumers must be idempotent.
type Outbox interface {
	Enqueue(ctx context.Context, msg OutboundMessage) error
}

// OrderedOutbox is the transactional, per-resource ordered publish handoff.
//
// Contract:
//   - EnqueueTx must be called with the caller's business transaction.
//   - On commit, the message becomes deliverable; on rollback, it does not.
//   - Enqueues for the same ResourceKey preserve enqueue order at consumers.
//   - DispatchKey must be non-empty and idempotent for the domain event.
//   - Delivery is at-least-once; consumers must be idempotent.
type OrderedOutbox interface {
	EnqueueTx(ctx context.Context, tx pgx.Tx, msg OutboundMessage) error
}

func (m OutboundMessage) Validate() error {
	log.Trace("OutboundMessage Validate")
	if strings.TrimSpace(m.Topic) == "" {
		return fmt.Errorf("outbound message requires topic")
	}
	if err := m.Message.Validate(); err != nil {
		return fmt.Errorf("%w: outbound message: %w", ErrEnvelopeInvalid, err)
	}
	if strings.TrimSpace(m.DispatchKey) == "" {
		return fmt.Errorf("%w: dispatch_key required", ErrDispatchKeyRequired)
	}
	return nil
}
