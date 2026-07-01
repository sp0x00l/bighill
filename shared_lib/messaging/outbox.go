package messaging

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"lib/shared_lib/idem"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

type OutboxMessage struct {
	Topic       string
	Message     Message
	Payload     []byte
	Headers     []kafka.Header
	DispatchKey string
}

type OutboxWriter interface {
	WriteMessage(ctx context.Context, message OutboxMessage) error
}

type RelayOutbox interface {
	ReadPending(ctx context.Context, maxMessages int32) ([]OutboxPendingMessage, error)
	MarkSent(ctx context.Context, pending OutboxPendingMessage) error
	MarkFailed(ctx context.Context, pending OutboxPendingMessage, lastError string, nextAttemptAt time.Time) error
}

type OutboxPendingMessage struct {
	PK              string
	SK              string
	Topic           string
	Payload         []byte
	Headers         []kafka.Header
	Attempts        int
	ProcessingOwner string
	ClaimToken      string
}

func outboxMessageFromOutbound(ctx context.Context, msg OutboundMessage) (OutboxMessage, error) {
	log.Trace("outboxMessageFromOutbound")

	if err := msg.Validate(); err != nil {
		return OutboxMessage{}, err
	}
	payload, err := msg.Message.SerializeEnvelope(ctx)
	if err != nil {
		return OutboxMessage{}, fmt.Errorf("serialize outbound message envelope: %w", err)
	}
	return OutboxMessage{
		Topic:       msg.Topic,
		Message:     msg.Message,
		Payload:     payload,
		Headers:     traceHeaders(ctx, msg.Headers),
		DispatchKey: msg.DispatchKey,
	}, nil
}

func traceHeaders(ctx context.Context, headers []kafka.Header) []kafka.Header {
	log.Trace("traceHeaders")

	propagator := otel.GetTextMapPropagator()
	carrier := TraceHeadersCarrier{}
	propagator.Inject(ctx, &carrier)
	out := append([]kafka.Header{}, headers...)
	return append(out, []kafka.Header(carrier)...)
}

func deriveOutboxEventID(topic string, message Message, payload []byte, createdAt string) string {
	log.Trace("deriveOutboxEventID")

	payloadHash := sha256.Sum256(payload)
	return idem.FromParts(
		idem.Outbox,
		topic,
		message.MsgType.String(),
		message.ResourceKey.String(),
		fmt.Sprintf("%x", payloadHash),
		createdAt,
	).String()
}
