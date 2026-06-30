package messaging

import (
	"context"
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type mockRelayOutbox struct {
	pending     []OutboxPendingMessage
	readErr     error
	markSent    []OutboxPendingMessage
	markFailed  []OutboxPendingMessage
	markSentErr error
	markFailErr error
}

func (m *mockRelayOutbox) WriteMessage(_ context.Context, _ OutboxMessage) error { return nil }

func (m *mockRelayOutbox) ReadPending(_ context.Context, _ int32) ([]OutboxPendingMessage, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	return m.pending, nil
}

func (m *mockRelayOutbox) MarkSent(_ context.Context, pending OutboxPendingMessage) error {
	m.markSent = append(m.markSent, pending)
	return m.markSentErr
}

func (m *mockRelayOutbox) MarkFailed(_ context.Context, pending OutboxPendingMessage, _ string, _ time.Time) error {
	m.markFailed = append(m.markFailed, pending)
	return m.markFailErr
}

type mockRelayPublisher struct {
	err       error
	published []OutboxPendingMessage
}

func (m *mockRelayPublisher) Publish(_ context.Context, _ string, _ Message, _ proto.Message) error {
	return nil
}

func (m *mockRelayPublisher) PublishOutboxMessage(_ context.Context, topic string, payload []byte, headers []kafka.Header) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, OutboxPendingMessage{
		Topic:   topic,
		Payload: payload,
		Headers: headers,
	})
	return nil
}

var _ = Describe("OutboxRelay", func() {
	It("marks message sent when raw publish succeeds", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		outbox := &mockRelayOutbox{
			pending: []OutboxPendingMessage{
				{PK: "p1", SK: "s1", Topic: "profile", Payload: []byte("abc")},
			},
		}
		pub := &mockRelayPublisher{}
		relay := NewOutboxRelay(outbox, pub, OutboxRelayConfig{
			PollInterval:   time.Millisecond,
			FailureBackoff: time.Millisecond,
			BatchSize:      10,
		})

		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		_ = relay.Run(ctx)
		Expect(outbox.markSent).ToNot(BeEmpty())
		Expect(outbox.markFailed).To(BeEmpty())
	})

	It("marks message failed when publish fails", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		outbox := &mockRelayOutbox{
			pending: []OutboxPendingMessage{
				{PK: "p1", SK: "s1", Topic: "profile", Payload: []byte("abc")},
			},
		}
		pub := &mockRelayPublisher{err: errors.New("publish-failed")}
		relay := NewOutboxRelay(outbox, pub, OutboxRelayConfig{
			PollInterval:   time.Millisecond,
			FailureBackoff: time.Millisecond,
			BatchSize:      10,
		})

		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		_ = relay.Run(ctx)
		Expect(outbox.markFailed).ToNot(BeEmpty())
	})
})
