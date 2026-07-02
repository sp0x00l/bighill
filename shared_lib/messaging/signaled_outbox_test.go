package messaging_test

import (
	"context"
	"errors"
	"time"

	"lib/shared_lib/messaging"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type signaledOutboxStub struct {
	err    error
	writes int
}

func (s *signaledOutboxStub) WriteMessage(context.Context, messaging.OutboxMessage) error {
	if s.err != nil {
		return s.err
	}
	s.writes++
	return nil
}

func (s *signaledOutboxStub) ReadPending(context.Context, int32) ([]messaging.OutboxPendingMessage, error) {
	return nil, nil
}

func (s *signaledOutboxStub) MarkSent(context.Context, messaging.OutboxPendingMessage) error {
	return nil
}

func (s *signaledOutboxStub) MarkFailed(context.Context, messaging.OutboxPendingMessage, string, time.Time) error {
	return nil
}

var _ = Describe("SignaledOutbox", func() {
	It("signals after a message is written", func() {
		signal := make(chan struct{}, 1)
		base := &signaledOutboxStub{}
		outbox := messaging.NewSignaledOutbox(base, signal)

		err := outbox.WriteMessage(context.Background(), messaging.OutboxMessage{
			Topic: "data_registry",
			Message: messaging.Message{
				ResourceKey: uuid.New(),
				MsgType:     messaging.MsgTypeDatasetUpdated,
				Payload:     []byte("payload"),
			},
			Payload: []byte("payload"),
			Headers: []kafka.Header{{Key: "traceparent", Value: []byte("trace")}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(base.writes).To(Equal(1))
		Eventually(signal).Should(Receive())
	})

	It("does not signal when the write fails", func() {
		signal := make(chan struct{}, 1)
		base := &signaledOutboxStub{err: errors.New("write failed")}
		outbox := messaging.NewSignaledOutbox(base, signal)

		err := outbox.WriteMessage(context.Background(), messaging.OutboxMessage{})

		Expect(err).To(MatchError("write failed"))
		Consistently(signal).ShouldNot(Receive())
	})

	It("uses non-blocking signal delivery", func() {
		signal := make(chan struct{}, 1)
		messaging.NotifyOutboxSignal(signal)

		Expect(func() { messaging.NotifyOutboxSignal(signal) }).NotTo(Panic())
		Expect(signal).To(HaveLen(1))
	})
})
