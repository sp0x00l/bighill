package messaging_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"lib/shared_lib/messaging"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type MockKafkaProducer struct {
	MessagesMap  map[string][]byte
	ErrorProduce error
	mu           sync.Mutex
}

type MockOutbox struct {
	Messages []messaging.OutboxMessage
	Err      error
	mu       sync.Mutex
}

func (m *MockOutbox) WriteMessage(_ context.Context, message messaging.OutboxMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Messages = append(m.Messages, message)
	return nil
}

func NewMockKafkaProducer() *MockKafkaProducer {
	return &MockKafkaProducer{
		MessagesMap: make(map[string][]byte),
	}
}

func (m *MockKafkaProducer) Produce(msg *kafka.Message, _ chan kafka.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ErrorProduce != nil {
		return m.ErrorProduce
	}
	m.MessagesMap[string(*msg.TopicPartition.Topic)] = msg.Value

	return nil
}

func (m *MockKafkaProducer) Flush(timeoutMs int) int {
	return 0
}

func (m *MockKafkaProducer) Close() {}

func TestInfraKafkaProducer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kafka Publisher Suite")
}

var _ = Describe("Kafka Publisher", func() {
	var (
		mockKafkaProducer *MockKafkaProducer
		mockOutbox        *MockOutbox
		publisher         messaging.Publisher
		topic             string
		message           messaging.Message
		key               uuid.UUID
		ctx               context.Context
	)

	BeforeEach(func() {
		key = uuid.New()
		topic = "test-topic"
		mockKafkaProducer = NewMockKafkaProducer()
		mockOutbox = &MockOutbox{}
		ctx = context.Background()

		var err error
		publisher, err = messaging.NewPublisher("test-brokers", messaging.WithOutbox(mockOutbox), messaging.WithProducer(mockKafkaProducer))
		Expect(err).To(BeNil())

		message = messaging.Message{
			MsgType:     messaging.MsgTypeUserCreated,
			Payload:     []byte("test message"),
			ResourceKey: key,
		}
	})

	Context("when successfully publishing a message", func() {
		It("should enqueue the message without direct kafka publish", func() {
			err := publisher.Publish(ctx, topic, message, nil)
			Expect(err).To(BeNil())
			msgBytes, _ := message.Serialize(ctx, nil)
			Expect(mockKafkaProducer.MessagesMap).To(BeEmpty())
			Expect(mockOutbox.Messages).To(HaveLen(1))
			Expect(mockOutbox.Messages[0].Topic).To(Equal(topic))
			Expect(mockOutbox.Messages[0].Payload).To(Equal(msgBytes))
		})
	})

	Context("when direct producer is failing", func() {
		BeforeEach(func() {
			mockKafkaProducer.ErrorProduce = errors.New("produce-error")
		})

		It("should still succeed with outbox enabled", func() {
			err := publisher.Publish(ctx, topic, message, nil)
			Expect(err).To(BeNil())
		})
	})

	Context("when outbox enqueue fails", func() {
		BeforeEach(func() {
			mockOutbox.Err = errors.New("outbox-error")
		})

		It("should return an error and not publish to kafka", func() {
			err := publisher.Publish(ctx, topic, message, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("outbox-error"))
			Expect(mockKafkaProducer.MessagesMap).To(BeEmpty())
		})
	})
})
