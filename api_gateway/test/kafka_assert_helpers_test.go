package test

import (
	"context"
	"errors"
	"fmt"
	env "lib/shared_lib/env"
	msgConn "lib/shared_lib/messaging"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type kafkaEventCollector[T proto.Message] struct {
	msgType    msgConn.MsgType
	newMessage func() T

	mu      sync.Mutex
	records []kafkaEventRecord[T]
}

type kafkaEventRecord[T proto.Message] struct {
	resourceKey uuid.UUID
	payload     T
}

func newKafkaEventCollector[T proto.Message](msgType msgConn.MsgType, newMessage func() T) *kafkaEventCollector[T] {
	return &kafkaEventCollector[T]{
		msgType:    msgType,
		newMessage: newMessage,
	}
}

func (c *kafkaEventCollector[T]) MsgType() msgConn.MsgType {
	return c.msgType
}

func (c *kafkaEventCollector[T]) NewMessage() T {
	return c.newMessage()
}

func (c *kafkaEventCollector[T]) Handle(_ context.Context, resourceKey uuid.UUID, payload T) error {
	if any(payload) == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, kafkaEventRecord[T]{
		resourceKey: resourceKey,
		payload:     cloneProto(payload),
	})
	return nil
}

func (c *kafkaEventCollector[T]) waitFor(resourceKey uuid.UUID, timeout time.Duration, predicate func(T) bool) T {
	var found T
	Eventually(func() bool {
		event, ok := c.find(resourceKey, predicate)
		if !ok {
			return false
		}
		found = event
		return true
	}, timeout, 100*time.Millisecond).Should(BeTrue(), "missing Kafka %s event for resource_key=%s", c.msgType.String(), resourceKey)
	return found
}

func (c *kafkaEventCollector[T]) find(resourceKey uuid.UUID, predicate func(T) bool) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range c.records {
		if record.resourceKey != resourceKey {
			continue
		}
		if predicate == nil || predicate(record.payload) {
			return record.payload, true
		}
	}
	var zero T
	return zero, false
}

func cloneProto[T proto.Message](payload T) T {
	cloned, ok := proto.Clone(payload).(T)
	if ok {
		return cloned
	}
	return payload
}

func topicList(raw string) []string {
	parts := strings.Split(raw, ",")
	topics := make([]string, 0, len(parts))
	for _, part := range parts {
		topic := strings.TrimSpace(part)
		if topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}

func newKafkaAssertsSubscriber(ctx context.Context, topics []string) (msgConn.Subscriber, func(), context.CancelFunc) {
	return newKafkaAssertsSubscriberWithOffset(ctx, topics, "")
}

func newKafkaAssertsSubscriberWithOffset(ctx context.Context, topics []string, autoOffsetReset string) (msgConn.Subscriber, func(), context.CancelFunc) {
	brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
	dlqURL := env.WithDefaultString("KAFKA_DLQ_URL", "")
	groupID := "api-gateway-asserts-" + uuid.NewString()[:8]

	factory := msgConn.NewMessenger(msgConn.MessengerConfig{
		DlqURL:          dlqURL,
		GroupID:         groupID,
		Brokers:         brokers,
		AutoOffsetReset: autoOffsetReset,
	}, nil)

	subscriber, err := factory.Subscriber(ctx)
	Expect(err).NotTo(HaveOccurred())

	subCtx, cancel := context.WithCancel(context.Background())
	start := func() {
		go func() {
			err := subscriber.Subscribe(subCtx, topics)
			if err != nil && !errors.Is(err, context.Canceled) {
				Fail(fmt.Sprintf("kafka assert subscriber failed for topics %v: %v", topics, err))
			}
		}()
		time.Sleep(500 * time.Millisecond)
	}

	return subscriber, start, cancel
}
