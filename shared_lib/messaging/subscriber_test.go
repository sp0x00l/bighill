package messaging_test

import (
	"context"
	"errors"
	"hash/fnv"
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"

	"google.golang.org/protobuf/types/known/wrapperspb"
	"lib/shared_lib/messaging"
	metrics "lib/shared_lib/metrics"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var mutex sync.Mutex

type MockKafkaConsumer struct {
	events         []kafka.Event
	SubscribeError error
	CommitError    error
	CommitCount    int
	CommitOffsets  []kafka.Offset
	SeekError      error
	SeekCount      int
	Assigned       []kafka.TopicPartition
	Paused         []kafka.TopicPartition
	Resumed        []kafka.TopicPartition
	Positions      []kafka.TopicPartition
	Watermarks     map[string]int64
	WatermarkError error
	Subscribed     []string
	WaitGroup      *sync.WaitGroup
	// TODO check close count
	CloseCount int
}

func NewMockKafkaConsumer() *MockKafkaConsumer {
	return &MockKafkaConsumer{
		events: make([]kafka.Event, 0),
	}
}

func (m *MockKafkaConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	mutex.Lock()
	defer mutex.Unlock()

	if m.SubscribeError != nil {
		return m.SubscribeError
	}

	m.Subscribed = append([]string(nil), topics...)
	assignments := make([]kafka.Event, 0, len(topics))
	for _, topic := range topics {
		topic := topic
		assignments = append(assignments, kafka.AssignedPartitions{
			Partitions: []kafka.TopicPartition{{Topic: &topic, Partition: 0}},
		})
	}
	if rebalanceCb != nil {
		for _, assignment := range assignments {
			if err := rebalanceCb(nil, assignment); err != nil {
				return err
			}
		}
		return nil
	}
	m.events = append(assignments, m.events...)
	return nil
}

func (m *MockKafkaConsumer) Poll(_ int) (event kafka.Event) {
	mutex.Lock()
	defer mutex.Unlock()

	if len(m.events) > 0 {
		event = m.events[0]
		m.events = m.events[1:]
	}
	return event
}

func (m *MockKafkaConsumer) WriteEvent(event kafka.Event) {
	mutex.Lock()
	defer mutex.Unlock()

	m.events = append(m.events, event)
}

func (m *MockKafkaConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if m.WaitGroup != nil {
		defer m.WaitGroup.Done()
	}

	m.CommitCount++
	m.CommitOffsets = append(m.CommitOffsets, msg.TopicPartition.Offset)
	return []kafka.TopicPartition{msg.TopicPartition}, m.CommitError
}

func (m *MockKafkaConsumer) Seek(partition kafka.TopicPartition, timeoutMs int) error {
	mutex.Lock()
	defer mutex.Unlock()

	m.SeekCount++
	return m.SeekError
}

func (m *MockKafkaConsumer) Assign(partitions []kafka.TopicPartition) error {
	mutex.Lock()
	defer mutex.Unlock()

	m.Assigned = append([]kafka.TopicPartition(nil), partitions...)
	return nil
}

func (m *MockKafkaConsumer) Unassign() error {
	mutex.Lock()
	defer mutex.Unlock()

	m.Assigned = nil
	return nil
}

func (m *MockKafkaConsumer) Pause(partitions []kafka.TopicPartition) error {
	mutex.Lock()
	defer mutex.Unlock()

	m.Paused = append([]kafka.TopicPartition(nil), partitions...)
	return nil
}

func (m *MockKafkaConsumer) Resume(partitions []kafka.TopicPartition) error {
	mutex.Lock()
	defer mutex.Unlock()

	m.Resumed = append([]kafka.TopicPartition(nil), partitions...)
	return nil
}

func (m *MockKafkaConsumer) Position(partitions []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if len(m.Positions) > 0 {
		return append([]kafka.TopicPartition(nil), m.Positions...), nil
	}
	return append([]kafka.TopicPartition(nil), partitions...), nil
}

func (m *MockKafkaConsumer) GetWatermarkOffsets(topic string, partition int32) (int64, int64, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if m.WatermarkError != nil {
		return 0, 0, m.WatermarkError
	}
	if m.Watermarks != nil {
		return 0, m.Watermarks[topic], nil
	}
	return 0, 0, nil
}

func (m *MockKafkaConsumer) Close() error {
	mutex.Lock()
	defer mutex.Unlock()

	m.CloseCount++
	return nil
}

type MockDLQ struct {
	Messages [][]byte
	Wait     *sync.WaitGroup
}

func NewMockDLQ() *MockDLQ {
	messages := make([][]byte, 0)
	return &MockDLQ{
		Messages: messages,
	}
}

func (m *MockDLQ) WriteMessage(ctx context.Context, message []byte) error {
	mutex.Lock()
	defer mutex.Unlock()

	m.Messages = append(m.Messages, message)
	if m.Wait != nil {
		m.Wait.Done()
	}
	return nil
}

func (m *MockDLQ) ReadMessages(_ context.Context, _ int32) ([][]byte, error) {
	return m.Messages, nil
}

type recordedConsumedMessage struct {
	groupID     string
	topic       string
	partition   int32
	messageType string
}

type recordedConsumerLag struct {
	groupID   string
	topic     string
	partition int32
	lag       int64
}

type metricsRecorder struct {
	mu       sync.Mutex
	consumed []recordedConsumedMessage
	lags     []recordedConsumerLag
}

func (r *metricsRecorder) RecordError(context.Context, metrics.Boundary, string, metrics.ErrorClass, string) {
}

func (r *metricsRecorder) RecordRequest(context.Context, metrics.Boundary, string, string) {
}

func (r *metricsRecorder) RecordDuration(context.Context, metrics.Boundary, string, string, float64) {
}

func (r *metricsRecorder) RecordKafkaLag(context.Context, string, float64) {
}

func (r *metricsRecorder) RecordKafkaMessageConsumed(_ context.Context, groupID, topic string, partition int32, messageType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumed = append(r.consumed, recordedConsumedMessage{
		groupID:     groupID,
		topic:       topic,
		partition:   partition,
		messageType: messageType,
	})
}

func (r *metricsRecorder) RecordKafkaConsumerLag(_ context.Context, groupID, topic string, partition int32, lag int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lags = append(r.lags, recordedConsumerLag{
		groupID:   groupID,
		topic:     topic,
		partition: partition,
		lag:       lag,
	})
}

func (r *metricsRecorder) consumedSnapshot() []recordedConsumedMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedConsumedMessage(nil), r.consumed...)
}

func (r *metricsRecorder) lagSnapshot() []recordedConsumerLag {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedConsumerLag(nil), r.lags...)
}

func backOffFactory() *backoff.ExponentialBackOff {
	expBackoff := backoff.NewExponentialBackOff(
		backoff.WithMaxInterval(0 * time.Millisecond),
	)
	return expBackoff
}

func newProto(data string) *wrapperspb.StringValue {
	return wrapperspb.String(data)
}

func shardForKey(key uuid.UUID, numShards int) int {
	h := fnv.New32a()
	_, _ = h.Write(key[:])
	return int(h.Sum32()) % numShards
}

func findResourceKeyForShard(targetShard, numShards int) uuid.UUID {
	for {
		key := uuid.New()
		if shardForKey(key, numShards) == targetShard {
			return key
		}
	}
}

type TestListener[T proto.Message] struct {
	mu                  sync.Mutex
	NextErrors          []error
	NextMsgType         messaging.MsgType
	NewMessageFn        func() T
	ExpectedResourceKey uuid.UUID
	ExpectedPayload     T
	PanicWith           any

	CallCount        int
	ReceivedKeys     []uuid.UUID
	ReceivedPayloads []T

	OnHandle func(resourceKey uuid.UUID, payload T)
}

func (l *TestListener[T]) MsgType() messaging.MsgType {
	return l.NextMsgType
}

func (l *TestListener[T]) NewMessage() T {
	if l.NewMessageFn != nil {
		return l.NewMessageFn()
	}
	var zero T
	return zero
}
func (l *TestListener[T]) Handle(ctx context.Context, resourceKey uuid.UUID, payload T) error {
	l.mu.Lock()
	l.CallCount++
	l.ReceivedKeys = append(l.ReceivedKeys, resourceKey)
	l.ReceivedPayloads = append(l.ReceivedPayloads, payload)
	expectedResourceKey := l.ExpectedResourceKey
	expectedPayload := l.ExpectedPayload
	onHandle := l.OnHandle
	panicWith := l.PanicWith
	var err error
	if len(l.NextErrors) > 0 {
		err = l.NextErrors[0]
		l.NextErrors = l.NextErrors[1:]
	}
	l.mu.Unlock()

	if panicWith != nil {
		panic(panicWith)
	}

	if expectedResourceKey != uuid.Nil {
		Expect(resourceKey).To(Equal(expectedResourceKey))
	}
	var zero T
	if !reflect.DeepEqual(expectedPayload, zero) {
		Expect(payload).To(Equal(expectedPayload))
	}

	if onHandle != nil {
		onHandle(resourceKey, payload)
	}
	return err
}

var testTopic = "test-topic-" + uuid.New().String()

var _ = Describe("Message subscribe client", Ordered, func() {
	var (
		mockKafkaConsumer *MockKafkaConsumer
		mockDLQ           *MockDLQ
		sub               messaging.Subscriber
		topics            []string
		testTopicMsgType  messaging.MsgType
		cancelCtx         context.Context
		cancelFtn         context.CancelFunc
		messageTest       messaging.Message
		ctx               context.Context
		resourceKey       uuid.UUID
	)

	Context("when consuming events from Kafka", Ordered, func() {

		BeforeAll(func() {
			topics = []string{testTopic}
			testTopicMsgType = messaging.MsgTypeUserCreated
			resourceKey = uuid.New()
		})

		BeforeEach(func() {
			mockKafkaConsumer = NewMockKafkaConsumer()
			mockDLQ = NewMockDLQ()
			sub = messaging.NewTestSubscriber(
				mockKafkaConsumer,
				mockDLQ,
				backOffFactory,
				messaging.DefaultNumShards,
				messaging.DefaultChannelBuffer,
			)

			ctx = context.Background()
			cancelCtx, cancelFtn = context.WithCancel(context.Background())
			messageTest = messaging.Message{
				ResourceKey: resourceKey,
				MsgType:     messaging.MsgTypeUserCreated,
				Payload:     []byte("should-be-overwritten"),
			}
		})

		AfterEach(func() {
			cancelFtn()
		})

		It("should return an error when subscribing to topics fails", func() {
			mockKafkaConsumer.SubscribeError = errors.New("subscribe-error")
			err := sub.Subscribe(cancelCtx, topics)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("failed to subscribe to topics"))
		})

		It("should track assignments from subscriber-owned rebalance handling", func() {
			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			assignment := []kafka.TopicPartition{{Topic: &testTopic, Partition: 0}}
			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Eventually(func() int {
				return reporter.Health().AssignedPartitions
			}, time.Second, 10*time.Millisecond).Should(Equal(1))

			mockKafkaConsumer.WriteEvent(kafka.RevokedPartitions{Partitions: assignment})
			Eventually(func() int {
				return reporter.Health().AssignedPartitions
			}, time.Second, 10*time.Millisecond).Should(Equal(0))
		})

		It("should keep consuming after nonfatal Kafka consumer errors", func() {
			done := make(chan error, 1)
			go func() {
				done <- sub.Subscribe(cancelCtx, topics)
			}()

			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Eventually(func() int {
				return reporter.Health().AssignedPartitions
			}, time.Second, 10*time.Millisecond).Should(Equal(1))

			consumerErrors := []struct {
				code    kafka.ErrorCode
				message string
			}{
				{code: kafka.ErrUnknownPartition, message: "desired partition is no longer available"},
				{code: kafka.ErrUnknownTopicOrPart, message: "Subscribed topic not available: feature_materializer"},
				{code: kafka.ErrAssignmentLost, message: "assignment lost"},
			}

			for _, consumerError := range consumerErrors {
				mockKafkaConsumer.WriteEvent(kafka.NewError(consumerError.code, consumerError.message, false))
				Eventually(func() string {
					return reporter.Health().LastTransientError
				}, time.Second, 10*time.Millisecond).Should(ContainSubstring(consumerError.message))
			}
			Expect(reporter.Health().LastError).To(BeEmpty())
			Consistently(done, 200*time.Millisecond).ShouldNot(Receive())

			cancelFtn()
			Eventually(done, time.Second, 10*time.Millisecond).Should(Receive(Equal(context.Canceled)))
		})

		It("should stop consuming after fatal Kafka consumer errors", func() {
			done := make(chan error, 1)
			go func() {
				done <- sub.Subscribe(cancelCtx, topics)
			}()

			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Eventually(func() int {
				return reporter.Health().AssignedPartitions
			}, time.Second, 10*time.Millisecond).Should(Equal(1))

			mockKafkaConsumer.WriteEvent(kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", true))

			Eventually(done, time.Second, 10*time.Millisecond).Should(Receive(MatchError(ContainSubstring("all brokers down"))))
		})

		It("should drop queued messages from revoked assignments without exposing rebalance handling to listeners", func() {
			mockKafkaConsumer = NewMockKafkaConsumer()
			mockDLQ = NewMockDLQ()
			sub = messaging.NewTestSubscriber(
				mockKafkaConsumer,
				mockDLQ,
				backOffFactory,
				1,
				1,
			)

			handlerStarted := make(chan struct{})
			releaseHandler := make(chan struct{})
			extraCall := make(chan struct{}, 1)
			var handlerMu sync.Mutex
			firstCall := true

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					handlerMu.Lock()
					isFirst := firstCall
					firstCall = false
					handlerMu.Unlock()

					if isFirst {
						close(handlerStarted)
						<-releaseHandler
						return
					}

					select {
					case extraCall <- struct{}{}:
					default:
					}
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Eventually(func() int {
				return reporter.Health().AssignedPartitions
			}, time.Second, 10*time.Millisecond).Should(Equal(1))

			for i := 0; i < 3; i++ {
				payload := newProto("revoked-backlog")
				bytes, err := messageTest.Serialize(ctx, payload)
				Expect(err).ToNot(HaveOccurred())
				mockKafkaConsumer.WriteEvent(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(i + 1)},
					Value:          bytes,
				})
			}

			<-handlerStarted
			Eventually(func() int {
				return reporter.Health().BacklogDepth
			}, time.Second, 10*time.Millisecond).Should(BeNumerically(">", 0))

			assignment := []kafka.TopicPartition{{Topic: &testTopic, Partition: 0}}
			mockKafkaConsumer.WriteEvent(kafka.RevokedPartitions{Partitions: assignment})

			Eventually(func() int {
				return reporter.Health().AssignedPartitions
			}, time.Second, 10*time.Millisecond).Should(Equal(0))
			Eventually(func() int {
				return reporter.Health().BacklogDepth
			}, time.Second, 10*time.Millisecond).Should(Equal(0))

			close(releaseHandler)

			Consistently(extraCall, 200*time.Millisecond).ShouldNot(Receive())
			mutex.Lock()
			defer mutex.Unlock()
			Expect(mockKafkaConsumer.CommitCount).To(Equal(0))
		})

		It("should call the handler to process events successfully", func() {
			var wg sync.WaitGroup
			wg.Add(2)
			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },

				ExpectedResourceKey: messageTest.ResourceKey,
				OnHandle: func(_ uuid.UUID, payload *wrapperspb.StringValue) {
					defer wg.Done()
					if payload.Value == "test-message-1" {
						Expect(payload.Value).To(Equal("test-message-1"))
					} else if payload.Value == "test-message-2" {
						Expect(payload.Value).To(Equal("test-message-2"))
					} else {
						Fail("unexpected payload")
					}
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			var err error
			payload1 := newProto("test-message-1")
			messageTest.Payload, err = proto.Marshal(payload1)
			Expect(err).ToNot(HaveOccurred())
			bytes1, err := messageTest.Serialize(ctx, payload1)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 1},
				Value:          bytes1,
			})

			payload2 := newProto("test-message-2")
			messageTest.Payload, err = proto.Marshal(payload2)
			Expect(err).ToNot(HaveOccurred())

			bytes2, err := messageTest.Serialize(ctx, payload2)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 2},
				Value:          bytes2,
			})

			wg.Wait()

			mutex.Lock()
			defer mutex.Unlock()

			Expect(listener.CallCount).Should(Equal(2))
		})

		It("should handle a message validation error and add the message to the DLQ", func() {
			var wg sync.WaitGroup
			wg.Add(1) // DLQ write
			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					Fail("handler should not be called on invalid message")
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			// Invalid payload: not a JSON/proto Message
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 5},
				Value:          []byte("not a valid message"),
			})

			go func() {
				defer wg.Done()
				time.Sleep(1 * time.Second)
			}()
			wg.Wait()

			mutex.Lock()
			defer mutex.Unlock()

			Expect(len(mockDLQ.Messages)).To(Equal(1))
			Expect(string(mockDLQ.Messages[0])).To(Equal("not a valid message"))
			Expect(listener.CallCount).To(Equal(0))
		})

		It("should drop the message when no handler is registered for the event", func() {
			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("test-message-1")
			var err error
			messageTest.Payload, err = proto.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())

			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 5},
				Value:          bytes,
			})

			go func() {
				defer wg.Done()
				time.Sleep(1 * time.Second)
			}()
			wg.Wait()

			mutex.Lock()
			defer mutex.Unlock()
			Expect(len(mockDLQ.Messages)).To(Equal(0))
			Expect(mockKafkaConsumer.CommitCount).To(Equal(1))
		})

		It("should handle a handler validation error and add the message to the DLQ", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				NextErrors: []error{
					messaging.NonRetryable(errors.New("validation failed")),
				},
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
				},
			}
			mockDLQ.Wait = &wg
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("test-message-1")
			var err error
			messageTest.Payload, err = proto.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())

			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 5},
				Value:          bytes,
			})

			wg.Wait()

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 500*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			mutex.Lock()
			defer mutex.Unlock()

			Expect(len(mockDLQ.Messages)).To(Equal(1))
			Expect(mockDLQ.Messages[0]).To(Equal(bytes))
		})

		It("should handle a handler duplicate error and return", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			mockKafkaConsumer.WaitGroup = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				NextErrors: []error{
					messaging.AlreadyProcessed(errors.New("already exists")),
				},
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("test-message-1")
			var err error
			messageTest.Payload, err = proto.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 5},
				Value:          bytes,
			})

			wg.Wait()

			mutex.Lock()
			defer mutex.Unlock()

			Expect(mockDLQ.Messages).To(BeEmpty())
			Expect(mockKafkaConsumer.CommitCount).To(Equal(1))
		})

		It("should DLQ and commit non-retryable handler errors without replaying", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			mockDLQ.Wait = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				NextErrors: []error{
					messaging.NonRetryable(errors.New("deterministic service error")),
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("non-retryable")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(6)},
				Value:          bytes,
			})

			wg.Wait()

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 500*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			mutex.Lock()
			defer mutex.Unlock()
			Expect(listener.CallCount).To(Equal(1))
			Expect(mockDLQ.Messages).To(HaveLen(1))
		})

		It("should DLQ and commit when AddListener cannot deserialize the payload", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			mockDLQ.Wait = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			// A garbage message body that the typed listener cannot decode.
			messageTest.Payload = []byte{0xff, 0xff, 0xff}
			bytes, err := messageTest.Serialize(ctx, nil)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(20)},
				Value:          bytes,
			})

			wg.Wait()

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 500*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			mutex.Lock()
			defer mutex.Unlock()
			Expect(listener.CallCount).To(Equal(0)) // Handle never invoked
			Expect(mockDLQ.Messages).To(HaveLen(1))
		})

		It("should DLQ and commit when the typed Handle panics", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			mockDLQ.Wait = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				PanicWith:    "boom from Handle",
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("panic-in-handle")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(21)},
				Value:          bytes,
			})

			wg.Wait()

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 500*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			mutex.Lock()
			defer mutex.Unlock()
			Expect(listener.CallCount).To(Equal(1))
			Expect(mockDLQ.Messages).To(HaveLen(1))
		})

		It("should DLQ and commit when a raw RegisterListener handler panics", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			mockDLQ.Wait = &wg

			var rawCallCount int
			sub.RegisterListener(testTopicMsgType, func(ctx context.Context, msg messaging.Message) error {
				mutex.Lock()
				rawCallCount++
				mutex.Unlock()
				panic("boom from raw RegisterListener handler")
			})

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("panic-in-raw-handler")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(22)},
				Value:          bytes,
			})

			wg.Wait()

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 500*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			mutex.Lock()
			defer mutex.Unlock()
			Expect(rawCallCount).To(Equal(1))
			Expect(mockDLQ.Messages).To(HaveLen(1))
		})

		It("should not report offset progress when a DLQ write succeeds but commit fails", func() {
			var wg sync.WaitGroup
			wg.Add(messaging.MaxCommitAttempts)
			mockKafkaConsumer.WaitGroup = &wg
			mockKafkaConsumer.CommitError = errors.New("commit failed")

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				NextErrors: []error{
					messaging.NonRetryable(errors.New("deterministic service error")),
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				_ = sub.Subscribe(cancelCtx, topics)
			}()

			reporter := sub.(messaging.SubscriberHealthReporter)
			Eventually(func() bool {
				return !reporter.Health().LastProgressAt.IsZero()
			}, 500*time.Millisecond, 10*time.Millisecond).Should(BeTrue())
			startedProgressAt := reporter.Health().LastProgressAt

			payload := newProto("non-retryable-commit-failure")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(8)},
				Value:          bytes,
			})

			wg.Wait()

			health := reporter.Health()
			Expect(health.MessagesDLQ).To(Equal(uint64(1)))
			Expect(health.MessagesCommitted).To(Equal(uint64(0)))
			Expect(health.LastCommitAt.IsZero()).To(BeTrue())
			Expect(health.LastProgressAt.Equal(startedProgressAt)).To(BeTrue())
		})

		It("should let services inject non-retryable error classification", func() {
			sentinel := errors.New("service deterministic error")
			Expect(messaging.ConfigureErrorPolicy(sub, messaging.ErrorPolicyFunc(func(err error) bool {
				return errors.Is(err, sentinel)
			}))).To(Succeed())

			var wg sync.WaitGroup
			wg.Add(1)
			mockDLQ.Wait = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				NextErrors:   []error{sentinel},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("service-classified-non-retryable")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(7)},
				Value:          bytes,
			})

			wg.Wait()

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 500*time.Millisecond, 10*time.Millisecond).Should(Equal(1))

			mutex.Lock()
			defer mutex.Unlock()
			Expect(listener.CallCount).To(Equal(1))
			Expect(mockDLQ.Messages).To(HaveLen(1))
		})

		It("should only classify explicitly marked failures as non-retryable by default", func() {
			err := errors.New("deterministic service error")
			Expect(messaging.IsNonRetryable(err)).To(BeFalse())
			Expect(messaging.IsNonRetryable(messaging.NonRetryable(err))).To(BeTrue())
		})

		It("should report unhealthy subscribers with no assignment, stale polls, or lag", func() {
			now := time.Now()
			healthy := messaging.SubscriberHealth{
				GroupID:            "feature-materializer-group",
				Topics:             []string{"feature_materializer"},
				Started:            true,
				AssignedPartitions: 1,
				LastPollAt:         now,
				LastProgressAt:     now,
				MaxLag:             10,
				LagByTopic:         map[string]int64{"feature_materializer": 10},
			}
			cfg := messaging.SubscriberHealthCheckConfig{
				RequireAssignment:  true,
				MaxPollSilence:     time.Second,
				MaxProgressSilence: time.Second,
				MaxLag:             100,
			}
			Expect(healthy.Check(context.Background(), cfg)).To(Succeed())

			canceledCtx, cancel := context.WithCancel(context.Background())
			cancel()
			Expect(healthy.Check(canceledCtx, cfg)).To(MatchError(context.Canceled))
			Expect(messaging.CheckSubscriberHealth(canceledCtx, sub, cfg)).To(MatchError(context.Canceled))

			noAssignment := healthy
			noAssignment.AssignedPartitions = 0
			Expect(noAssignment.Check(context.Background(), cfg)).To(MatchError(ContainSubstring("no active message broker assignment")))

			stalePoll := healthy
			stalePoll.LastPollAt = now.Add(-2 * time.Second)
			Expect(stalePoll.Check(context.Background(), cfg)).To(MatchError(ContainSubstring("has not polled message broker")))

			lagged := healthy
			lagged.MaxLag = 101
			lagged.LagByTopic = map[string]int64{"feature_materializer": 101}
			Expect(lagged.Check(context.Background(), cfg)).To(MatchError(ContainSubstring("exceeds threshold")))
		})

		It("records committed throughput and true offset lag metrics with consumer group labels", func() {
			originalMetrics := metrics.Default()
			recorder := &metricsRecorder{}
			metrics.SetDefault(recorder)
			DeferCleanup(func() {
				metrics.SetDefault(originalMetrics)
			})

			mockKafkaConsumer.Positions = []kafka.TopicPartition{{
				Topic:     &testTopic,
				Partition: 0,
				Offset:    kafka.Offset(5),
			}}
			mockKafkaConsumer.Watermarks = map[string]int64{testTopic: 12}

			var wg sync.WaitGroup
			wg.Add(1)
			mockKafkaConsumer.WaitGroup = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:         testTopicMsgType,
				NewMessageFn:        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				ExpectedResourceKey: messageTest.ResourceKey,
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("metrics-test")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(5)},
				Value:          bytes,
				Timestamp:      time.Now(),
			})

			wg.Wait()

			Eventually(func() []recordedConsumedMessage {
				return recorder.consumedSnapshot()
			}, 500*time.Millisecond, 10*time.Millisecond).Should(ContainElement(recordedConsumedMessage{
				groupID:     "test-subscriber",
				topic:       testTopic,
				partition:   0,
				messageType: testTopicMsgType.String(),
			}))

			Eventually(func() []recordedConsumerLag {
				return recorder.lagSnapshot()
			}, 500*time.Millisecond, 10*time.Millisecond).Should(ContainElement(recordedConsumerLag{
				groupID:   "test-subscriber",
				topic:     testTopic,
				partition: 0,
				lag:       7,
			}))
		})

		It("should not return an error when committing a message fails (logs and continues)", func() {
			var wg sync.WaitGroup
			wg.Add(3)
			mockKafkaConsumer.WaitGroup = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:         testTopicMsgType,
				NewMessageFn:        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				ExpectedResourceKey: messageTest.ResourceKey,
				OnHandle: func(resourceKey uuid.UUID, payload *wrapperspb.StringValue) {
					Expect(resourceKey).To(Equal(messageTest.ResourceKey))
					Expect(payload.Value).To(Equal("test-message-1"))
				},
			}
			messaging.AddListener(sub, listener)
			mockKafkaConsumer.CommitError = errors.New("commit-error")

			go func() {
				_ = sub.Subscribe(cancelCtx, topics)
			}()

			payload := newProto("test-message-1")
			var err error
			messageTest.Payload, err = proto.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())

			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: 5},
				Value:          bytes,
			})

			wg.Wait()

			mutex.Lock()
			defer mutex.Unlock()

			Expect(listener.CallCount).Should(Equal(1))
			Expect(mockKafkaConsumer.CommitCount).Should(Equal(messaging.MaxCommitAttempts))
		})

		It("should not commit offsets out of order within the same partition", func() {
			mockKafkaConsumer = NewMockKafkaConsumer()
			mockDLQ = NewMockDLQ()
			sub = messaging.NewTestSubscriber(
				mockKafkaConsumer,
				mockDLQ,
				backOffFactory,
				2,
				10,
			)

			firstKey := findResourceKeyForShard(0, 2)
			secondKey := findResourceKeyForShard(1, 2)

			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondDone := make(chan struct{})

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				OnHandle: func(resourceKey uuid.UUID, payload *wrapperspb.StringValue) {
					switch payload.Value {
					case "first":
						close(firstStarted)
						<-releaseFirst
					case "second":
						Expect(resourceKey).To(Equal(secondKey))
						close(secondDone)
					default:
						Fail("unexpected payload")
					}
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			firstPayload := newProto("first")
			firstMsg := messaging.Message{
				ResourceKey: firstKey,
				MsgType:     testTopicMsgType,
			}
			firstBytes, err := firstMsg.Serialize(ctx, firstPayload)
			Expect(err).ToNot(HaveOccurred())

			secondPayload := newProto("second")
			secondMsg := messaging.Message{
				ResourceKey: secondKey,
				MsgType:     testTopicMsgType,
			}
			secondBytes, err := secondMsg.Serialize(ctx, secondPayload)
			Expect(err).ToNot(HaveOccurred())

			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(1)},
				Value:          firstBytes,
			})
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(2)},
				Value:          secondBytes,
			})

			<-firstStarted
			<-secondDone

			Consistently(func() []kafka.Offset {
				mutex.Lock()
				defer mutex.Unlock()
				return append([]kafka.Offset(nil), mockKafkaConsumer.CommitOffsets...)
			}, 200*time.Millisecond, 20*time.Millisecond).Should(BeEmpty())

			close(releaseFirst)

			Eventually(func() []kafka.Offset {
				mutex.Lock()
				defer mutex.Unlock()
				return append([]kafka.Offset(nil), mockKafkaConsumer.CommitOffsets...)
			}, time.Second, 20*time.Millisecond).Should(Equal([]kafka.Offset{kafka.Offset(1), kafka.Offset(2)}))
		})

		It("should pause assigned partitions instead of blocking polling when shard queues are full", func() {
			mockKafkaConsumer = NewMockKafkaConsumer()
			mockDLQ = NewMockDLQ()
			sub = messaging.NewTestSubscriber(
				mockKafkaConsumer,
				mockDLQ,
				backOffFactory,
				1,
				1,
			)

			handlerStarted := make(chan struct{})
			releaseHandler := make(chan struct{})
			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:  testTopicMsgType,
				NewMessageFn: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					select {
					case <-handlerStarted:
					default:
						close(handlerStarted)
					}
					<-releaseHandler
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			assignment := []kafka.TopicPartition{{Topic: &testTopic, Partition: 0}}
			mockKafkaConsumer.WriteEvent(kafka.AssignedPartitions{Partitions: assignment})

			for i := 0; i < 3; i++ {
				payload := newProto("backpressure")
				bytes, err := messageTest.Serialize(ctx, payload)
				Expect(err).ToNot(HaveOccurred())
				mockKafkaConsumer.WriteEvent(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(i + 1)},
					Value:          bytes,
				})
			}

			<-handlerStarted

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return len(mockKafkaConsumer.Paused)
			}, time.Second, 10*time.Millisecond).Should(Equal(1))

			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Eventually(func() int {
				return reporter.Health().BacklogDepth
			}, time.Second, 10*time.Millisecond).Should(BeNumerically(">", 0))

			close(releaseHandler)
		})

		It("should DLQ and commit after max replay attempts (and keep running)", func() {
			var wg sync.WaitGroup
			wg.Add(6) // 1 initial + 3 replays + 1 commit + 1 sleep goroutine
			mockKafkaConsumer.WaitGroup = &wg

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:         testTopicMsgType,
				NewMessageFn:        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				ExpectedResourceKey: messageTest.ResourceKey,
				NextErrors: []error{
					errors.New("first random error"),
					errors.New("second random error"),
					errors.New("third random error"),
					errors.New("fourth random error"), // This will trigger the max replay attempts error
				},
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					defer wg.Done()
				},
			}

			go func() {
				messaging.AddListener(sub, listener)
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).ToNot(BeNil())
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("test-message-1")
			var err error
			messageTest.Payload, err = proto.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())

			// Write the message once - replays happen internally via shard channel, not by re-reading from Kafka
			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(4)},
				Value:          bytes,
			})

			go func() {
				defer wg.Done()
				time.Sleep(1 * time.Second) // wait for backoff
			}()

			wg.Wait()

			mutex.Lock()
			defer mutex.Unlock()

			Expect(listener.CallCount).Should(Equal(messaging.MaxReplayAttempts + 1))
			Expect(len(mockDLQ.Messages)).To(Equal(1))
			Expect(mockKafkaConsumer.CommitCount).To(Equal(1))

			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Expect(reporter.Health().LastError).To(ContainSubstring("fourth random error"))
		})

		It("should not return an error after replay succeeds on the second attempt", func() {
			var wg sync.WaitGroup
			wg.Add(3) // 2 handler calls (1 fail + 1 retry success) + 1 sleep goroutine

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:         testTopicMsgType,
				NewMessageFn:        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				ExpectedResourceKey: messageTest.ResourceKey,
				NextErrors: []error{
					errors.New("random error"), // first call
					nil,                        // second call (retry succeeds)
				},
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					defer wg.Done()
				},
			}

			go func() {
				messaging.AddListener(sub, listener)
				err := sub.Subscribe(cancelCtx, topics)
				Expect(err).To(Equal(context.Canceled))
			}()

			payload := newProto("test-message-1")
			var err error
			messageTest.Payload, err = proto.Marshal(payload)
			Expect(err).ToNot(HaveOccurred())
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())

			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic},
				Value:          bytes,
			})

			go func() {
				defer wg.Done()
				time.Sleep(1 * time.Second)
			}()

			wg.Wait()
			mutex.Lock()
			defer mutex.Unlock()

			Expect(listener.CallCount).Should(Equal(2))

			reporter, ok := sub.(messaging.SubscriberHealthReporter)
			Expect(ok).To(BeTrue())
			Expect(reporter.Health().LastError).To(ContainSubstring("random error"))
		})

		It("should commit a completed message during shutdown", func() {
			mockKafkaConsumer = NewMockKafkaConsumer()
			mockDLQ = NewMockDLQ()
			sub = messaging.NewTestSubscriber(
				mockKafkaConsumer,
				mockDLQ,
				backOffFactory,
				1,
				10,
			)

			handlerStarted := make(chan struct{})
			releaseHandler := make(chan struct{})
			subscribeDone := make(chan error, 1)

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:         testTopicMsgType,
				NewMessageFn:        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				ExpectedResourceKey: messageTest.ResourceKey,
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					close(handlerStarted)
					<-releaseHandler
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				subscribeDone <- sub.Subscribe(cancelCtx, topics)
			}()

			payload := newProto("shutdown-commit")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())

			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(7)},
				Value:          bytes,
			})

			<-handlerStarted
			cancelFtn()
			close(releaseHandler)

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, time.Second, 20*time.Millisecond).Should(Equal(1))

			Eventually(subscribeDone, time.Second).Should(Receive(Equal(context.Canceled)))
		})

		It("should not retry commit when shutdown context is canceled", func() {
			mockKafkaConsumer = NewMockKafkaConsumer()
			mockKafkaConsumer.CommitError = errors.New("Operation not allowed on closed client")
			mockDLQ = NewMockDLQ()
			sub = messaging.NewTestSubscriber(
				mockKafkaConsumer,
				mockDLQ,
				backOffFactory,
				1,
				10,
			)

			handlerStarted := make(chan struct{})
			releaseHandler := make(chan struct{})
			subscribeDone := make(chan error, 1)

			listener := &TestListener[*wrapperspb.StringValue]{
				NextMsgType:         testTopicMsgType,
				NewMessageFn:        func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				ExpectedResourceKey: messageTest.ResourceKey,
				OnHandle: func(_ uuid.UUID, _ *wrapperspb.StringValue) {
					close(handlerStarted)
					<-releaseHandler
				},
			}
			messaging.AddListener(sub, listener)

			go func() {
				subscribeDone <- sub.Subscribe(cancelCtx, topics)
			}()

			payload := newProto("shutdown-no-retry")
			bytes, err := messageTest.Serialize(ctx, payload)
			Expect(err).ToNot(HaveOccurred())

			mockKafkaConsumer.WriteEvent(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &testTopic, Partition: 0, Offset: kafka.Offset(8)},
				Value:          bytes,
			})

			<-handlerStarted
			cancelFtn()
			close(releaseHandler)

			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, time.Second, 20*time.Millisecond).Should(Equal(1))

			Consistently(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return mockKafkaConsumer.CommitCount
			}, 200*time.Millisecond, 20*time.Millisecond).Should(Equal(1))

			Eventually(subscribeDone, time.Second).Should(Receive(Equal(context.Canceled)))
		})
	})
})
