package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type subscriberConsumerStub struct {
	commits int
}

func (s *subscriberConsumerStub) SubscribeTopics([]string, kafka.RebalanceCb) error {
	return nil
}

func (s *subscriberConsumerStub) Poll(int) kafka.Event {
	return nil
}

func (s *subscriberConsumerStub) CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error) {
	s.commits++
	return nil, nil
}

func (s *subscriberConsumerStub) Seek(kafka.TopicPartition, int) error {
	return nil
}

func (s *subscriberConsumerStub) Assign([]kafka.TopicPartition) error {
	return nil
}

func (s *subscriberConsumerStub) Unassign() error {
	return nil
}

func (s *subscriberConsumerStub) Close() error {
	return nil
}

type subscriberDLQStub struct {
	err      error
	writes   int
	messages [][]byte
}

func (s *subscriberDLQStub) WriteMessage(_ context.Context, message []byte) error {
	s.writes++
	if s.err != nil {
		return s.err
	}
	s.messages = append(s.messages, append([]byte(nil), message...))
	return nil
}

func (s *subscriberDLQStub) ReadMessages(context.Context, int32) ([][]byte, error) {
	return s.messages, nil
}

var _ = Describe("Subscriber DLQ failure handling", func() {
	It("does not commit invalid envelopes when DLQ write fails", func() {
		topic := "events"
		tp := kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 7}
		dlq := &subscriberDLQStub{err: errors.New("sqs down")}
		sub := newSubscriberDLQTestSubject(dlq)

		_, ok, err := sub.prepareMessage(context.Background(), &kafka.Message{
			TopicPartition: tp,
			Value:          []byte("not-json"),
		})

		Expect(ok).To(BeFalse())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("send invalid Kafka message to DLQ"))
		Expect(dlq.writes).To(Equal(MaxCommitAttempts))
		Expect(sub.commitCh).To(BeEmpty())
	})

	It("does not commit non-retryable handler failures when DLQ write fails", func() {
		topic := "events"
		resourceKey := uuid.New()
		tp := kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 9}
		dlq := &subscriberDLQStub{err: errors.New("sqs down")}
		sub := newSubscriberDLQTestSubject(dlq)
		sub.RegisterListener(MsgTypeModelUpdated, func(context.Context, Message) error {
			return NonRetryable(errors.New("bad payload"))
		})

		sub.processMessage(shardedMessage{
			ctx: context.Background(),
			msg: &kafka.Message{
				TopicPartition: tp,
				Value:          []byte(`{"event":"bad"}`),
			},
			message: Message{
				ResourceKey: resourceKey,
				MsgType:     MsgTypeModelUpdated,
				Payload:     []byte("payload"),
			},
			assignment: sub.assignments.Token(tp),
		})

		Expect(dlq.writes).To(Equal(MaxCommitAttempts))
		Expect(sub.commitCh).To(BeEmpty())
		replayKey := fmt.Sprintf("%s-%d-%d", topic, tp.Partition, tp.Offset)
		Expect(sub.ReplayMap[replayKey]).To(Equal(1))

		shardID := sub.getShardID(resourceKey)
		Eventually(sub.shardChannels[shardID]).Should(Receive())
		sub.processingWg.Done()
	})

	It("does not commit max-replay failures when DLQ write fails", func() {
		topic := "events"
		resourceKey := uuid.New()
		tp := kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 11}
		dlq := &subscriberDLQStub{err: errors.New("sqs down")}
		sub := newSubscriberDLQTestSubject(dlq)
		replayKey := fmt.Sprintf("%s-%d-%d", topic, tp.Partition, tp.Offset)
		sub.ReplayMap[replayKey] = MaxReplayAttempts

		err := sub.replayMessageToShard(shardedMessage{
			ctx: context.Background(),
			msg: &kafka.Message{
				TopicPartition: tp,
				Value:          []byte(`{"event":"bad"}`),
			},
			message: Message{
				ResourceKey: resourceKey,
				MsgType:     MsgTypeModelUpdated,
				Payload:     []byte("payload"),
			},
			assignment: sub.assignments.Token(tp),
		}, errors.New("database unavailable"))

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("send max-replay Kafka message to DLQ"))
		Expect(dlq.writes).To(Equal(MaxCommitAttempts))
		Expect(sub.commitCh).To(BeEmpty())
		Expect(sub.ReplayMap[replayKey]).To(Equal(MaxReplayAttempts))
	})
})

func newSubscriberDLQTestSubject(dlq *subscriberDLQStub) *subscriber {
	testSubscriber := NewTestSubscriber(&subscriberConsumerStub{}, dlq, fastSubscriberBackoff, 1, 2)
	sub, ok := testSubscriber.(*subscriber)
	if !ok {
		panic("NewTestSubscriber did not return *subscriber")
	}
	topic := "events"
	sub.assignments.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: 0}})
	return sub
}

func fastSubscriberBackoff() *backoff.ExponentialBackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = time.Nanosecond
	expBackoff.MaxInterval = time.Nanosecond
	expBackoff.Multiplier = 1
	expBackoff.RandomizationFactor = 0
	expBackoff.Reset()
	return expBackoff
}
