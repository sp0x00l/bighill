package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"
	metrics "lib/shared_lib/metrics"
)

const timeoutMillisecond = 15000
const queueFullMaxRetries = 3

type KafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
	Close()
}

type Publisher interface {
	Publish(ctx context.Context, topic string, message Message, payload proto.Message) error
	Close()
}

type RelayPublisher interface {
	PublishOutboxMessage(ctx context.Context, topic string, payload []byte, headers []kafka.Header) error
}

type PublisherOption func(*publisher)

func WithOutbox(outbox OutboxWriter) PublisherOption {
	return func(p *publisher) {
		p.outbox = outbox
	}
}

func WithProducer(producer KafkaProducer) PublisherOption {
	return func(p *publisher) {
		p.Producer = producer
	}
}

type publisher struct {
	Producer  KafkaProducer
	outbox    OutboxWriter
	stopCh    chan struct{}
	closeOnce sync.Once
}

func NewPublisher(brokers string, opts ...PublisherOption) (Publisher, error) {
	log.Trace("NewPublisher")

	p := &publisher{
		stopCh: make(chan struct{}),
	}
	for _, opt := range opts {
		if opt == nil {
			log.Fatal("NewPublisher: nil option")
		}
		opt(p)
	}

	if p.Producer == nil {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":   brokers,
			"enable.idempotence":  true,
			"acks":                "all",
			"delivery.timeout.ms": timeoutMillisecond,
			"linger.ms":           5,      // small latency tradeoff for big throughput gains
			"batch.num.messages":  10000,  // payload sizes and broker limits
			"compression.type":    "zstd", // or lz4/snappy if CPU is tight
		})
		if err != nil {
			metrics.Default().RecordError(context.Background(), metrics.BoundaryKafka, "create_producer", metrics.ClassifyKafka(err), "")
			return nil, err
		}
		p.Producer = producer
	}

	if eventProducer, ok := any(p.Producer).(interface{ Events() chan kafka.Event }); ok {
		go p.drainProducerEvents(eventProducer.Events(), p.stopCh)
	}
	return p, nil
}

func (pc *publisher) Publish(ctx context.Context, topic string, message Message, payload proto.Message) error {
	log.Trace("publisher Publish")
	log.Infof("Publishing message %s to topic: %s", message.MsgType.String(), topic)

	start := time.Now()
	statusLabel := "OK"
	defer func() {
		metrics.Default().RecordDuration(ctx, metrics.BoundaryKafka, "publish", statusLabel, time.Since(start).Seconds())
		metrics.Default().RecordRequest(ctx, metrics.BoundaryKafka, "publish", statusLabel)
	}()
	msgBytes, err := message.Serialize(ctx, payload)
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "publish", metrics.ErrorClassSerialization, "")
		statusLabel = "ERROR"
		log.WithContext(ctx).Errorf("failed to serialize message: %v", err)
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	propagator := otel.GetTextMapPropagator()
	carrier := TraceHeadersCarrier([]kafka.Header{})
	propagator.Inject(ctx, &carrier)
	headers := []kafka.Header(carrier)

	if pc.outbox != nil {
		err := pc.outbox.WriteMessage(ctx, OutboxMessage{
			Topic:   topic,
			Message: message,
			Payload: msgBytes,
			Headers: headers,
		})
		if err != nil {
			metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "publish", metrics.ErrorClassInternal, "")
			statusLabel = "ERROR"
			log.WithContext(ctx).Errorf("failed to enqueue outbox message: %v", err)
			return fmt.Errorf("failed to enqueue outbox message: %w", err)
		}
		log.Infof("Successfully enqueued message to outbox for topic: %s", topic)
		return nil
	}

	errProduce := pc.publishRaw(ctx, topic, msgBytes, headers)
	if errProduce != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "publish", metrics.ClassifyKafka(errProduce), "")
		statusLabel = "ERROR"
		log.WithContext(ctx).Errorf("failed to publish message (%s) to topic: %s", string(msgBytes), topic)
		return fmt.Errorf("failed to publish message to topic: %w", errProduce)
	}

	log.Infof("Successfully published message to topic: %s", topic)
	return nil
}

func (pc *publisher) PublishOutboxMessage(ctx context.Context, topic string, payload []byte, headers []kafka.Header) error {
	log.Trace("publisher PublishOutboxMessage")

	err := pc.publishRaw(ctx, topic, payload, headers)
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "publish_raw", metrics.ClassifyKafka(err), "")
		return fmt.Errorf("failed to publish raw message to topic: %w", err)
	}
	return nil
}

func (pc *publisher) publishRaw(ctx context.Context, topic string, payload []byte, headers []kafka.Header) error {
	log.Trace("publisher publishRaw")

	var envelope Message
	if err := envelope.Deserialize(ctx, payload); err != nil {
		return fmt.Errorf("publish raw message requires a valid message envelope: %w", err)
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Key:            []byte(envelope.ResourceKey.String()),
		Headers:        headers,
	}

	deliveryChan := make(chan kafka.Event, 1)
	errProduce := pc.publishWithRetry(kafkaMsg, deliveryChan)
	if errProduce != nil {
		log.WithContext(ctx).WithError(errProduce).Errorf("failed raw publish to topic: %s", topic)
		return errProduce
	}
	return pc.waitForDelivery(ctx, deliveryChan)
}

func (pc *publisher) publishWithRetry(kafkaMsg *kafka.Message, deliveryChan chan kafka.Event) error {
	var lastErr error
	for attempt := 0; attempt <= queueFullMaxRetries; attempt++ {
		errProduce := pc.Producer.Produce(kafkaMsg, deliveryChan)
		if errProduce == nil {
			return nil
		}
		lastErr = errProduce
		if !isQueueFullError(errProduce) || attempt == queueFullMaxRetries {
			return errProduce
		}
		time.Sleep(time.Duration(25*(attempt+1)) * time.Millisecond)
	}
	return lastErr
}

func (pc *publisher) waitForDelivery(ctx context.Context, deliveryChan <-chan kafka.Event) error {
	timer := time.NewTimer(time.Duration(timeoutMillisecond) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return kafka.NewError(kafka.ErrTimedOut, "delivery report timed out", false)
	case event := <-deliveryChan:
		message, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected kafka delivery event %T", event)
		}
		if message.TopicPartition.Error != nil {
			return fmt.Errorf("kafka delivery failed: %w", message.TopicPartition.Error)
		}
		return nil
	}
}

func isQueueFullError(err error) bool {
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		return kErr.Code() == kafka.ErrQueueFull
	}
	return false
}

func (pc *publisher) drainProducerEvents(events <-chan kafka.Event, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if msg, ok := ev.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
				log.WithError(msg.TopicPartition.Error).Warn("kafka delivery failed")
			}
		}
	}
}

func (pc *publisher) Close() {
	log.Trace("publisher Close")

	pc.closeOnce.Do(func() {
		close(pc.stopCh)
	})
	pc.Producer.Flush(timeoutMillisecond)
	pc.Producer.Close()
}
