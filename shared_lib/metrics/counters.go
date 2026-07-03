package metrics

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Counters struct {
	errors                metric.Int64Counter
	requests              metric.Int64Counter
	durations             metric.Float64Histogram
	kafkaLag              metric.Float64Histogram
	kafkaMessagesConsumed metric.Int64Counter
	kafkaConsumerLag      metric.Int64Gauge
}

type Recorder interface {
	RecordError(ctx context.Context, boundary Boundary, operation string, class ErrorClass, status string)
	RecordRequest(ctx context.Context, boundary Boundary, operation string, status string)
	RecordDuration(ctx context.Context, boundary Boundary, operation string, status string, seconds float64)
	RecordKafkaLag(ctx context.Context, topic string, seconds float64)
	RecordKafkaMessageConsumed(ctx context.Context, groupID, topic string, partition int32, messageType string)
	RecordKafkaConsumerLag(ctx context.Context, groupID, topic string, partition int32, lag int64)
}

var (
	defaultRecorder Recorder
	defaultOnce     sync.Once
)

func Default() Recorder {
	defaultOnce.Do(func() {
		if defaultRecorder == nil {
			defaultRecorder = NewCounters("infra")
		}
	})
	return defaultRecorder
}

func SetDefault(recorder Recorder) {
	if recorder == nil {
		return
	}
	defaultRecorder = recorder
}

func NewCounters(meterName string) *Counters {
	if meterName == "" {
		meterName = "infra"
	}
	meter := otel.Meter(fmt.Sprintf("bighill.%s", meterName))
	return &Counters{
		errors:                MustCounter(meter, "infra_errors_total", "Total infra errors", "infra-fallback"),
		requests:              MustCounter(meter, "infra_requests_total", "Total infra requests", "infra-fallback"),
		durations:             MustHistogram(meter, "infra_request_duration_seconds", "Infra request duration in seconds", "s", "infra-fallback"),
		kafkaLag:              MustHistogram(meter, "kafka_message_lag_seconds", "Kafka message lag in seconds", "s", "infra-fallback"),
		kafkaMessagesConsumed: MustCounter(meter, "kafka_messages_consumed", "Total Kafka messages successfully consumed and committed", "infra-fallback"),
		kafkaConsumerLag:      MustGauge(meter, "kafka_consumer_lag_messages", "Current Kafka consumer group offset lag in messages", "1", "infra-fallback"),
	}
}

func (c *Counters) RecordError(ctx context.Context, boundary Boundary, operation string, class ErrorClass, status string) {
	attrs := baseAttrs(boundary, operation)
	if class == "" {
		class = ErrorClassUnknown
	}
	attrs = append(attrs, attribute.String(labelErrorClass, string(class)))
	if status != "" {
		attrs = append(attrs, attribute.String(labelStatus, status))
	}
	c.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (c *Counters) RecordRequest(ctx context.Context, boundary Boundary, operation string, status string) {
	attrs := baseAttrs(boundary, operation)
	if status != "" {
		attrs = append(attrs, attribute.String(labelStatus, status))
	}
	c.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (c *Counters) RecordDuration(ctx context.Context, boundary Boundary, operation string, status string, seconds float64) {
	attrs := baseAttrs(boundary, operation)
	if status != "" {
		attrs = append(attrs, attribute.String(labelStatus, status))
	}
	c.durations.Record(ctx, seconds, metric.WithAttributes(attrs...))
}

func (c *Counters) RecordKafkaLag(ctx context.Context, topic string, seconds float64) {
	if topic == "" {
		topic = "unknown"
	}
	attrs := []attribute.KeyValue{
		attribute.String("topic", topic),
	}
	c.kafkaLag.Record(ctx, seconds, metric.WithAttributes(attrs...))
}

func (c *Counters) RecordKafkaMessageConsumed(ctx context.Context, groupID, topic string, partition int32, messageType string) {
	attrs := kafkaConsumerAttrs(groupID, topic, partition)
	if messageType == "" {
		messageType = "unknown"
	}
	attrs = append(attrs, attribute.String("message_type", messageType))
	c.kafkaMessagesConsumed.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (c *Counters) RecordKafkaConsumerLag(ctx context.Context, groupID, topic string, partition int32, lag int64) {
	if lag < 0 {
		lag = 0
	}
	c.kafkaConsumerLag.Record(ctx, lag, metric.WithAttributes(kafkaConsumerAttrs(groupID, topic, partition)...))
}

func kafkaConsumerAttrs(groupID, topic string, partition int32) []attribute.KeyValue {
	if groupID == "" {
		groupID = "unknown"
	}
	if topic == "" {
		topic = "unknown"
	}
	return []attribute.KeyValue{
		attribute.String("group_id", groupID),
		attribute.String("topic", topic),
		attribute.Int64("partition", int64(partition)),
	}
}

func baseAttrs(boundary Boundary, operation string) []attribute.KeyValue {
	if boundary == "" {
		boundary = BoundaryExternal
	}
	if operation == "" {
		operation = "unknown"
	}
	return []attribute.KeyValue{
		attribute.String(labelBoundary, string(boundary)),
		attribute.String(labelOperation, operation),
	}
}
