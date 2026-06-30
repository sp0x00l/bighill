package messaging

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
)

const (
	defaultNumPartitions     = 3
	defaultReplicationFactor = 1
	adminTimeout             = 30 * time.Second
)

// CreateTopic creates a Kafka topic if it doesn't already exist.
// It's safe to call even if the topic exists - TopicAlreadyExists is not treated as an error.
func CreateTopic(ctx context.Context, brokers string, topic string) error {
	return CreateTopicWithConfig(ctx, brokers, topic, defaultNumPartitions, defaultReplicationFactor)
}

// CreateTopicWithConfig creates a Kafka topic with specified partitions and replication factor.
func CreateTopicWithConfig(ctx context.Context, brokers string, topic string, numPartitions int, replicationFactor int) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		log.WithError(err).WithField("topic", topic).Error("failed to create Kafka admin client")
		return err
	}
	defer admin.Close()

	topicSpec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec},
		kafka.SetAdminOperationTimeout(adminTimeout))
	if err != nil {
		log.WithError(err).WithField("topic", topic).Error("failed to create topic")
		return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.WithField("topic", result.Topic).WithField("error_code", result.Error.Code()).Error("topic creation failed")
			return result.Error
		}
		if result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.WithField("topic", result.Topic).Info("topic created")
		}
	}

	return nil
}

// CreateTopics creates multiple Kafka topics. Existing topics are skipped.
func CreateTopics(ctx context.Context, brokers string, topics []string) error {
	if len(topics) == 0 {
		return nil
	}

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		log.WithError(err).Error("failed to create Kafka admin client")
		return err
	}
	defer admin.Close()

	specs := make([]kafka.TopicSpecification, len(topics))
	for i, topic := range topics {
		specs[i] = kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     defaultNumPartitions,
			ReplicationFactor: defaultReplicationFactor,
		}
	}

	results, err := admin.CreateTopics(ctx, specs, kafka.SetAdminOperationTimeout(adminTimeout))
	if err != nil {
		log.WithError(err).WithField("topics", topics).Error("failed to create topics")
		return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.WithField("topic", result.Topic).WithField("error_code", result.Error.Code()).Error("topic creation failed")
			return result.Error
		}
		if result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.WithField("topic", result.Topic).Info("topic created")
		}
	}

	return nil
}
