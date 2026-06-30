//go:build kafka

package healthcheck

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
)

func messageBrokerCheck(ctx context.Context, config HealthCheckConfig) error {
	log.Trace("Monitor MessageBroker Healthcheck")

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": config.MessageBrokerConnectionString})
	if err != nil {
		return fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	defer adminClient.Close()

	_, err = adminClient.GetMetadata(nil, true, int(config.MessageBrokerLatencyThresholdSec.Milliseconds()))
	if err != nil {
		return fmt.Errorf("failed to get Kafka metadata: %w", err)
	}

	return nil
}
