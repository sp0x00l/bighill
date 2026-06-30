package cmd

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var topicsListCmd = &cobra.Command{
	Use:   "topics-list",
	Short: "List all Kafka topics",
	Run: func(cmd *cobra.Command, args []string) {
		topicsList()
	},
}

func topicsList() {
	conf := &kafka.ConfigMap{"bootstrap.servers": brokers}

	adminClient, err := kafka.NewAdminClient(conf)
	if err != nil {
		log.Fatalf("Failed to create Admin client: %s", err)
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		log.Fatalf("Failed to get metadata: %s", err)
	}

	log.Info("====================================================")
	log.Info("Kafka Topics:")
	log.Info("====================================================")
	for topicName, topic := range metadata.Topics {
		if topic.Error.Code() != kafka.ErrNoError {
			log.Errorf("Error in topic %s: %v", topicName, topic.Error)
			continue
		}
		log.Infof(" - Topic: %s", topicName)
		for _, partition := range topic.Partitions {
			log.Infof("   - Partition ID: %d, Leader: %d, Replicas: %v, In Sync Replicas: %v",
				partition.ID, partition.Leader, partition.Replicas, partition.Isrs)
		}
	}
}
