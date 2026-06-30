package cmd

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	checkTopic string
)

var topicPartitionsCmd = &cobra.Command{
	Use:   "topic-partitions",
	Short: "Check the availability of topic partitions",
	Run: func(cmd *cobra.Command, args []string) {
		topicPartitions()
	},
}

func init() {
	topicPartitionsCmd.Flags().StringVarP(&checkTopic, "topic", "t", "", "Topic to check partitions for (required)")
	topicPartitionsCmd.MarkFlagRequired("topic")
}

func topicPartitions() {
	conf := &kafka.ConfigMap{"bootstrap.servers": brokers}

	// Create a new Admin client
	adminClient, err := kafka.NewAdminClient(conf)
	if err != nil {
		log.Fatalf("Failed to create Admin client: %s", err)
	}
	defer adminClient.Close()

	// Get metadata for the topic
	metadata, err := adminClient.GetMetadata(&checkTopic, false, 5000)
	if err != nil {
		log.Fatalf("Failed to get metadata for topic %s: %s", checkTopic, err)
	}

	topicMeta, exists := metadata.Topics[checkTopic]
	if !exists || topicMeta.Error.Code() != kafka.ErrNoError {
		log.Errorf("Error in topic %s: %v", checkTopic, topicMeta.Error)
		return
	}

	log.Infof("Topic: %s", checkTopic)
	for _, partition := range topicMeta.Partitions {
		if partition.Error.Code() != kafka.ErrNoError {
			log.Errorf(" - Partition %d: Error: %v", partition.ID, partition.Error)
			continue
		}
		log.Infof(" - Partition %d: Leader: %d, Replicas: %v, ISR: %v", partition.ID, partition.Leader, partition.Replicas, partition.Isrs)
	}
}
