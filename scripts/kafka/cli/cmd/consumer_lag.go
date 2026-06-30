package cmd

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	consumerGroup string
	topicsFilter  string
)

var consumerLagCmd = &cobra.Command{
	Use:   "consumer-lag",
	Short: "Display the number of outstanding messages (lag) for each topic and partition",
	Run: func(cmd *cobra.Command, args []string) {
		consumerLag()
	},
}

func init() {
	consumerLagCmd.Flags().StringVarP(&consumerGroup, "group", "g", "", "Consumer group ID (required)")
	consumerLagCmd.Flags().StringVarP(&topicsFilter, "topics", "t", "", "Comma-separated list of topics to include (optional)")
	consumerLagCmd.MarkFlagRequired("group")
}

func consumerLag() {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}

	adminClient, err := kafka.NewAdminClient(conf)
	if err != nil {
		log.Fatalf("Failed to create Admin client: %s", err)
	}
	defer adminClient.Close()

	var topics []string
	if topicsFilter != "" {
		topics = strings.Split(topicsFilter, ",")
	} else {
		metadata, err := adminClient.GetMetadata(nil, true, 5000)
		if err != nil {
			log.Fatalf("Failed to get metadata: %s", err)
		}
		for topicName, topic := range metadata.Topics {
			if topic.Error.Code() != kafka.ErrNoError {
				log.Errorf("Error in topic %s: %v", topicName, topic.Error)
				continue
			}
			topics = append(topics, topicName)
		}
	}

	if len(topics) == 0 {
		log.Warn("No topics found to calculate lag.")
		return
	}

	// Get partitions for the topics
	var partitions []kafka.TopicPartition
	for _, topic := range topics {
		// Get metadata for the topic
		metadata, err := adminClient.GetMetadata(&topic, false, 5000)
		if err != nil {
			log.Errorf("Failed to get metadata for topic %s: %s", topic, err)
			continue
		}
		topicMeta, exists := metadata.Topics[topic]
		if !exists || topicMeta.Error.Code() != kafka.ErrNoError {
			log.Errorf("Error in topic %s: %v", topic, topicMeta.Error)
			continue
		}

		for _, partition := range topicMeta.Partitions {
			partitions = append(partitions, kafka.TopicPartition{
				Topic:     &topic,
				Partition: partition.ID,
			})
		}
	}

	if len(partitions) == 0 {
		log.Warn("No partitions found to calculate lag.")
		return
	}

	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           consumerGroup,
		"enable.auto.commit": false, // IMPORTANT: Ensure auto commit is disabled, WE MUST NOT COMMIT OFFSETS
	}
	consumer, err := kafka.NewConsumer(consumerConf)
	if err != nil {
		log.Fatalf("Failed to create Consumer: %s", err)
	}
	defer consumer.Close()

	committedTopicPartitionOffsets, err := consumer.Committed(partitions, 5000)
	if err != nil {
		log.Fatalf("Failed to get committed offsets: %s", err)
	}
	// log.Info("Committed offsets:", committedTopicPartitionOffsets)

	endOffsets := make(map[kafka.TopicPartition]int64)
	for _, tp := range partitions {
		_, high, err := consumer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, 5000)
		if err != nil {
			log.Errorf("Failed to get watermark offsets for %s-%d: %s", *tp.Topic, tp.Partition, err)
			continue
		}
		endOffsets[tp] = high
	}

	type partitionLag struct {
		CommittedOffset int64
		Partition       int32
		EndOffset       int64
		Lag             int64
	}

	lagPartionTable := make(map[string][]partitionLag)
	lagTotal := make(map[string]int64)

	for _, tp := range committedTopicPartitionOffsets {
		val := partitionLag{
			Partition:       tp.Partition,
			EndOffset:       endOffsets[tp],
			CommittedOffset: int64(tp.Offset),
		}
		log.Info("-----------------------------")
		log.Infof("Topic: %s, Partition: %d", *tp.Topic, tp.Partition)
		log.Infof("End Offset: %d", val.EndOffset)
		log.Infof("Committed Offset: %d", val.CommittedOffset)
		log.Info("-----------------------------")

		if tp.Offset == kafka.OffsetInvalid {
			val.CommittedOffset = 0
		}

		lag := val.EndOffset - val.CommittedOffset
		if lag < 0 {
			lag = 0
		}
		val.Lag = lag
		lagPartionTable[*tp.Topic] = append(lagPartionTable[*tp.Topic], val)
		lagTotal[*tp.Topic] += lag
	}

	log.Info("====================================================")
	log.Infof("Consumer Lag for Group %s", consumerGroup)
	log.Info("====================================================")
	log.Info("Consumer Lag per Partition:")
	for key, lag := range lagPartionTable {
		log.Infof("- Topic: %s:", key)
		for _, p := range lag {
			log.Infof("    - Partition ID: %d, Lag=%d (committed %d, end %d)", p.Partition, p.Lag, p.CommittedOffset, p.EndOffset)
		}
	}

	log.Info("====================================================")
	log.Info("Summary Consumer Lag per Topic:")
	for topic, lag := range lagTotal {
		log.Infof(" - Topic %s: Total Lag=%d", topic, lag)
	}
}
