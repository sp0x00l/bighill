package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/stats_example/stats_example.go
// https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md

var (
	metricsTopic                string
	metricsMsgCount             int
	metricsMessageSize          int
	metricsProducerConfigString string
	metricsConsumerGroup        string
)

var producerMetricsCmd = &cobra.Command{
	Use:   "producer-metrics",
	Short: "Produce messages to a Kafka topic and test flush behavior, with a consumer and producer metrics",
	Run: func(cmd *cobra.Command, args []string) {
		producerMetrics()
	},
}

func init() {
	producerMetricsCmd.Flags().StringVarP(&metricsTopic, "topic", "t", "metrics-test-topic", "Kafka topic")
	producerMetricsCmd.Flags().IntVarP(&metricsMsgCount, "count", "c", 10000, "Number of messages to produce")
	producerMetricsCmd.Flags().IntVarP(&metricsMessageSize, "size", "s", 100, "Size of each message in bytes")
	producerMetricsCmd.Flags().StringVarP(&metricsProducerConfigString, "producer-config", "p", "", "Additional producer configuration (key=value pairs separated by commas)")
	producerMetricsCmd.Flags().StringVarP(&metricsConsumerGroup, "group", "g", "producer-test-group", "Consumer group ID")
}

func producerMetrics() {
	transactionalID := fmt.Sprintf("bighill-%s", uuid.New().String())
	producerConfigMap := &kafka.ConfigMap{
		"bootstrap.servers":      brokers,
		"transactional.id":       transactionalID,
		"enable.idempotence":     true,
		"acks":                   "all",
		"delivery.timeout.ms":    30000,
		"statistics.interval.ms": 1000, // Enable statistics collection every 1 second
	}

	if metricsProducerConfigString != "" {
		configPairs := strings.Split(metricsProducerConfigString, ",")
		for _, pair := range configPairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				log.Fatalf("Invalid producer configuration: %s", pair)
			}
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			producerConfigMap.SetKey(key, value)
		}
	}

	producer, err := kafka.NewProducer(producerConfigMap)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	err = producer.InitTransactions(context.TODO())
	if err != nil {
		log.Fatalf("Failed to initialize kafka transactions: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Info("Terminate signal received. Exiting.")
		cancel()
	}()

	consumerDoneChan := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessagesForStats(ctx, metricsTopic, metricsMsgCount, consumerDoneChan)
	}()
	time.Sleep(1 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		handleProducerEvents(ctx, producer)
	}()

	payload := make([]byte, metricsMessageSize)
	for i := 0; i < metricsMsgCount; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			err = producer.BeginTransaction()
			if err != nil {
				log.Fatalf("Failed to begin transaction: %s", err)
			}

			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &metricsTopic, Partition: kafka.PartitionAny},
				Value:          payload,
			}, nil)
			if err != nil {
				log.Errorf("Failed to produce message: %s", err)
			}

			ctxTimeout, ctxTimeoutCancelFtn := context.WithTimeout(context.Background(), 15000*time.Millisecond)
			err = producer.CommitTransaction(ctxTimeout)
			ctxTimeoutCancelFtn()
			if err != nil {
				log.Errorf("Failed to commit transaction: %s", err)
			}
		}
	}

	log.Info("Flushing producer")
	flushTimeoutMs := 10000
	remainingEvents := producer.Flush(flushTimeoutMs)
	if remainingEvents > 0 {
		log.Warnf("Flush did not complete within %d ms. %d events remaining.", flushTimeoutMs, remainingEvents)
	} else {
		log.Info("Flush completed successfully.")
	}

	// Wait for the consumer to finish consuming all messages
	select {
	case <-consumerDoneChan:
		log.Info("All messages have been consumed.")
	case <-ctx.Done():
		log.Info("Context canceled before consuming all messages.")
	}

	// Cancel the context and wait for all goroutines to finish
	cancel()
	wg.Wait()
	producer.Close()
}

func handleProducerEvents(ctx context.Context, producer *kafka.Producer) {
	for {
		select {
		case <-ctx.Done():
			log.Info("Producer events handler exiting.")
			return
		case e := <-producer.Events():
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Errorf("Failed to deliver message: %v (partition %d)", *ev.TopicPartition.Topic, ev.TopicPartition.Partition)
				} else {
					log.Infof("Message delivered to %v (partition %d)", *ev.TopicPartition.Topic, ev.TopicPartition.Partition)
				}
			case kafka.Error:
				log.Errorf("Kafka error: %v", ev)
			case *kafka.Stats:
				var statsMap map[string]interface{}
				if err := json.Unmarshal([]byte(ev.String()), &statsMap); err != nil {
					log.Errorf("Failed to parse stats JSON: %s", err)
					continue
				}
				processProducerStats(statsMap)
			default:
				log.Info("Ignored event:", ev)
			}
		}
	}
}

func consumeMessagesForStats(ctx context.Context, topic string, expectedMessages int, consumerDoneChan chan struct{}) {
	consumerConfigMap := &kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "bighill-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}

	consumer, err := kafka.NewConsumer(consumerConfigMap)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic %s: %s", topic, err)
	}

	log.Infof("Consumer started for topic %s", topic)
	consumedCount := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("Consumer context canceled. Exiting consumer loop.")
			return
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				log.Infof("Consumed message from topic %s (partition %d) at offset %v", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				consumedCount++
				if consumedCount >= expectedMessages {
					log.Infof("Consumed all %d messages.", expectedMessages)
					// Signal that all messages have been consumed
					consumerDoneChan <- struct{}{}
					return
				}
			case kafka.Error:
				log.Errorf("Consumer error: %v", e)
				// Handle fatal errors
				if e.IsFatal() {
					log.Fatalf("Fatal consumer error: %v", e)
				}
			default:
				log.Info("Ignored consumer event:", ev)
			}
		}
	}
}

func processProducerStats(statsMap map[string]interface{}) {
	log.Info("====================================================")
	log.Info("Producer Metrics:")
	log.Info("====================================================")

	// Extract total messages sent and bytes
	txmsgs, _ := statsMap["txmsgs"].(float64)
	txmsg_bytes, _ := statsMap["txmsg_bytes"].(float64)
	log.Infof("Total messages sent: %v messages (%v bytes)", txmsgs, txmsg_bytes)
	log.Info(" - The total number of messages the producer has sent to Kafka.")

	// Extract current message queue status
	msg_cnt, _ := statsMap["msg_cnt"].(float64)
	msg_size, _ := statsMap["msg_size"].(float64)
	msg_max, _ := statsMap["msg_max"].(float64)
	log.Infof("Current message queue: %v messages (%v bytes), Max queue size: %v messages", msg_cnt, msg_size, msg_max)
	log.Info(" - The message count currently in the producer's queue.")

	// Extract reply queue size
	replyq, _ := statsMap["replyq"].(float64)
	log.Infof("Reply queue size: %v", replyq)
	log.Info(" - The number of messages awaiting acknowledgment from Kafka brokers.")

	// Extract error counts if available
	txerrs, _ := statsMap["txerrs"].(float64)
	txretries, _ := statsMap["txretries"].(float64)
	log.Infof("Transmission errors: %v, Retries: %v", txerrs, txretries)
	log.Info(" - Any errors or retries during message transmission.")

	if eosStats, ok := statsMap["eos"].(map[string]interface{}); ok {
		log.Info("Exactly-Once Semantics (EOS) Metrics:")
		// idemp_state
		idemp_state, _ := eosStats["idemp_state"].(string)
		log.Infof(" - Idempotent Producer State: %s", idemp_state)
		log.Info("   - Indicates the current state of the idempotent producer.")

		// idemp_stateage
		idemp_stateage, _ := eosStats["idemp_stateage"].(float64)
		log.Infof(" - Idempotent State Age: %.0f ms", idemp_stateage)
		log.Info("   - Time elapsed since the last idempotent state change.")

		// txn_state
		txn_state, _ := eosStats["txn_state"].(string)
		log.Infof(" - Transactional Producer State: %s", txn_state)
		log.Info("   - Indicates the current transactional state of the producer.")

		// txn_stateage
		txn_stateage, _ := eosStats["txn_stateage"].(float64)
		log.Infof(" - Transactional State Age: %.0f ms", txn_stateage)
		log.Info("   - Time elapsed since the last transactional state change.")

		// txn_may_enq
		txn_may_enq, _ := eosStats["txn_may_enq"].(bool)
		log.Infof(" - Transaction May Enqueue: %v", txn_may_enq)
		log.Info("   - Indicates if the transactional producer can enqueue (produce) new messages.")

		// producer_id
		producer_id, _ := eosStats["producer_id"].(float64)
		log.Infof(" - Producer ID: %.0f", producer_id)
		log.Info("   - The currently assigned Producer ID (-1 if not assigned).")

		// producer_epoch
		producer_epoch, _ := eosStats["producer_epoch"].(float64)
		log.Infof(" - Producer Epoch: %.0f", producer_epoch)
		log.Info("   - The current producer epoch (-1 if not assigned).")

		// epoch_cnt
		epoch_cnt, _ := eosStats["epoch_cnt"].(float64)
		log.Infof(" - Producer ID Assignment Count: %.0f", epoch_cnt)
		log.Info("   - The number of Producer ID assignments since start.")
	} else {
		log.Info("No EOS statistics available.")
	}

	// Extract broker metrics
	if brokers, ok := statsMap["brokers"].(map[string]interface{}); ok {
		for brokerName, brokerInfo := range brokers {
			if brokerInfoMap, ok := brokerInfo.(map[string]interface{}); ok {
				state, _ := brokerInfoMap["state"].(string)
				outbuf_cnt, _ := brokerInfoMap["outbuf_cnt"].(float64)
				waitresp_cnt, _ := brokerInfoMap["waitresp_cnt"].(float64)
				broker_msg_cnt, _ := brokerInfoMap["msg_cnt"].(float64)
				log.Infof("Broker %s:", brokerName)
				log.Infof(" - State: %s", state)
				log.Info("   - The connection state with the broker (e.g., UP, DOWN).")
				log.Infof(" - Outgoing request queue: %v", outbuf_cnt)
				log.Info("   - The message count waiting to be sent to the broker.")
				log.Infof(" - Requests awaiting response: %v", waitresp_cnt)
				log.Info("   - The message count (sent to the broker) awaiting response.")
				log.Infof(" - Messages in broker queue: %v", broker_msg_cnt)
				log.Info("   - Number of messages queued for sending to this broker.")
			}
		}
	}

	// Extract topic and partition metrics
	if topics, ok := statsMap["topics"].(map[string]interface{}); ok {
		for topicName, topicInfo := range topics {
			if topicInfoMap, ok := topicInfo.(map[string]interface{}); ok {
				log.Infof("Topic %s:", topicName)
				if partitions, ok := topicInfoMap["partitions"].(map[string]interface{}); ok {
					for partitionID, partitionInfo := range partitions {
						if partitionInfoMap, ok := partitionInfo.(map[string]interface{}); ok {
							msgs, _ := partitionInfoMap["msgs"].(float64)
							msgs_inflight, _ := partitionInfoMap["msgs_inflight"].(float64)
							txmsgs, _ := partitionInfoMap["txmsgs"].(float64)
							log.Infof(" - Partition %s:", partitionID)
							log.Infof("   - Messages in queue: %v", msgs)
							log.Info("     - Number of messages queued for this partition.")
							log.Infof("   - Messages in flight: %v", msgs_inflight)
							log.Info("     - Messages sent to broker but not yet acknowledged.")
							log.Infof("   - Messages transmitted: %v", txmsgs)
							log.Info("     - Total messages transmitted for this partition.")
						}
					}
				}
			}
		}
	}
}
