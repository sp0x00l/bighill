package cmd

import (
	"context"
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

var (
	producerTopic     string
	messageCount      int
	messageSize       int
	producerConfigStr string
)

var producerFlushCmd = &cobra.Command{
	Use:   "producer-flush",
	Short: "Produce messages to a Kafka topic and tests flush behavior",
	Long:  "Produce messages to a kafka topic, using transactions, and tests the flush behavior. The producer config can be extended using '--producer-config \"acks=all,retries=3\"'. Bottlenecks can be tested by setting the message size, e.g. '--size 1000'.",
	Run: func(cmd *cobra.Command, args []string) {
		producerTest()
	},
}

func init() {
	producerFlushCmd.Flags().StringVarP(&producerTopic, "topic", "t", "", "Topic to produce messages to (required)")
	producerFlushCmd.Flags().IntVarP(&messageCount, "count", "c", 1, "Number of messages to produce")
	producerFlushCmd.Flags().IntVarP(&messageSize, "size", "s", 0, "Size of each message in bytes. If no specified, it will send an ID index as the value.")
	producerFlushCmd.Flags().StringVarP(&producerConfigStr, "producer-config", "p", "", "Additional producer configuration (key=value pairs separated by commas)")
	producerFlushCmd.MarkFlagRequired("topic")
}

func producerTest() {
	flushTimeoutMs := 10000

	transactionalID := fmt.Sprintf("bighill-%s", uuid.New().String())
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   brokers,
		"transactional.id":    transactionalID,
		"enable.idempotence":  true,
		"acks":                "all",
		"delivery.timeout.ms": 30000,
	}

	if producerConfigStr != "" {
		configPairs := strings.Split(producerConfigStr, ",")
		for _, pair := range configPairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				log.Fatalf("Invalid producer configuration: %s. Use key=value syntax.", pair)
			}
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			configMap.SetKey(key, value)
		}
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	err = producer.InitTransactions(context.TODO())
	if err != nil {
		log.Fatalf("Failed to initialize kafka transactions: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Channel to cancel and exit the process
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Channel to signal when all messages have been consumed
	consumerDoneChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessages(ctx, messageCount, consumerDoneChan)
	}()

	time.Sleep(1 * time.Second) // conext switch to setup the consumer

	go func() {
		for e := range producer.Events() {
			select {
			case <-ctx.Done():
				return
			default:
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Errorf("Failed to deliver message: %v", ev.TopicPartition)
					} else {
						if messageSize == 0 {
							log.Infof("Message %s delivered to topic %v, partiition %v, offset %v", string(ev.Value), *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
						} else {
							log.Infof("Message delivered to %v", ev.TopicPartition)
						}
					}
				}
			}
		}
	}()

	payload := make([]byte, messageSize)
	for i := 0; i < messageCount; i++ {
		err = producer.BeginTransaction()
		if err != nil {
			log.Fatalf("Failed to begin transaction: %s", err)
		}

		if messageSize == 0 {
			payload = []byte(fmt.Sprint(i))
		}
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &producerTopic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil)
		if err != nil {
			log.Errorf("Failed to produce message: %s", err)
		}

		ctxTimeout, ctxTimeoutCancelFtn := context.WithTimeout(context.Background(), 15000*time.Millisecond)
		err = producer.CommitTransaction(ctxTimeout)
		remainingEvents := producer.Flush(flushTimeoutMs)
		if remainingEvents > 0 {
			log.Warnf("Flush did not complete within %d ms. %d events remaining.", flushTimeoutMs, remainingEvents)
		} else {
			log.Info("Flush completed successfully. Remaining events:", remainingEvents)
		}
		ctxTimeoutCancelFtn()
		if err != nil {
			log.Errorf("Failed to commit transaction: %s", err)

		}
	}

	log.Info("====================================================")
	log.Info("Producer flush")
	if producerConfigStr != "" {
		log.Infof("   - with producer configuration: %s", producerConfigStr)
	}
	log.Infof("Produced %d messages to topic %s", messageCount, producerTopic)
	log.Info("====================================================")

	log.Info("Flushing producer...")
	remainingEvents := producer.Flush(flushTimeoutMs)
	if remainingEvents > 0 {
		log.Warnf("Flush did not complete within %d ms. %d events remaining.", flushTimeoutMs, remainingEvents)
	} else {
		log.Info("Flush completed successfully. Remaining events:", remainingEvents)
	}

	select {
	case <-consumerDoneChan:
		log.Info("All messages have been consumed.")
	case <-ctx.Done():
		log.Info("Context canceled before consuming all messages.")
	}

	cancel()
	wg.Wait()
	producer.Close()
}

func consumeMessages(ctx context.Context, expectedMessages int, doneChan chan struct{}) {
	log.Info("Starting consumer for topic:", producerTopic)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "bighill-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{producerTopic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic %s: %s", producerTopic, err)
	}

	log.Infof("Consumer started for topic %s", producerTopic)
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
				log.Infof("Consumed message from topic %s [%d] at offset %v: %s", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset, string(e.Value))
				consumer.CommitMessage(e)
				consumedCount++
				if consumedCount >= expectedMessages {
					log.Infof("Consumed all %d messages.", expectedMessages)
					// all messages have been consumed
					doneChan <- struct{}{}
					return
				}
			case kafka.Error:
				log.Errorf("Consumer error: %v", e)
				if e.IsFatal() {
					log.Fatalf("Fatal consumer error: %v", e)
				}
			default:
				log.Info("Ignored event:", ev)
			}
		}
	}
}
