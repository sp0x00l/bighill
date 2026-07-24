package messaging_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	ingestionpb "lib/data_contracts_lib/ingestion"
	env "lib/shared_lib/env"
	"lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type recordingDatasetFileUploadedListener struct {
	mu       sync.Mutex
	received map[uuid.UUID]*ingestionpb.DatasetFileUploadedEvent
	events   chan uuid.UUID
}

func newRecordingDatasetFileUploadedListener(capacity int) *recordingDatasetFileUploadedListener {
	return &recordingDatasetFileUploadedListener{
		received: make(map[uuid.UUID]*ingestionpb.DatasetFileUploadedEvent),
		events:   make(chan uuid.UUID, capacity),
	}
}

func (l *recordingDatasetFileUploadedListener) MsgType() messaging.MsgType {
	return messaging.MsgTypeDatasetFileUploaded
}

func (l *recordingDatasetFileUploadedListener) NewMessage() *ingestionpb.DatasetFileUploadedEvent {
	return &ingestionpb.DatasetFileUploadedEvent{}
}

func (l *recordingDatasetFileUploadedListener) Handle(_ context.Context, resourceKey uuid.UUID, payload *ingestionpb.DatasetFileUploadedEvent) error {
	if payload == nil {
		return fmt.Errorf("payload is required")
	}
	if strings.TrimSpace(payload.GetDatasetId()) != resourceKey.String() {
		return fmt.Errorf("dataset id %q does not match resource key %s", payload.GetDatasetId(), resourceKey)
	}

	l.mu.Lock()
	l.received[resourceKey] = payload
	l.mu.Unlock()

	select {
	case l.events <- resourceKey:
	default:
	}
	return nil
}

func (l *recordingDatasetFileUploadedListener) receivedCount(ids []uuid.UUID) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	count := 0
	for _, id := range ids {
		if _, ok := l.received[id]; ok {
			count++
		}
	}
	return count
}

var _ = Describe("Kafka publisher/subscriber integration", func() {
	It("delivers dataset upload events through Kafka and commits them through the subscriber", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		topic := "shared-lib-messaging-it-" + strings.ReplaceAll(uuid.NewString(), "-", "")
		groupID := "shared-lib-messaging-it-" + strings.ReplaceAll(uuid.NewString(), "-", "")
		const messageCount = 8

		Expect(messaging.CreateTopicWithConfig(ctx, brokers, topic, 1, 1)).To(Succeed())

		messengerFactory := messaging.NewMessenger(messaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         groupID,
			DlqURL:          "",
			AutoOffsetReset: "earliest",
			NumShards:       1,
			ChannelBuffer:   messageCount,
		}, nil)
		defer func() {
			Expect(messengerFactory.Close(context.Background())).To(Succeed())
		}()
		subscriber, err := messengerFactory.Subscriber(ctx)
		Expect(err).NotTo(HaveOccurred())
		listener := newRecordingDatasetFileUploadedListener(messageCount)
		messaging.AddListener(subscriber, listener)

		subscribeCtx, stopSubscriber := context.WithCancel(ctx)
		defer stopSubscriber()
		subscribeDone := make(chan error, 1)
		go func() {
			err := subscriber.Subscribe(subscribeCtx, []string{topic})
			if errors.Is(err, context.Canceled) {
				err = nil
			}
			subscribeDone <- err
		}()
		defer func() {
			stopSubscriber()
			Eventually(subscribeDone, 10*time.Second).Should(Receive(Succeed()))
		}()

		Eventually(func() error {
			return messaging.CheckSubscriberHealth(ctx, subscriber, messaging.SubscriberHealthCheckConfig{
				RequireAssignment: true,
				MaxPollSilence:    5 * time.Second,
			})
		}, 30*time.Second, 100*time.Millisecond).Should(Succeed())

		publisher, err := messengerFactory.Publisher(ctx)
		Expect(err).NotTo(HaveOccurred())

		datasetIDs := make([]uuid.UUID, 0, messageCount)
		userID := uuid.New()
		orgID := uuid.New()
		for i := 0; i < messageCount; i++ {
			datasetID := uuid.New()
			datasetIDs = append(datasetIDs, datasetID)
			err := publisher.Publish(ctx, topic, messaging.Message{
				ResourceKey: datasetID,
				MsgType:     messaging.MsgTypeDatasetFileUploaded,
			}, &ingestionpb.DatasetFileUploadedEvent{
				DatasetId:         datasetID.String(),
				UserId:            userID.String(),
				OrgId:             orgID.String(),
				StorageLocation:   fmt.Sprintf("s3://local-dev-bucket/raw/%s/file-%d.csv", datasetID, i),
				ContentType:       "text/csv",
				FileExtension:     "csv",
				TableNamespace:    "features",
				TableName:         "movies",
				TableFormat:       "PARQUET",
				CatalogProvider:   "LOCAL",
				ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
			})
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(func() int {
			return listener.receivedCount(datasetIDs)
		}, 20*time.Second, 100*time.Millisecond).Should(Equal(messageCount))

		reporter, ok := subscriber.(messaging.SubscriberHealthReporter)
		Expect(ok).To(BeTrue())
		Eventually(func() uint64 {
			return reporter.Health().MessagesCommitted
		}, 20*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", messageCount))
	})
})
