package integration_test

import (
	"bytes"
	"context"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	usecase "data_ingestion_service/pkg/app"
	"data_ingestion_service/pkg/domain/model"
	resthandler "data_ingestion_service/pkg/infra/network/rest"
	restsupport "data_ingestion_service/pkg/infra/network/restsupport"
	"data_ingestion_service/pkg/infra/repo/bucket"
	repo "data_ingestion_service/pkg/infra/repo/db"
	datasetpb "lib/data_contracts_lib/dataset"
	corebucket "lib/shared_lib/bucket"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	messaging "lib/shared_lib/messaging"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestDataIngestionIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data ingestion integration test suite")
}

type testAuthenticator struct {
	userID uuid.UUID
}

type memoryMultipartFile struct {
	*bytes.Reader
}

func (f memoryMultipartFile) Close() error {
	return nil
}

func (a testAuthenticator) Authenticate(context.Context, *http.Request) (resthandler.AuthResult, error) {
	return resthandler.AuthResult{UserID: a.userID, ExpUnix: time.Now().Add(time.Hour).Unix()}, nil
}

var _ = Describe("Data ingestion integration", Ordered, func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		database   *dbconn.Database
		datasetDB  *repo.DatasetDB
		datasets   *usecase.DatasetUsecase
		uploader   resthandler.DataUploadUseCase
		topic      string
		brokers    string
		msgFactory messaging.Messenger
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		cfg := dbconn.DatabaseConfig{}
		cfg.WithDbName("DATA_INGESTION_DB_NAME", "bighill_data_ingestion_db")
		cfg.WithDbUser("DATA_INGESTION_DB_USER", "bighill_data_ingestion_db_user")
		cfg.WithDbPassword("DATA_INGESTION_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
		cfg.WithDbMaxConnections("DATA_INGESTION_DB_MAX_CONNECTIONS", "20")

		var err error
		database, err = dbconn.InitDatabase(ctx, cfg.GetName(), cfg.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		brokers = env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		topic = env.WithDefaultString("DATA_INGESTION_SERVICE_TOPIC", "data_ingestion")
		Expect(purgeTopic(ctx, brokers, topic)).To(Succeed())

		msgFactory = messaging.NewMessenger(messaging.MessengerConfig{
			Brokers:   brokers,
			GroupID:   "data-ingestion-integration-" + uuid.NewString(),
			OutboxURL: "noop://local",
		}, cancel)
		publisher, err := msgFactory.Publisher(ctx)
		Expect(err).NotTo(HaveOccurred())

		datasetDB = repo.NewDatasetDB(database)
		datasets = usecase.NewDatasetUseCase(datasetDB)
		uploadBucket := bucket.NewDataBucket("local-dev-bucket", corebucket.NewBucket(ctx, corebucket.LocalDevS3Region, 10*1024*1024))
		uploader = usecase.NewDataUploadUseCase(uploadBucket, usecase.WithUploadEventPublisher(publisher, topic))
	})

	BeforeEach(func() {
		Expect(purgeTopic(ctx, brokers, topic)).To(Succeed())
	})

	AfterAll(func() {
		if msgFactory != nil {
			_ = msgFactory.Close(ctx)
		}
		if datasetDB != nil {
			datasetDB.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("uploads a file to local object storage and records a Kafka dataset_file_uploaded event", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		Expect(datasets.AddDataset(ctx, datasetID, userID)).To(Succeed())

		upload := &model.DataFile{
			DatasetID:   datasetID,
			UserID:      userID,
			File:        memoryMultipartFile{Reader: bytes.NewReader([]byte("title,release_year\nMetropolis,1927\n"))},
			Extension:   "csv",
			ContentType: "text/csv",
		}
		Expect(uploader.UploadFile(ctx, upload)).To(Succeed())

		envelope, event := consumeDatasetUploadedEvent(ctx, brokers, topic, datasetID)
		Expect(envelope.ResourceKey).To(Equal(datasetID))
		Expect(envelope.MsgType).To(Equal(messaging.MsgTypeDatasetFileUploaded))
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.ContentType).To(Equal("text/csv"))
		Expect(event.FileExtension).To(Equal("csv"))
		Expect(event.StorageLocation).To(HavePrefix("s3://local-dev-bucket/raw/" + userID.String() + "/" + datasetID.String() + "/"))
	})

	It("does not upload or publish when the dataset is blacklisted", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		Expect(datasets.AddDataset(ctx, datasetID, userID)).To(Succeed())
		Expect(datasets.BlacklistDataset(ctx, datasetID, userID)).To(Succeed())

		detector := resthandler.NewDetector(map[string]resthandler.FormatValidatorFunc{
			resthandler.FileTypeCSV:     resthandler.IsCSV,
			resthandler.FileTypeJSON:    resthandler.IsJSON,
			resthandler.FileTypeParquet: resthandler.IsParquet,
		})
		handler := resthandler.NewDataUploadHandlers(uploader, datasets, detector, testAuthenticator{userID: userID}, 1024*1024)

		req := uploadRequest(datasetID, "movies.csv", []byte("title,release_year\nMetropolis,1927\n"))
		response, err := handler.UploadDataFile(ctx, req)

		Expect(response).To(BeNil())
		Expect(err).To(MatchError("No valid dataset found for upload"))
		var httpErr *restsupport.HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(noDatasetUploadedEvent(ctx, brokers, topic, datasetID, 1500*time.Millisecond)).To(BeTrue())
	})
})

func uploadRequest(datasetID uuid.UUID, filename string, content []byte) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filename)
	Expect(err).NotTo(HaveOccurred())
	_, err = part.Write(content)
	Expect(err).NotTo(HaveOccurred())
	Expect(writer.Close()).To(Succeed())

	req := httptest.NewRequest(http.MethodPost, "/v1/data/store/"+datasetID.String(), body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return mux.SetURLVars(req, map[string]string{"id": datasetID.String()})
}

func consumeDatasetUploadedEvent(ctx context.Context, brokers, topic string, datasetID uuid.UUID) (messaging.Message, *datasetpb.DatasetFileUploadedEvent) {
	consumer := newKafkaConsumer(brokers, "data-ingestion-assert-"+uuid.NewString())
	defer consumer.Close()
	Expect(consumer.SubscribeTopics([]string{topic}, nil)).To(Succeed())

	var envelope messaging.Message
	var payload datasetpb.DatasetFileUploadedEvent
	Eventually(func() bool {
		switch ev := consumer.Poll(250).(type) {
		case *kafka.Message:
			Expect(envelope.Deserialize(ctx, ev.Value)).To(Succeed())
			if envelope.ResourceKey != datasetID {
				return false
			}
			Expect(envelope.DeserializePayload(&payload)).To(Succeed())
			return true
		case kafka.Error:
			Fail(ev.Error())
		}
		return false
	}, 15*time.Second, 100*time.Millisecond).Should(BeTrue(), "timed out waiting for dataset_file_uploaded Kafka event")
	return envelope, &payload
}

func noDatasetUploadedEvent(ctx context.Context, brokers, topic string, datasetID uuid.UUID, timeout time.Duration) bool {
	consumer := newKafkaConsumer(brokers, "data-ingestion-empty-"+uuid.NewString())
	defer consumer.Close()
	Expect(consumer.SubscribeTopics([]string{topic}, nil)).To(Succeed())

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		switch ev := consumer.Poll(100).(type) {
		case *kafka.Message:
			var envelope messaging.Message
			if err := envelope.Deserialize(ctx, ev.Value); err == nil && envelope.ResourceKey == datasetID {
				return false
			}
		case kafka.Error:
			Fail(ev.Error())
		}
	}
	return true
}

func newKafkaConsumer(brokers, groupID string) *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	Expect(err).NotTo(HaveOccurred())
	return consumer
}

func purgeTopic(ctx context.Context, brokers, topic string) error {
	Expect(messaging.CreateTopic(ctx, brokers, topic)).Should(Succeed())

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer admin.Close()

	md, err := admin.GetMetadata(&topic, false, 10000)
	if err != nil {
		return err
	}
	tmd, ok := md.Topics[topic]
	if !ok || tmd.Error.Code() != kafka.ErrNoError {
		return nil
	}

	partitions := make([]kafka.TopicPartition, 0, len(tmd.Partitions))
	for _, partition := range tmd.Partitions {
		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition.ID,
			Offset:    kafka.OffsetEnd,
		})
	}
	if len(partitions) == 0 {
		return nil
	}

	for attempt := 0; attempt < 5; attempt++ {
		if err := deleteTopicRecords(ctx, admin, partitions); err != nil {
			if isRetriableTopicPurgeError(err) && attempt < 4 {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	return nil
}

func deleteTopicRecords(ctx context.Context, admin *kafka.AdminClient, partitions []kafka.TopicPartition) error {
	res, err := admin.DeleteRecords(
		ctx,
		partitions,
		kafka.SetAdminOperationTimeout(30*time.Second),
	)
	if err != nil {
		if !isKafkaErrorCode(err, -186) {
			return err
		}
		return nil
	}

	for _, result := range res.DeleteRecordsResults {
		if result.TopicPartition.Error != nil {
			if !isKafkaErrorCode(result.TopicPartition.Error, -186) {
				return result.TopicPartition.Error
			}
		}
	}
	return nil
}

func isRetriableTopicPurgeError(err error) bool {
	return isKafkaErrorCode(err, kafka.ErrNotLeaderForPartition) ||
		isKafkaErrorCode(err, kafka.ErrLeaderNotAvailable)
}

func isKafkaErrorCode(err error, code kafka.ErrorCode) bool {
	var kafkaErr kafka.Error
	return errors.As(err, &kafkaErr) && kafkaErr.Code() == code
}
