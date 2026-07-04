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

	usecase "ingestion_service/pkg/app"
	"ingestion_service/pkg/domain/model"
	ingestionadapter "ingestion_service/pkg/infra/network/adapter"
	resthandler "ingestion_service/pkg/infra/network/rest"
	restsupport "ingestion_service/pkg/infra/network/restsupport"
	"ingestion_service/pkg/infra/repo/bucket"
	repo "ingestion_service/pkg/infra/repo/db"
	ingestionpb "lib/data_contracts_lib/ingestion"
	corebucket "lib/shared_lib/bucket"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	messaging "lib/shared_lib/messaging"
	serializers "lib/shared_lib/serializer"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestIngestionIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion integration test suite")
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

var _ = Describe("Ingestion integration", Ordered, func() {
	var (
		ctx          context.Context
		cancel       context.CancelFunc
		database     *dbconn.Database
		datasetDB    *repo.DatasetDB
		datasets     *usecase.DatasetUsecase
		uploader     resthandler.DataUploadUseCase
		objectBucket *corebucket.S3Bucket
		topic        string
		brokers      string
		msgFactory   messaging.Messenger
		relayCancel  context.CancelFunc
		relayDone    chan struct{}
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		cfg := dbconn.DatabaseConfig{}
		cfg.WithDbName("INGESTION_SERVICE_DB_NAME", "bighill_ingestion_db")
		cfg.WithDbUser("INGESTION_SERVICE_DB_USER", "bighill_ingestion_db_user")
		cfg.WithDbPassword("INGESTION_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
		cfg.WithDbMaxConnections("INGESTION_SERVICE_DB_MAX_CONNECTIONS", "20")

		var err error
		database, err = dbconn.InitDatabase(ctx, cfg.GetName(), cfg.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		brokers = env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		topic = env.WithDefaultString("INGESTION_SERVICE_TOPIC", "ingestion")
		Expect(purgeTopic(ctx, brokers, topic)).To(Succeed())

		msgFactory = messaging.NewMessenger(messaging.MessengerConfig{
			Brokers: brokers,
			GroupID: "ingestion-integration-" + uuid.NewString(),
		}, cancel)
		relayPublisher, err := msgFactory.Publisher(ctx)
		Expect(err).NotTo(HaveOccurred())

		datasetDB = repo.NewDatasetDB(database)
		datasets = usecase.NewDatasetUseCase(datasetDB)
		objectBucket = corebucket.NewBucket(ctx, corebucket.LocalDevS3Region, 10*1024*1024)
		uploadBucket := bucket.NewDataBucket("local-dev-bucket", objectBucket)
		outboxWriter, err := messaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(messaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		outboxSignal := make(chan struct{}, 1)
		signaledOutbox := messaging.NewSignaledOutbox(outboxWriter, outboxSignal)
		relayOutbox, ok := signaledOutbox.(messaging.RelayOutbox)
		Expect(ok).To(BeTrue())
		relayPublisherForOutbox, ok := relayPublisher.(messaging.RelayPublisher)
		Expect(ok).To(BeTrue())
		relayCtx, stopRelay := context.WithCancel(ctx)
		relayCancel = stopRelay
		relayDone = make(chan struct{})
		relay := messaging.NewOutboxRelay(relayOutbox, relayPublisherForOutbox, messaging.OutboxRelayConfig{
			PollInterval:   100 * time.Millisecond,
			FailureBackoff: 100 * time.Millisecond,
			BatchSize:      10,
			Signal:         outboxSignal,
		})
		go func() {
			defer close(relayDone)
			_ = relay.Run(relayCtx)
		}()
		uploadSessionRepo := repo.NewUploadSessionDB(
			database,
			repo.WithUploadSessionOutbox(orderedOutbox, topic),
			repo.WithUploadSessionOutboxSignal(func() { messaging.NotifyOutboxSignal(outboxSignal) }),
		)
		detector := resthandler.NewDetector(map[string]resthandler.FormatValidatorFunc{
			resthandler.FileTypeCSV:      resthandler.IsCSV,
			resthandler.FileTypeJSON:     resthandler.IsJSON,
			resthandler.FileTypeParquet:  resthandler.IsParquet,
			resthandler.FileTypePDF:      resthandler.IsPDF,
			resthandler.FileTypeHTML:     resthandler.IsHTML,
			resthandler.FileTypeMarkdown: resthandler.IsMarkdown,
			resthandler.FileTypeText:     resthandler.IsText,
		})
		uploader = usecase.NewDataUploadUseCase(uploadBucket,
			usecase.WithUploadSessionRepository(uploadSessionRepo),
			usecase.WithUploadDatasetRepository(datasetDB),
			usecase.WithUploadFileDetector(detector),
			usecase.WithUploadPolicy(20*1000*1000, 15*time.Minute, 5*1000*1000),
		)
	})

	BeforeEach(func() {
		Expect(purgeTopic(ctx, brokers, topic)).To(Succeed())
	})

	AfterAll(func() {
		if relayCancel != nil {
			relayCancel()
		}
		if relayDone != nil {
			<-relayDone
		}
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
		Expect(upsertIngestionTenant(ctx, database, userID)).To(Succeed())
		Expect(datasets.AddDataset(ctx, validIngestionDataset(datasetID, userID))).To(Succeed())

		upload := &model.DataFile{
			DatasetID:         datasetID,
			UserID:            userID,
			File:              memoryMultipartFile{Reader: bytes.NewReader([]byte("title,release_year\nMetropolis,1927\n"))},
			Extension:         "csv",
			ContentType:       "text/csv",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
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
		Expect(event.TableNamespace).To(Equal("features"))
		Expect(event.TableName).To(Equal("movies"))
		Expect(event.TableFormat).To(Equal("PARQUET"))
		Expect(event.CatalogProvider).To(Equal("LOCAL"))
		Expect(event.ProcessingProfile).To(Equal("TEXT_RAG"))
	})

	It("does not upload or publish when the dataset is blacklisted", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		Expect(upsertIngestionTenant(ctx, database, userID)).To(Succeed())
		Expect(datasets.AddDataset(ctx, validIngestionDataset(datasetID, userID))).To(Succeed())
		Expect(datasets.BlacklistDataset(ctx, datasetID, userID)).To(Succeed())

		detector := resthandler.NewDetector(map[string]resthandler.FormatValidatorFunc{
			resthandler.FileTypeCSV:      resthandler.IsCSV,
			resthandler.FileTypeJSON:     resthandler.IsJSON,
			resthandler.FileTypeParquet:  resthandler.IsParquet,
			resthandler.FileTypePDF:      resthandler.IsPDF,
			resthandler.FileTypeHTML:     resthandler.IsHTML,
			resthandler.FileTypeMarkdown: resthandler.IsMarkdown,
			resthandler.FileTypeText:     resthandler.IsText,
		})
		uploadDTOAdapter := ingestionadapter.NewUploadDTOAdapter(serializers.NewJSONSerializer())
		handler := resthandler.NewDataUploadHandlers(uploader, datasets, uploadDTOAdapter, detector, testAuthenticator{userID: userID}, 1024*1024)

		req := uploadRequest(datasetID, "movies.csv", []byte("title,release_year\nMetropolis,1927\n"))
		response, err := handler.UploadDataFile(ctx, req)

		Expect(response).To(BeNil())
		Expect(err).To(MatchError("No valid dataset found for upload"))
		var httpErr *restsupport.HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(noDatasetUploadedEvent(ctx, brokers, topic, datasetID, 1500*time.Millisecond)).To(BeTrue())
	})

	It("promotes a large presigned parquet upload and records a Kafka dataset_file_uploaded event", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		Expect(upsertIngestionTenant(ctx, database, userID)).To(Succeed())
		Expect(datasets.AddDataset(ctx, validIngestionDataset(datasetID, userID))).To(Succeed())

		parquet := largeParquetObject(6*1000*1000 + 8)
		initiated, err := uploader.InitiateUploadSession(ctx, model.InitiateUploadSessionRequest{
			DatasetID:           datasetID,
			UserID:              userID,
			ClientNonce:         "large-parquet-" + datasetID.String(),
			FileName:            "large report.parquet",
			DeclaredFormat:      resthandler.FileTypeParquet,
			DeclaredContentType: "application/vnd.apache.parquet",
			DeclaredSizeBytes:   int64(len(parquet)),
			TableNamespace:      "features",
			TableName:           "large_parquet",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG",
		})
		Expect(err).NotTo(HaveOccurred())
		stagingKey := initiated.Fields["key"]
		Expect(stagingKey).To(ContainSubstring("large report.parquet"))

		Expect(objectBucket.Upload(ctx, "local-dev-bucket", stagingKey, "application/vnd.apache.parquet", bytes.NewReader(parquet))).To(Succeed())

		completed, err := uploader.CompleteUploadSession(ctx, model.CompleteUploadSessionRequest{
			UploadID: initiated.UploadID,
			UserID:   userID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(completed.Status).To(Equal(model.UploadSessionPromoted))
		Expect(completed.ActualSizeBytes).To(Equal(int64(len(parquet))))
		Expect(completed.StorageLocation).To(HavePrefix("s3://local-dev-bucket/raw/" + datasetID.String() + "/" + initiated.UploadID.String() + "/"))

		envelope, event := consumeDatasetUploadedEvent(ctx, brokers, topic, datasetID)
		Expect(envelope.ResourceKey).To(Equal(datasetID))
		Expect(event.FileExtension).To(Equal(resthandler.FileTypeParquet))
		Expect(event.ContentType).To(Equal("application/vnd.apache.parquet"))
		Expect(event.StorageLocation).To(Equal(completed.StorageLocation))
		Expect(event.TableName).To(Equal("large_parquet"))
	})
})

func largeParquetObject(size int) []byte {
	if size < 8 {
		size = 8
	}
	data := make([]byte, size)
	copy(data[:4], []byte("PAR1"))
	copy(data[len(data)-4:], []byte("PAR1"))
	return data
}

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

func validIngestionDataset(datasetID, userID uuid.UUID) *model.Dataset {
	return &model.Dataset{
		DatasetID:         datasetID,
		UserID:            userID,
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		ProcessingProfile: "TEXT_RAG",
		SchemaVersion:     1,
		SchemaMetadata:    "{}",
	}
}

func upsertIngestionTenant(ctx context.Context, database *dbconn.Database, userID uuid.UUID) error {
	_, err := database.Pool.Exec(ctx, `
		INSERT INTO `+database.Name+`.tenants (id, email, deleted)
		VALUES ($1, $2, false)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, deleted = false
	`, userID, userID.String()+"@example.test")
	return err
}

func consumeDatasetUploadedEvent(ctx context.Context, brokers, topic string, datasetID uuid.UUID) (messaging.Message, *ingestionpb.DatasetFileUploadedEvent) {
	consumer := newKafkaConsumer(brokers, "ingestion-assert-"+uuid.NewString())
	defer consumer.Close()
	Expect(consumer.SubscribeTopics([]string{topic}, nil)).To(Succeed())

	var envelope messaging.Message
	var payload ingestionpb.DatasetFileUploadedEvent
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
	consumer := newKafkaConsumer(brokers, "ingestion-empty-"+uuid.NewString())
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
