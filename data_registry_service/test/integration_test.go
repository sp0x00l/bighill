package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	usecase "data_registry_service/pkg/app"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	catalogclient "data_registry_service/pkg/infra/network/client"
	registrymessaging "data_registry_service/pkg/infra/network/messaging"
	repo "data_registry_service/pkg/infra/repo/db"
	featurepb "lib/data_contracts_lib/feature_materializer"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"
	"lib/shared_lib/transport"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestDataRegistryIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry integration test suite")
}

var _ = Describe("Data registry integration", Ordered, func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		database   *dbconn.Database
		datasetDB  usecase.DatasetRepositoryAdapter
		sourceDB   usecase.SourceRepositoryAdapter
		datasets   usecase.DatasetUsecase
		connectors usecase.SourceUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		cfg := dbconn.DatabaseConfig{}
		cfg.WithDbName("DATA_REGISTRY_DB_NAME", "bighill_data_registry_db")
		cfg.WithDbUser("DATA_REGISTRY_DB_USER", "bighill_data_registry_db_user")
		cfg.WithDbPassword("DATA_REGISTRY_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw")
		cfg.WithDbMaxConnections("DATA_REGISTRY_DB_MAX_CONNECTIONS", "20")

		var err error
		database, err = dbconn.InitDatabase(ctx, cfg.GetName(), cfg.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())

		datasetDB = repo.NewDatasetDB(database, repo.WithTransactionalOutbox(orderedOutbox, "data_registry"))
		sourceDB = repo.NewSourceConnectorDB(database)
		datasets = usecase.NewDatasetUseCase(datasetDB)
		connectors = usecase.NewSourceUsecase(sourceDB, catalogclient.NewLocalCatalogClient())
	})

	AfterAll(func() {
		if datasetDB != nil {
			datasetDB.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("persists dataset metadata through Postgres", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		dataset := &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Title:           "Movie features",
			Description:     "Feature metadata for movie records",
			Location:        "s3://local-dev-bucket/raw/movies.csv",
			Category:        "movies",
			TableNamespace:  "features",
			TableName:       "movie_features",
			TableFormat:     model.Parquet,
			CatalogProvider: model.LocalCatalog,
			SchemaVersion:   1,
			SchemaMetadata:  `{"columns":["title","release_year"]}`,
		}

		Expect(datasets.CreateDataset(ctx, dataset, uuid.New())).To(Succeed())

		read, err := datasets.ReadDatasetForUser(ctx, datasetID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.Title).To(Equal("Movie features"))
		Expect(read.TableNamespace).To(Equal("features"))
		Expect(read.TableName).To(Equal("movie_features"))
		Expect(read.TableFormat).To(Equal(model.Parquet))
		Expect(read.ProcessingState).To(Equal(model.DatasetProcessingPending))

		rawReady, err := datasets.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)
		Expect(err).NotTo(HaveOccurred())
		Expect(rawReady.ProcessingState).To(Equal(model.DatasetProcessingRawMaterialized))
		Expect(rawReady.DatasetVersion).To(BeNumerically(">=", 2))

		rawSnapshotID := uuid.New()
		featureSnapshotID := uuid.New()
		materialized, err := datasets.RecordDatasetMaterialization(ctx, &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Location:          "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "movie_features",
			TableFormat:       model.Parquet,
			CatalogProvider:   model.LocalCatalog,
			SchemaVersion:     2,
			SchemaMetadata:    `{"columns":["title","views"]}`,
			RawSnapshotID:     rawSnapshotID,
			FeatureSnapshotID: featureSnapshotID,
		}, model.DatasetProcessingFeatureMaterialized)
		Expect(err).NotTo(HaveOccurred())
		Expect(materialized.ProcessingState).To(Equal(model.DatasetProcessingFeatureMaterialized))
		Expect(materialized.Location).To(Equal("s3://local-dev-bucket/lakehouse/features/data.parquet"))
		Expect(materialized.RawSnapshotID).To(Equal(rawSnapshotID))
		Expect(materialized.FeatureSnapshotID).To(Equal(featureSnapshotID))

		embeddingSnapshotID := uuid.New()
		embedded, err := datasets.RecordDatasetMaterialization(ctx, &model.Dataset{
			ID:                  datasetID,
			UserID:              userID,
			FeatureSnapshotID:   featureSnapshotID,
			EmbeddingSnapshotID: embeddingSnapshotID,
			VectorStore:         "pgvector",
			CollectionName:      "movie_features",
			EmbeddingDimensions: 384,
			EmbeddingCount:      2,
		}, model.DatasetProcessingEmbeddingsMaterialized)
		Expect(err).NotTo(HaveOccurred())
		Expect(embedded.ProcessingState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
		Expect(embedded.EmbeddingSnapshotID).To(Equal(embeddingSnapshotID))
		Expect(embedded.VectorStore).To(Equal("pgvector"))

		var outboxCount int
		err = database.Pool.QueryRow(ctx,
			"SELECT COUNT(*) FROM "+database.Name+".outbox_messages WHERE resource_key = $1::uuid AND event_type = 'dataset_updated'",
			datasetID.String(),
		).Scan(&outboxCount)
		Expect(err).NotTo(HaveOccurred())
		Expect(outboxCount).To(Equal(2))

		lateRaw, err := datasets.AdvanceDatasetProcessingState(ctx, datasetID, userID, model.DatasetProcessingRawMaterialized)
		Expect(err).NotTo(HaveOccurred())
		Expect(lateRaw.ProcessingState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))

		Expect(datasets.PublishDataset(ctx, datasetID, userID)).To(Succeed())

		published, err := datasets.ReadPublishedDatasetByID(ctx, datasetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(published.Status).To(Equal(model.Published))

		Expect(datasets.DeleteDataset(ctx, datasetID, userID)).To(Succeed())
		_, err = datasets.ReadDatasetForUser(ctx, datasetID, userID)
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("reports duplicate idempotency keys and missing datasets with domain errors", func() {
		idempotencyKey := uuid.New()
		userID := uuid.New()
		first := &model.Dataset{ID: uuid.New(), UserID: userID, Title: "duplicate-a"}
		second := &model.Dataset{ID: uuid.New(), UserID: userID, Title: "duplicate-b"}

		Expect(datasets.CreateDataset(ctx, first, idempotencyKey)).To(Succeed())
		err := datasets.CreateDataset(ctx, second, idempotencyKey)
		Expect(errors.Is(err, domainErrors.ErrResourceAlreadyExists)).To(BeTrue())

		_, err = datasets.ReadDatasetForUser(ctx, uuid.New(), userID)
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("persists source connectors through Postgres", func() {
		userID := uuid.New()
		connector := &model.SourceConnector{
			UserID: userID,
			Config: &model.ClickHouseConnCfg{
				Hostname:           "127.0.0.1",
				Port:               19000,
				DatabaseName:       "mlops",
				Username:           "user",
				Password:           "password",
				AuthenticationType: model.Master,
			},
		}

		Expect(connectors.CreateSourceConnector(ctx, connector, uuid.New())).To(Succeed())
		Expect(connector.ID).NotTo(Equal(uuid.Nil))
		Expect(connector.CatalogID).NotTo(Equal(uuid.Nil))

		read, err := connectors.ReadSourceConnector(ctx, connector.ID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.Config.GetStorageType()).To(Equal(model.ClickHouse))
		cfg, ok := read.Config.(*model.ClickHouseConnCfg)
		Expect(ok).To(BeTrue())
		Expect(cfg.DatabaseName).To(Equal("mlops"))

		Expect(connectors.DeleteSourceConnector(ctx, connector.ID, userID)).To(Succeed())
		_, err = connectors.ReadSourceConnector(ctx, connector.ID, userID)
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("returns no rows rather than failing when a requested page is beyond the dataset count", func() {
		userID := uuid.New()
		Expect(datasets.CreateDataset(ctx, &model.Dataset{
			ID:     uuid.New(),
			UserID: userID,
			Title:  "pagination-check",
		}, uuid.New())).To(Succeed())

		got, total, err := datasets.ReadDatasetsForUser(ctx, userID, transport.Pagination{Limit: 10, Page: 99}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(total).To(BeNumerically(">", 0))
		Expect(got).To(BeNil())
	})

	It("updates dataset processing state from Kafka materialization events", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		Expect(datasets.CreateDataset(ctx, &model.Dataset{
			ID:     datasetID,
			UserID: userID,
			Title:  "kafka-materialization",
		}, uuid.New())).To(Succeed())

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		topics := registrymessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		}
		Expect(purgeTopic(ctx, brokers, topics.FeatureMaterializer)).To(Succeed())

		subscriberMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "data-registry-integration-" + suffix,
			DlqURL:          "http://localhost:4566/data-registry-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, cancel)
		defer func() {
			_ = subscriberMessenger.Close(runCtx)
		}()

		publisherMessenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers: brokers,
			GroupID: "data-registry-integration-publisher-" + suffix,
			DlqURL:  "http://localhost:4566/data-registry-dev-env-queue/",
		}, cancel)
		publisher, err := publisherMessenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())
		subscriber, err := subscriberMessenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())

		materializationSubscriber := registrymessaging.NewMaterializationSubscriber(subscriber, datasets, topics)
		go func() {
			_ = materializationSubscriber.Start(runCtx)
		}()
		time.Sleep(4 * time.Second)

		Expect(publisher.Publish(runCtx, topics.FeatureMaterializer, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeRawSnapshotReady,
		}, &featurepb.RawSnapshotReadyEvent{
			RawSnapshotId:     uuid.NewString(),
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			StorageLocation:   "s3://local-dev-bucket/lakehouse/raw/data.parquet",
			TableNamespace:    "raw",
			TableName:         "kafka_materialization_raw",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
			ProcessingProfile: "TEXT_RAG",
		})).To(Succeed())

		Expect(publisher.Publish(runCtx, topics.FeatureMaterializer, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeFeatureSnapshotReady,
		}, &featurepb.FeatureSnapshotReadyEvent{
			FeatureSnapshotId: uuid.NewString(),
			RawSnapshotId:     uuid.NewString(),
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			StorageLocation:   "s3://local-dev-bucket/lakehouse/features/data.parquet",
			TableNamespace:    "features",
			TableName:         "kafka_materialization",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			SchemaVersion:     3,
			SchemaMetadata:    `{"columns":["title"]}`,
			ProcessingProfile: "TEXT_RAG",
		})).To(Succeed())

		Expect(publisher.Publish(runCtx, topics.FeatureMaterializer, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeEmbeddingSnapshotReady,
		}, &featurepb.EmbeddingSnapshotReadyEvent{
			EmbeddingSnapshotId: uuid.NewString(),
			FeatureSnapshotId:   uuid.NewString(),
			DatasetId:           datasetID.String(),
			UserId:              userID.String(),
			VectorStore:         "pgvector",
			CollectionName:      "kafka_materialization",
			EmbeddingDimensions: 384,
			EmbeddingCount:      1,
		})).To(Succeed())
		Expect(publisherMessenger.Close(runCtx)).To(Succeed())

		Eventually(func(g Gomega) {
			dataset, err := datasets.ReadDatasetForUser(ctx, datasetID, userID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(dataset.Location).To(Equal("s3://local-dev-bucket/lakehouse/features/data.parquet"))
			g.Expect(dataset.TableName).To(Equal("kafka_materialization"))
			g.Expect(dataset.ProcessingProfile).To(Equal(model.TextRAGProfile))
			g.Expect(dataset.ProcessingState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})

func purgeTopic(ctx context.Context, brokers, topic string) error {
	log.Trace("purgeTopic")

	Expect(sharedmessaging.CreateTopic(ctx, brokers, topic)).Should(Succeed())

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
	log.Trace("deleteTopicRecords")

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
	log.Trace("isRetriableTopicPurgeError")

	return isKafkaErrorCode(err, kafka.ErrNotLeaderForPartition) ||
		isKafkaErrorCode(err, kafka.ErrLeaderNotAvailable)
}

func isKafkaErrorCode(err error, code kafka.ErrorCode) bool {
	log.Trace("isKafkaErrorCode")

	var kafkaErr kafka.Error
	return errors.As(err, &kafkaErr) && kafkaErr.Code() == code
}
