package integration_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"feature_materializer_service/pkg/infra/materialization"
	featuremessaging "feature_materializer_service/pkg/infra/network/messaging"
	repo "feature_materializer_service/pkg/infra/repo/db"
	datasetpb "lib/data_contracts_lib/dataset"
	corebucket "lib/shared_lib/bucket"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestFeatureMaterializerIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer integration test suite")
}

var _ = Describe("Feature materializer integration", Ordered, func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		database   *dbconn.Database
		snapshots  *repo.SnapshotRepository
		rawUsecase usecase.RawSnapshotUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		dbName := env.WithDefaultString("FEATURE_MATERIALIZER_DB_NAME", "bighill_feature_materializer_db")
		connectionString := testPostgresConnectionString(dbName)

		var err error
		database, err = dbconn.InitDatabase(ctx, dbName, connectionString, log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		snapshots = repo.NewSnapshotRepository(database)
		rawUsecase = usecase.NewRawSnapshotUsecase(snapshots, nil)
	})

	BeforeEach(func() {
		Expect(truncateSnapshots(ctx, database)).To(Succeed())
	})

	AfterAll(func() {
		if database != nil {
			database.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("has pgvector installed in the service database", func() {
		var installed bool
		err := database.Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')").Scan(&installed)
		Expect(err).NotTo(HaveOccurred())
		Expect(installed).To(BeTrue())
	})

	It("persists raw, feature, and embedding snapshots with database idempotency", func() {
		idempotencyKey := uuid.New()
		datasetFile := validIntegrationDatasetFile()

		rawSnapshot, err := rawUsecase.MaterializeRawSnapshot(ctx, datasetFile, idempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(rawSnapshot.RawSnapshotID).NotTo(Equal(uuid.Nil))
		Expect(rawSnapshot.Status).To(Equal(model.SnapshotStatusPending))

		replayedRaw, err := rawUsecase.MaterializeRawSnapshot(ctx, datasetFile, idempotencyKey)
		Expect(err).To(HaveOccurred())
		Expect(replayedRaw.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		_, ok := domain.IsRawSnapshotAlreadyMaterialized(err)
		Expect(ok).To(BeTrue())

		featureSnapshot, err := snapshots.SavePendingFeatureSnapshot(ctx, rawSnapshot.RawSnapshotID, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(featureSnapshot.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		featureSnapshot.StorageLocation = "s3://lakehouse/features/snapshot.parquet"
		featureSnapshot.SchemaVersion = 1
		featureSnapshot.SchemaMetadata = "{}"
		Expect(snapshots.MarkFeatureReady(ctx, featureSnapshot)).To(Succeed())

		embeddingIdempotencyKey := uuid.New()
		embeddingSnapshot, err := snapshots.SavePendingEmbeddingSnapshot(ctx, featureSnapshot.FeatureSnapshotID, embeddingIdempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(embeddingSnapshot.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		embeddingSnapshot.VectorStore = "pgvector"
		embeddingSnapshot.CollectionName = "movies"
		embeddingSnapshot.EmbeddingDimensions = 384
		embeddingSnapshot.EmbeddingCount = 3
		Expect(snapshots.MarkEmbeddingReady(ctx, embeddingSnapshot)).To(Succeed())

		readFeature, err := snapshots.ReadFeatureSnapshot(ctx, featureSnapshot.FeatureSnapshotID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readFeature.Status).To(Equal(model.SnapshotStatusReady))

		readEmbedding, err := snapshots.ReadEmbeddingByIdempotencyKey(ctx, embeddingIdempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(readEmbedding.VectorStore).To(Equal("pgvector"))
	})

	It("materializes an uploaded dataset file through Kafka", func() {
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		datasetTopic := "data_ingestion"
		featureMaterializerTopic := "feature_materializer"
		topics := featuremessaging.MaterializationTopics{
			FeatureMaterializer: featureMaterializerTopic,
		}

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		messenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "feature-materializer-integration-" + suffix,
			DlqURL:          "http://localhost:4566/feature-materializer-dev-env-queue/",
			OutboxURL:       "noop://local",
			AutoOffsetReset: "earliest",
		}, cancel)
		defer func() {
			_ = messenger.Close(runCtx)
		}()

		publisher, err := messenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())
		subscriber, err := messenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())

		artifactStore, err := materialization.NewObjectArtifactStore(runCtx, "local-dev-bucket", "local-dev", 10*1024*1024)
		Expect(err).NotTo(HaveOccurred())
		rawWriter := materialization.NewRawSnapshotWriter(artifactStore)
		featureBuilder := materialization.NewFeatureSnapshotBuilder(artifactStore)
		embeddingWriter := materialization.NewEmbeddingWriter(artifactStore, snapshots, materialization.NewDeterministicEmbeddingProvider(384), "pgvector", 10)
		eventPublisher := featuremessaging.NewMaterializationEventPublisher(publisher, topics)
		materializationSubscriber := featuremessaging.NewMaterializationSubscriber(
			subscriber,
			usecase.NewRawSnapshotUsecase(snapshots, rawWriter),
			usecase.NewFeatureSnapshotUsecase(snapshots, snapshots, featureBuilder),
			usecase.NewEmbeddingMaterializationUsecase(snapshots, snapshots, embeddingWriter),
			eventPublisher,
			[]string{datasetTopic, featureMaterializerTopic},
		)

		go func() {
			_ = materializationSubscriber.Start(runCtx)
		}()
		time.Sleep(750 * time.Millisecond)

		datasetID := uuid.New()
		userID := uuid.New()
		storageKey := fmt.Sprintf("raw/%s/upload.csv", datasetID)
		bucket := corebucket.NewBucket(runCtx, "local-dev", 10*1024*1024)
		Expect(bucket.Upload(runCtx, "local-dev-bucket", storageKey, "text/csv", strings.NewReader("title,views\nIntro,10\nNext,20\n"))).To(Succeed())
		storageLocation := "s3://local-dev-bucket/" + storageKey

		err = publisher.Publish(runCtx, datasetTopic, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeDatasetFileUploaded,
		}, &datasetpb.DatasetFileUploadedEvent{
			DatasetId:       datasetID.String(),
			UserId:          userID.String(),
			StorageLocation: storageLocation,
			ContentType:     "text/csv",
			FileExtension:   "csv",
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     "PARQUET",
			CatalogProvider: "LOCAL",
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			rawStatus, featureStatus, embeddingStatus, embeddingCount := materializationState(ctx, database, datasetID)
			g.Expect(rawStatus).To(Equal(model.SnapshotStatusReady.String()))
			g.Expect(featureStatus).To(Equal(model.SnapshotStatusReady.String()))
			g.Expect(embeddingStatus).To(Equal(model.SnapshotStatusReady.String()))
			g.Expect(embeddingCount).To(Equal(int64(2)))
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})

func validIntegrationDatasetFile() *model.DatasetFile {
	return &model.DatasetFile{
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		StorageLocation: "s3://local-dev-bucket/raw/user/dataset/file.csv",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "default",
		TableName:       "dataset_movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
	}
}

func truncateSnapshots(ctx context.Context, database *dbconn.Database) error {
	for _, table := range []string{"embedding_records", "embedding_snapshots", "feature_snapshots", "raw_snapshots"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

func materializationState(ctx context.Context, database *dbconn.Database, datasetID uuid.UUID) (string, string, string, int64) {
	var rawStatus, featureStatus, embeddingStatus string
	var embeddingCount int64
	err := database.Pool.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT status::text FROM `+database.Name+`.raw_snapshots WHERE dataset_id = $1 ORDER BY created_at DESC LIMIT 1), ''),
			COALESCE((SELECT status::text FROM `+database.Name+`.feature_snapshots WHERE dataset_id = $1 ORDER BY created_at DESC LIMIT 1), ''),
			COALESCE((SELECT status::text FROM `+database.Name+`.embedding_snapshots WHERE dataset_id = $1 ORDER BY created_at DESC LIMIT 1), ''),
			COALESCE((SELECT embedding_count FROM `+database.Name+`.embedding_snapshots WHERE dataset_id = $1 ORDER BY created_at DESC LIMIT 1), 0)
	`, datasetID).Scan(&rawStatus, &featureStatus, &embeddingStatus, &embeddingCount)
	Expect(err).NotTo(HaveOccurred())
	return rawStatus, featureStatus, embeddingStatus, embeddingCount
}

func testPostgresConnectionString(dbName string) string {
	user := env.WithDefaultString("FEATURE_MATERIALIZER_DB_USER", "bighill_feature_materializer_db_user")
	password := env.WithDefaultString("FEATURE_MATERIALIZER_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
	host := env.WithDefaultString("FEATURE_MATERIALIZER_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1"))
	port := env.WithDefaultString("FEATURE_MATERIALIZER_DB_PORT", env.WithDefaultString("PGPORT", "5432"))
	sslMode := env.WithDefaultString("FEATURE_MATERIALIZER_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable"))
	maxConnections := env.WithDefaultInt("FEATURE_MATERIALIZER_DB_MAX_CONNECTIONS", "20")
	if value := os.Getenv("FEATURE_MATERIALIZER_DB_NAME"); value != "" {
		dbName = value
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", url.QueryEscape(user), url.QueryEscape(password), host, port, dbName, q.Encode())
}
