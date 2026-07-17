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
	featuretemporal "feature_materializer_service/pkg/infra/temporalworker"
	featurepb "lib/data_contracts_lib/feature_materializer"
	ingestionpb "lib/data_contracts_lib/ingestion"
	corebucket "lib/shared_lib/bucket"
	"lib/shared_lib/ctxutil"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

func TestFeatureMaterializerIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer integration test suite")
}

var _ = Describe("Feature materializer integration", Ordered, func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		database    *dbconn.Database
		snapshots   *repo.SnapshotRepository
		snapshotUOW *shareduow.UnitOfWork
		rawUsecase  usecase.RawSnapshotUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		dbName := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_NAME", "bighill_feature_materializer_db")
		connectionString := testPostgresConnectionString(dbName)

		var err error
		database, err = dbconn.InitDatabase(ctx, dbName, connectionString, log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		snapshots = repo.NewSnapshotRepository(database)
		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		snapshotUOW = shareduow.New(database.Pool, shareduow.WithTransactionalOutbox(orderedOutbox))
		rawUsecase = usecase.NewRawSnapshotUsecase(snapshots, snapshotUOW, featuremessaging.NewSnapshotEventBuilder(featuremessaging.MaterializationTopics{
			FeatureMaterializer: "feature_materializer",
		}), integrationRawSnapshotWriter{})
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
		Expect(upsertFeatureMaterializerTenant(ctx, database, datasetFile.UserID)).To(Succeed())
		tenantCtx := ctxutil.WithActorOrg(ctx, datasetFile.UserID, datasetFile.OrgID)

		rawSnapshot, err := rawUsecase.MaterializeRawSnapshot(tenantCtx, datasetFile, idempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(rawSnapshot.RawSnapshotID).NotTo(Equal(uuid.Nil))
		Expect(rawSnapshot.Status).To(Equal(model.SnapshotStatusReady))

		replayedRaw, err := rawUsecase.MaterializeRawSnapshot(tenantCtx, datasetFile, idempotencyKey)
		Expect(err).To(HaveOccurred())
		Expect(replayedRaw.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		_, ok := domain.IsRawSnapshotAlreadyMaterialized(err)
		Expect(ok).To(BeTrue())

		var featureSnapshot *model.FeatureSnapshot
		err = snapshotUOW.Do(tenantCtx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
			out, err := snapshots.SavePendingFeatureSnapshot(ctx, tx, rawSnapshot.RawSnapshotID, uuid.New())
			if err != nil {
				return err
			}
			featureSnapshot = out
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(featureSnapshot.RawSnapshotID).To(Equal(rawSnapshot.RawSnapshotID))
		featureSnapshot.StorageLocation = "s3://lakehouse/features/snapshot.parquet"
		featureSnapshot.SchemaVersion = 1
		featureSnapshot.SchemaMetadata = "{}"
		Expect(snapshotUOW.Do(tenantCtx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
			return snapshots.MarkFeatureReady(ctx, tx, featureSnapshot)
		})).To(Succeed())

		embeddingStrategy := integrationEmbeddingStrategy()
		embeddingIdempotencyKey := uuid.New()
		var embeddingSnapshot *model.EmbeddingSnapshot
		err = snapshotUOW.Do(tenantCtx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
			out, err := snapshots.SavePendingEmbeddingSnapshot(ctx, tx, featureSnapshot.FeatureSnapshotID, embeddingIdempotencyKey, embeddingStrategy)
			if err != nil {
				return err
			}
			embeddingSnapshot = out
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(embeddingSnapshot.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(embeddingSnapshot.StrategyVersion).To(Equal(embeddingStrategy.StrategyVersion))
		embeddingSnapshot.VectorStore = "pgvector"
		embeddingSnapshot.CollectionName = "movies"
		embeddingSnapshot.EmbeddingDimensions = embeddingStrategy.EmbeddingDimensions
		embeddingSnapshot.EmbeddingCount = 3
		embeddingSnapshot.StrategyVersion = embeddingStrategy.StrategyVersion
		embeddingSnapshot.ChunkerName = embeddingStrategy.ChunkerName
		embeddingSnapshot.ChunkerVersion = embeddingStrategy.ChunkerVersion
		embeddingSnapshot.ChunkSize = embeddingStrategy.ChunkSize
		embeddingSnapshot.ChunkOverlap = embeddingStrategy.ChunkOverlap
		embeddingSnapshot.EmbeddingProvider = embeddingStrategy.EmbeddingProvider
		embeddingSnapshot.EmbeddingModel = embeddingStrategy.EmbeddingModel
		Expect(snapshotUOW.Do(tenantCtx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
			return snapshots.MarkEmbeddingReady(ctx, tx, embeddingSnapshot)
		})).To(Succeed())

		readFeature, err := snapshots.ReadFeatureSnapshot(tenantCtx, featureSnapshot.FeatureSnapshotID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readFeature.Status).To(Equal(model.SnapshotStatusReady))

		readEmbedding, err := snapshots.ReadEmbeddingByIdempotencyKey(tenantCtx, embeddingIdempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(readEmbedding.VectorStore).To(Equal("pgvector"))
	})

	It("materializes an uploaded dataset file through Kafka", func() {
		brokers := env.WithDefaultString("KAFKA_BROKER", "localhost:9092")
		suffix := fmt.Sprintf("%d", rand.Int64())
		datasetTopic := "ingestion"
		featureMaterializerTopic := "feature_materializer"
		taskQueue := "feature-materializer-integration-" + suffix

		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		messenger := sharedmessaging.NewMessenger(sharedmessaging.MessengerConfig{
			Brokers:         brokers,
			GroupID:         "feature-materializer-integration-" + suffix,
			DlqURL:          "http://localhost:4566/feature-materializer-dev-env-queue/",
			AutoOffsetReset: "earliest",
		}, cancel)
		defer func() {
			_ = messenger.Close(runCtx)
		}()

		publisher, err := messenger.Publisher(runCtx)
		Expect(err).NotTo(HaveOccurred())
		relayPublisher, ok := publisher.(sharedmessaging.RelayPublisher)
		Expect(ok).To(BeTrue())
		subscriber, err := messenger.Subscriber(runCtx)
		Expect(err).NotTo(HaveOccurred())

		outboxSignal := make(chan struct{}, 1)
		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		signaledOutbox := sharedmessaging.NewSignaledOutbox(outboxWriter, outboxSignal)
		relayOutbox, ok := signaledOutbox.(sharedmessaging.RelayOutbox)
		Expect(ok).To(BeTrue())
		workflowSnapshotUnitOfWork := shareduow.New(database.Pool,
			shareduow.WithTransactionalOutbox(orderedOutbox),
			shareduow.WithOutboxSignal(func() { sharedmessaging.NotifyOutboxSignal(outboxSignal) }),
		)
		outboxRelay := sharedmessaging.NewOutboxRelay(relayOutbox, relayPublisher, sharedmessaging.OutboxRelayConfig{
			PollInterval:   100 * time.Millisecond,
			FailureBackoff: 250 * time.Millisecond,
			BatchSize:      10,
			Signal:         outboxSignal,
			InstanceID:     "feature-materializer-integration-" + suffix,
			LeaseDuration:  time.Second,
		})
		go func() {
			_ = outboxRelay.Run(runCtx)
		}()

		artifactStore, err := materialization.NewObjectArtifactStore(runCtx, "local-dev-bucket", "local-dev", 10*1024*1024)
		Expect(err).NotTo(HaveOccurred())
		rawWriter := materialization.NewRawSnapshotWriter(artifactStore)
		featureBuilder := materialization.NewFeatureSnapshotBuilder(artifactStore)
		embeddingStrategy := integrationEmbeddingStrategy()
		embeddingWriter := materialization.NewEmbeddingWriter(artifactStore, integrationEmbeddingProvider{dimensions: embeddingStrategy.EmbeddingDimensions}, nil, embeddingStrategy, "pgvector", 10)
		rawDispatcher := materialization.NewRawSnapshotWriterDispatcher(rawWriter)
		featureDispatcher := materialization.NewFeatureSnapshotBuilderDispatcher(featureBuilder)
		embeddingDispatcher := materialization.NewEmbeddingWriterDispatcher(embeddingWriter)
		graphExtractor := materialization.NewDisabledGraphExtractor()
		snapshotEventBuilder := featuremessaging.NewSnapshotEventBuilder(featuremessaging.MaterializationTopics{
			FeatureMaterializer: featureMaterializerTopic,
		})
		temporalClient, err := client.Dial(client.Options{
			HostPort:  env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_ADDRESS", env.WithDefaultString("TEMPORAL_ADDRESS", "localhost:7233")),
			Namespace: env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_TEMPORAL_NAMESPACE", env.WithDefaultString("TEMPORAL_NAMESPACE", "default")),
		})
		Expect(err).NotTo(HaveOccurred())
		defer temporalClient.Close()

		activities := featuretemporal.NewMaterializationActivities(
			usecase.NewRawSnapshotUsecase(snapshots, workflowSnapshotUnitOfWork, snapshotEventBuilder, rawDispatcher),
			usecase.NewFeatureSnapshotUsecase(snapshots, workflowSnapshotUnitOfWork, snapshotEventBuilder, snapshots, featureDispatcher),
			usecase.NewEmbeddingMaterializationUsecase(snapshots, workflowSnapshotUnitOfWork, snapshotEventBuilder, snapshots, embeddingDispatcher),
			usecase.NewGraphMaterializationUsecase(snapshots, workflowSnapshotUnitOfWork, snapshotEventBuilder, graphExtractor),
		)
		worker := featuretemporal.NewMaterializationWorker(temporalClient, taskQueue, activities)
		Expect(worker.Start()).To(Succeed())
		defer worker.Stop()

		workflowStarter := featuretemporal.NewMaterializationWorkflowStarter(temporalClient, taskQueue, embeddingStrategy, usecase.GraphWorkflowConfig{})
		materializationSubscriber := featuremessaging.NewMaterializationSubscriber(
			subscriber,
			workflowStarter,
			[]string{datasetTopic},
		)

		go func() {
			_ = materializationSubscriber.Start(runCtx)
		}()
		time.Sleep(750 * time.Millisecond)

		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		Expect(upsertFeatureMaterializerTenant(ctx, database, userID)).To(Succeed())
		storageKey := fmt.Sprintf("raw/%s/upload.csv", datasetID)
		bucket := corebucket.NewBucket(runCtx, "local-dev", 10*1024*1024)
		Expect(bucket.Upload(runCtx, "local-dev-bucket", storageKey, "text/csv", strings.NewReader("title,views\nIntro,10\nNext,20\n"))).To(Succeed())
		storageLocation := "s3://local-dev-bucket/" + storageKey

		err = publisher.Publish(runCtx, datasetTopic, sharedmessaging.Message{
			ResourceKey: datasetID,
			MsgType:     sharedmessaging.MsgTypeDatasetFileUploaded,
		}, &ingestionpb.DatasetFileUploadedEvent{
			DatasetId:         datasetID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			StorageLocation:   storageLocation,
			ContentType:       "text/csv",
			FileExtension:     "csv",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			rawStatus, featureStatus, embeddingStatus, embeddingCount := materializationState(ctxutil.WithActorOrg(ctx, userID, orgID), database, datasetID)
			g.Expect(rawStatus).To(Equal(model.SnapshotStatusReady.String()))
			g.Expect(featureStatus).To(Equal(model.SnapshotStatusReady.String()))
			g.Expect(embeddingStatus).To(Equal(model.SnapshotStatusReady.String()))
			g.Expect(embeddingCount).To(Equal(int64(2)))
			g.Expect(outboxSentCount(ctx, database, datasetID)).To(Equal(3))
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())

		featureReadyEvent, err := readFeatureSnapshotReadyEvent(ctx, database, datasetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(featureReadyEvent.GetStorageLocation()).To(MatchRegexp(`^s3://local-dev-bucket/lakehouse/features/.+\.parquet$`))
	})
})

func validIntegrationDatasetFile() *model.DatasetFile {
	return &model.DatasetFile{
		DatasetID:       uuid.New(),
		UserID:          uuid.New(),
		OrgID:           uuid.New(),
		StorageLocation: "s3://local-dev-bucket/raw/user/dataset/file.csv",
		ContentType:     "text/csv",
		FileExtension:   "csv",
		TableNamespace:  "default",
		TableName:       "dataset_movies",
		TableFormat:     "PARQUET",
		CatalogProvider: "LOCAL",
	}
}

func integrationEmbeddingStrategy() model.EmbeddingStrategy {
	return model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
		StrategyVersion:     "rag-v1",
		ChunkerName:         "go-token-window",
		ChunkerVersion:      "v1",
		ChunkSize:           512,
		ChunkOverlap:        64,
		EmbeddingProvider:   "tei",
		EmbeddingModel:      "test-model",
		EmbeddingDimensions: 384,
	})
}

type integrationEmbeddingProvider struct {
	dimensions int
}

type integrationRawSnapshotWriter struct{}

func (integrationRawSnapshotWriter) WriteRawSnapshot(_ context.Context, datasetFile *model.DatasetFile, rawSnapshot *model.RawSnapshot) (*model.RawSnapshot, error) {
	rawSnapshot.StorageLocation = fmt.Sprintf("s3://local-dev-bucket/lakehouse/raw/%s/%s/data.parquet", datasetFile.DatasetID, rawSnapshot.RawSnapshotID)
	rawSnapshot.ContentType = datasetFile.ContentType
	rawSnapshot.FileExtension = datasetFile.FileExtension
	rawSnapshot.TableNamespace = datasetFile.TableNamespace
	rawSnapshot.TableName = datasetFile.TableName
	rawSnapshot.TableFormat = datasetFile.TableFormat
	rawSnapshot.CatalogProvider = datasetFile.CatalogProvider
	rawSnapshot.ProcessingProfile = datasetFile.ProcessingProfile
	rawSnapshot.SchemaVersion = 1
	rawSnapshot.SchemaMetadata = "{}"
	rawSnapshot.Status = model.SnapshotStatusReady
	return rawSnapshot, nil
}

func (p integrationEmbeddingProvider) Dimensions() int {
	return p.dimensions
}

func (p integrationEmbeddingProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for textIndex := range texts {
		vector := make([]float32, p.dimensions)
		for i := range vector {
			vector[i] = float32((textIndex+i)%7+1) / 10
		}
		vectors[textIndex] = vector
	}
	return vectors, nil
}

func truncateSnapshots(ctx context.Context, database *dbconn.Database) error {
	for _, table := range []string{
		"outbox_messages",
		"graph_node_chunks",
		"graph_edges",
		"graph_nodes",
		"graph_snapshots",
		"embedding_records",
		"embedding_snapshots",
		"feature_snapshots",
		"raw_snapshots",
		"tenants",
	} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

func upsertFeatureMaterializerTenant(ctx context.Context, database *dbconn.Database, userID uuid.UUID) error {
	ctx = ctxutil.WithSystemContext(ctx)
	_, err := database.Pool.Exec(ctx, `
		INSERT INTO `+database.Name+`.tenants (id, email, deleted)
		VALUES ($1, $2, false)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, deleted = false
	`, userID, userID.String()+"@example.test")
	return err
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

func outboxSentCount(ctx context.Context, database *dbconn.Database, datasetID uuid.UUID) int {
	var count int
	err := database.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM `+database.Name+`.outbox_messages
		WHERE resource_key = $1 AND status = 'SENT'
	`, datasetID).Scan(&count)
	Expect(err).NotTo(HaveOccurred())
	return count
}

func readFeatureSnapshotReadyEvent(ctx context.Context, database *dbconn.Database, datasetID uuid.UUID) (*featurepb.FeatureSnapshotReadyEvent, error) {
	var payload []byte
	err := database.Pool.QueryRow(ctx, `
		SELECT payload
		FROM `+database.Name+`.outbox_messages
		WHERE resource_key = $1 AND event_type = 'feature_snapshot_ready'
		ORDER BY created_at DESC
		LIMIT 1
	`, datasetID).Scan(&payload)
	if err != nil {
		return nil, err
	}

	var envelope sharedmessaging.Message
	if err := envelope.Deserialize(ctx, payload); err != nil {
		return nil, err
	}
	event := &featurepb.FeatureSnapshotReadyEvent{}
	if err := envelope.DeserializePayload(event); err != nil {
		return nil, err
	}
	return event, nil
}

func testPostgresConnectionString(dbName string) string {
	user := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_USER", "bighill_feature_materializer_db_user")
	password := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
	host := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1"))
	port := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_PORT", env.WithDefaultString("PGPORT", "5432"))
	sslMode := env.WithDefaultString("FEATURE_MATERIALIZER_SERVICE_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable"))
	maxConnections := env.WithDefaultInt("FEATURE_MATERIALIZER_SERVICE_DB_MAX_CONNECTIONS", "20")
	if value := os.Getenv("FEATURE_MATERIALIZER_SERVICE_DB_NAME"); value != "" {
		dbName = value
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", url.QueryEscape(user), url.QueryEscape(password), host, port, dbName, q.Encode())
}
