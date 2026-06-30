package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	repo "feature_materializer_service/pkg/infra/repo/db"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"

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
		Expect(snapshots.MarkFeatureReady(ctx, featureSnapshot.FeatureSnapshotID, "s3://lakehouse/features/snapshot.parquet")).To(Succeed())

		embeddingIdempotencyKey := uuid.New()
		embeddingSnapshot, err := snapshots.SavePendingEmbeddingSnapshot(ctx, featureSnapshot.FeatureSnapshotID, embeddingIdempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(embeddingSnapshot.FeatureSnapshotID).To(Equal(featureSnapshot.FeatureSnapshotID))
		Expect(snapshots.MarkEmbeddingReady(ctx, embeddingSnapshot.EmbeddingSnapshotID, "pgvector", "movies")).To(Succeed())

		readFeature, err := snapshots.ReadFeatureSnapshot(ctx, featureSnapshot.FeatureSnapshotID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readFeature.Status).To(Equal(model.SnapshotStatusReady))

		readEmbedding, err := snapshots.ReadEmbeddingByIdempotencyKey(ctx, embeddingIdempotencyKey)
		Expect(err).NotTo(HaveOccurred())
		Expect(readEmbedding.VectorStore).To(Equal("pgvector"))
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
	for _, table := range []string{"embedding_snapshots", "feature_snapshots", "raw_snapshots"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
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
