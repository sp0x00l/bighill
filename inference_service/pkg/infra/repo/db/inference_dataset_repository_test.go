package db_test

import (
	"context"
	"errors"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"
	"lib/shared_lib/uuidutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("InferenceDatasetRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.InferenceDatasetRepository
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewInferenceDatasetRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	Describe("UpsertDataset", func() {
		It("upserts a dataset projection and scans the saved row", func() {
			dataset := validInferenceDataset()
			idempotencyKey := uuid.New()
			pool.nextRows = []pgx.Row{inferenceDatasetRow(dataset)}

			record, err := repository.UpsertDataset(ctx, dataset, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(dataset))
			Expect(pool.queryRowCalled).To(BeTrue())
			Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.inference_datasets"))
			Expect(pool.lastQuery).To(ContainSubstring("ON CONFLICT (dataset_id) DO UPDATE SET"))
			Expect(pool.lastQuery).To(ContainSubstring("WHERE EXCLUDED.dataset_version >= test_db.inference_datasets.dataset_version"))
			args := namedArgs(pool.lastArgs)
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: dataset.DatasetID, Valid: true}),
				HaveKeyWithValue("user_id", pgtype.UUID{Bytes: dataset.UserID, Valid: true}),
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: dataset.OrgID, Valid: true}),
				HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}),
				HaveKeyWithValue("dataset_version", dataset.DatasetVersion),
				HaveKeyWithValue("processing_state", model.DatasetProcessingEmbeddingsMaterialized.String()),
				HaveKeyWithValue("schema_metadata", dataset.SchemaMetadata),
				HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: dataset.EmbeddingSnapshotID, Valid: true}),
				HaveKeyWithValue("embedding_dimensions", dataset.EmbeddingDimensions),
				HaveKeyWithValue("embedding_model", dataset.EmbeddingModel),
			))
		})

		It("reads the current dataset when a stale version is ignored by the SQL guard", func() {
			staleDataset := validInferenceDataset()
			currentDataset := validInferenceDataset()
			currentDataset.DatasetID = staleDataset.DatasetID
			currentDataset.DatasetVersion = staleDataset.DatasetVersion + 1
			pool.nextRows = []pgx.Row{
				&repositoryRow{err: pgx.ErrNoRows},
				inferenceDatasetRow(currentDataset),
			}

			record, err := repository.UpsertDataset(ctx, staleDataset, uuid.New())

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(currentDataset))
			Expect(pool.queries).To(HaveLen(2))
			Expect(pool.queries[0]).To(ContainSubstring("INSERT INTO test_db.inference_datasets"))
			Expect(pool.queries[1]).To(ContainSubstring("FROM test_db.inference_datasets WHERE dataset_id = @dataset_id"))
			Expect(namedArgs(pool.args[1])).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: staleDataset.DatasetID, Valid: true}))
			Expect(namedArgs(pool.args[1])).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: staleDataset.OrgID, Valid: true}))
		})

		It("rejects empty schema metadata", func() {
			dataset := validInferenceDataset()
			dataset.SchemaMetadata = ""

			record, err := repository.UpsertDataset(ctx, dataset, uuid.New())

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(pool.queries).To(BeEmpty())
		})

		It("wraps database failures from the upsert", func() {
			pool.nextRows = []pgx.Row{&repositoryRow{err: errors.New("insert failed")}}

			record, err := repository.UpsertDataset(ctx, validInferenceDataset(), uuid.New())

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("upsert inference dataset: insert failed")))
		})

		It("propagates not found when stale upsert fallback cannot find the dataset", func() {
			pool.nextRows = []pgx.Row{
				&repositoryRow{err: pgx.ErrNoRows},
				&repositoryRow{err: pgx.ErrNoRows},
			}

			record, err := repository.UpsertDataset(ctx, validInferenceDataset(), uuid.New())

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrDatasetNotFound)).To(BeTrue())
		})
	})

	Describe("ReadDataset", func() {
		It("reads a dataset by id", func() {
			dataset := validInferenceDataset()
			pool.nextRows = []pgx.Row{inferenceDatasetRow(dataset)}

			record, err := repository.ReadDataset(ctx, dataset.OrgID, dataset.DatasetID)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(dataset))
			Expect(pool.lastQuery).To(ContainSubstring("SELECT dataset_id::text"))
			Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.inference_datasets WHERE dataset_id = @dataset_id AND org_id = @org_id"))
			Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: dataset.OrgID, Valid: true}))
		})

		It("maps empty optional snapshot ids to uuid.Nil", func() {
			dataset := validInferenceDataset()
			dataset.RawSnapshotID = uuid.Nil
			dataset.FeatureSnapshotID = uuid.Nil
			dataset.EmbeddingSnapshotID = uuid.Nil
			pool.nextRows = []pgx.Row{inferenceDatasetRow(dataset)}

			record, err := repository.ReadDataset(ctx, dataset.OrgID, dataset.DatasetID)

			Expect(err).NotTo(HaveOccurred())
			Expect(record.RawSnapshotID).To(Equal(uuid.Nil))
			Expect(record.FeatureSnapshotID).To(Equal(uuid.Nil))
			Expect(record.EmbeddingSnapshotID).To(Equal(uuid.Nil))
		})

		It("returns a domain not-found error when no dataset row exists", func() {
			record, err := repository.ReadDataset(ctx, uuid.New(), uuid.New())

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrDatasetNotFound)).To(BeTrue())
		})
	})
})

func validInferenceDataset() *model.InferenceDataset {
	return &model.InferenceDataset{
		DatasetID:                uuid.New(),
		UserID:                   uuid.New(),
		OrgID:                    uuid.New(),
		DatasetVersion:           3,
		ProcessingState:          model.DatasetProcessingEmbeddingsMaterialized,
		StorageLocation:          "s3://datasets/user/dataset/features.parquet",
		TableNamespace:           "feature_store",
		TableName:                "dataset_features",
		TableFormat:              "PARQUET",
		CatalogProvider:          "POLARIS",
		ProcessingProfile:        "TEXT_RAG_PROCESSING_PROFILE",
		SchemaVersion:            2,
		SchemaMetadata:           `{"columns":[{"name":"text","type":"string"}]}`,
		RawSnapshotID:            uuid.New(),
		FeatureSnapshotID:        uuid.New(),
		EmbeddingSnapshotID:      uuid.New(),
		VectorStore:              "pgvector",
		CollectionName:           "dataset_embeddings",
		EmbeddingDimensions:      384,
		EmbeddingCount:           42,
		EmbeddingStrategyVersion: "rag-v1",
		EmbeddingChunkerName:     "go-token-window",
		EmbeddingChunkerVersion:  "v1",
		EmbeddingChunkSize:       384,
		EmbeddingChunkOverlap:    64,
		EmbeddingProvider:        "ollama",
		EmbeddingModel:           "bge-small-en-v1.5",
	}
}

func inferenceDatasetRow(dataset *model.InferenceDataset) pgx.Row {
	return &repositoryRow{values: []any{
		dataset.DatasetID.String(),
		dataset.UserID.String(),
		dataset.OrgID.String(),
		dataset.DatasetVersion,
		dataset.ProcessingState.String(),
		dataset.StorageLocation,
		dataset.TableNamespace,
		dataset.TableName,
		dataset.TableFormat,
		dataset.CatalogProvider,
		dataset.ProcessingProfile,
		dataset.SchemaVersion,
		dataset.SchemaMetadata,
		uuidutil.StringOrEmpty(dataset.RawSnapshotID),
		uuidutil.StringOrEmpty(dataset.FeatureSnapshotID),
		uuidutil.StringOrEmpty(dataset.EmbeddingSnapshotID),
		dataset.VectorStore,
		dataset.CollectionName,
		dataset.EmbeddingDimensions,
		dataset.EmbeddingCount,
		dataset.EmbeddingStrategyVersion,
		dataset.EmbeddingChunkerName,
		dataset.EmbeddingChunkerVersion,
		dataset.EmbeddingChunkSize,
		dataset.EmbeddingChunkOverlap,
		dataset.EmbeddingProvider,
		dataset.EmbeddingModel,
	}}
}
