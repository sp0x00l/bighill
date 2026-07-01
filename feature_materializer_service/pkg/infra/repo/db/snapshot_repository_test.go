package db_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featuredb "feature_materializer_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSnapshotRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer db unit test suite")
}

type testConnectionPool struct {
	CloseCalled         bool
	QueryRowCalled      bool
	QueryRowCalledCount int
	ExecCalled          bool
	ExecCalledCount     int
	QueryCalls          []string
	QueryArgs           [][]any
	ExecCalls           []string
	ExecArgs            [][]any
	NextRows            []pgx.Row
	NextRowsAffected    int64
	NextError           error
	NextExecErrors      []error
}

func (p *testConnectionPool) Close() {
	p.CloseCalled = true
}

func (p *testConnectionPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.QueryRowCalled = true
	p.QueryRowCalledCount++
	p.QueryCalls = append(p.QueryCalls, sql)
	p.QueryArgs = append(p.QueryArgs, args)
	if len(p.NextRows) > 0 {
		nextRow := p.NextRows[0]
		p.NextRows = p.NextRows[1:]
		return nextRow
	}
	return errorRow{err: pgx.ErrNoRows}
}

func (p *testConnectionPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, p.NextError
}

func (p *testConnectionPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ExecCalled = true
	p.ExecCalledCount++
	p.ExecCalls = append(p.ExecCalls, sql)
	p.ExecArgs = append(p.ExecArgs, args)
	nextErr := p.NextError
	if nextErr == nil && len(p.NextExecErrors) > 0 {
		nextErr = p.NextExecErrors[0]
		p.NextExecErrors = p.NextExecErrors[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.NextRowsAffected)), nextErr
}

func (p *testConnectionPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, p.NextError
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(...any) error {
	return r.err
}

type rawSnapshotRow struct {
	RawSnapshotID     uuid.UUID
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	StorageLocation   string
	ContentType       string
	FileExtension     string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile string
	SchemaVersion     int
	SchemaMetadata    string
	Status            string
	FailureReason     string
}

func (r rawSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.RawSnapshotID.String()
	*(dest[1].(*string)) = r.DatasetID.String()
	*(dest[2].(*string)) = r.UserID.String()
	*(dest[3].(*string)) = r.StorageLocation
	*(dest[4].(*string)) = r.ContentType
	*(dest[5].(*string)) = r.FileExtension
	*(dest[6].(*string)) = r.TableNamespace
	*(dest[7].(*string)) = r.TableName
	*(dest[8].(*string)) = r.TableFormat
	*(dest[9].(*string)) = r.CatalogProvider
	*(dest[10].(*string)) = r.ProcessingProfile
	*(dest[11].(*int)) = r.SchemaVersion
	*(dest[12].(*string)) = r.SchemaMetadata
	*(dest[13].(*string)) = r.Status
	*(dest[14].(*string)) = r.FailureReason
	return nil
}

type featureSnapshotRow struct {
	FeatureSnapshotID uuid.UUID
	RawSnapshotID     uuid.UUID
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	StorageLocation   string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile string
	SchemaVersion     int
	SchemaMetadata    string
	Status            string
	FailureReason     string
}

func (r featureSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.FeatureSnapshotID.String()
	*(dest[1].(*string)) = r.RawSnapshotID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*string)) = r.UserID.String()
	*(dest[4].(*string)) = r.StorageLocation
	*(dest[5].(*string)) = r.TableNamespace
	*(dest[6].(*string)) = r.TableName
	*(dest[7].(*string)) = r.TableFormat
	*(dest[8].(*string)) = r.CatalogProvider
	*(dest[9].(*string)) = r.ProcessingProfile
	*(dest[10].(*int)) = r.SchemaVersion
	*(dest[11].(*string)) = r.SchemaMetadata
	*(dest[12].(*string)) = r.Status
	*(dest[13].(*string)) = r.FailureReason
	return nil
}

type embeddingSnapshotRow struct {
	EmbeddingSnapshotID uuid.UUID
	FeatureSnapshotID   uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	VectorStore         string
	CollectionName      string
	EmbeddingDimensions int
	EmbeddingCount      int64
	StrategyVersion     string
	ChunkerName         string
	ChunkerVersion      string
	ChunkSize           int
	ChunkOverlap        int
	EmbeddingProvider   string
	EmbeddingModel      string
	Status              string
	FailureReason       string
}

func (r embeddingSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EmbeddingSnapshotID.String()
	*(dest[1].(*string)) = r.FeatureSnapshotID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*string)) = r.UserID.String()
	*(dest[4].(*string)) = r.VectorStore
	*(dest[5].(*string)) = r.CollectionName
	*(dest[6].(*int)) = r.EmbeddingDimensions
	*(dest[7].(*int64)) = r.EmbeddingCount
	*(dest[8].(*string)) = r.StrategyVersion
	*(dest[9].(*string)) = r.ChunkerName
	*(dest[10].(*string)) = r.ChunkerVersion
	*(dest[11].(*int)) = r.ChunkSize
	*(dest[12].(*int)) = r.ChunkOverlap
	*(dest[13].(*string)) = r.EmbeddingProvider
	*(dest[14].(*string)) = r.EmbeddingModel
	*(dest[15].(*string)) = r.Status
	*(dest[16].(*string)) = r.FailureReason
	return nil
}

var _ = Describe("SnapshotRepository", func() {
	var (
		ctx            context.Context
		poolMock       *testConnectionPool
		repository     *featuredb.SnapshotRepository
		datasetID      uuid.UUID
		userID         uuid.UUID
		rawSnapshotID  uuid.UUID
		featureID      uuid.UUID
		embeddingID    uuid.UUID
		idempotencyKey uuid.UUID
		datasetFile    *model.DatasetFile
	)

	BeforeEach(func() {
		ctx = context.Background()
		poolMock = &testConnectionPool{NextRowsAffected: 1}
		dbCore := coreDB.NewDatabase(poolMock, "test_db")
		repository = featuredb.NewSnapshotRepository(dbCore)

		datasetID = uuid.New()
		userID = uuid.New()
		rawSnapshotID = uuid.New()
		featureID = uuid.New()
		embeddingID = uuid.New()
		idempotencyKey = uuid.New()
		datasetFile = &model.DatasetFile{
			DatasetID:         datasetID,
			UserID:            userID,
			StorageLocation:   "s3://local/raw/file.csv",
			ContentType:       "text/csv",
			FileExtension:     "csv",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: model.ProcessingProfileTextRAG,
		}
	})

	Describe("SavePendingRawSnapshot", func() {
		It("inserts a pending raw snapshot with named args", func() {
			poolMock.NextRows = []pgx.Row{newRawSnapshotRow(rawSnapshotID, datasetID, userID)}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, datasetFile, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(rawSnapshot.RawSnapshotID).To(Equal(rawSnapshotID))
			Expect(rawSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(poolMock.QueryRowCalled).To(BeTrue())
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.raw_snapshots"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ON CONFLICT (idempotency_key) DO NOTHING"))

			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}))
			Expect(args).To(HaveKeyWithValue("storage_location", datasetFile.StorageLocation))
			Expect(args).To(HaveKeyWithValue("table_name", datasetFile.TableName))
			Expect(args).To(HaveKeyWithValue("processing_profile", model.ProcessingProfileTextRAG.String()))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusPending.String()))
		})

		It("returns a domain idempotency error when the insert is replayed", func() {
			readyRow := newRawSnapshotRow(rawSnapshotID, datasetID, userID)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, datasetFile, idempotencyKey)

			Expect(rawSnapshot).To(BeNil())
			existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err)
			Expect(ok).To(BeTrue())
			Expect(existing.RawSnapshotID).To(Equal(rawSnapshotID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("FROM test_db.raw_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("reopens failed raw snapshots so Temporal can retry the activity body", func() {
			failedRow := newRawSnapshotRow(rawSnapshotID, datasetID, userID)
			failedRow.Status = model.SnapshotStatusFailed.String()
			failedRow.FailureReason = "object store timeout"
			reopenedRow := failedRow
			reopenedRow.Status = model.SnapshotStatusPending.String()
			reopenedRow.FailureReason = ""
			poolMock.NextRows = []pgx.Row{
				errorRow{err: pgx.ErrNoRows},
				failedRow,
				reopenedRow,
			}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, datasetFile, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(rawSnapshot.RawSnapshotID).To(Equal(rawSnapshotID))
			Expect(rawSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(rawSnapshot.FailureReason).To(BeEmpty())
			Expect(poolMock.QueryRowCalledCount).To(Equal(3))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("UPDATE test_db.raw_snapshots"))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("failure_reason = NULL"))
		})

		It("returns retryable in-progress errors for pending raw snapshot replays", func() {
			poolMock.NextRows = []pgx.Row{
				errorRow{err: pgx.ErrNoRows},
				newRawSnapshotRow(rawSnapshotID, datasetID, userID),
			}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, datasetFile, idempotencyKey)

			Expect(rawSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrRawSnapshotInProgress)).To(BeTrue())
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
		})

		It("wraps non-idempotency insert errors", func() {
			expectedErr := errors.New("database failed")
			poolMock.NextRows = []pgx.Row{errorRow{err: expectedErr}}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, datasetFile, idempotencyKey)

			Expect(rawSnapshot).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("insert raw snapshot")))
			Expect(errors.Is(err, expectedErr)).To(BeTrue())
		})
	})

	Describe("ReadRawSnapshot", func() {
		It("returns raw snapshot not found when no row exists", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			rawSnapshot, err := repository.ReadRawSnapshot(ctx, rawSnapshotID)

			Expect(rawSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrRawSnapshotNotFound)).To(BeTrue())
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.raw_snapshots WHERE raw_snapshot_id = @raw_snapshot_id"))
		})
	})

	Describe("MarkRawReady", func() {
		It("marks the raw snapshot ready", func() {
			err := repository.MarkRawReady(ctx, &model.RawSnapshot{
				RawSnapshotID:   rawSnapshotID,
				StorageLocation: "s3://lakehouse/raw/snapshot.parquet",
				TableFormat:     "PARQUET",
				SchemaVersion:   1,
				SchemaMetadata:  "{}",
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(poolMock.ExecCalled).To(BeTrue())
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("UPDATE test_db.raw_snapshots"))
			args := namedArgs(poolMock.ExecArgs[0])
			Expect(args).To(HaveKeyWithValue("raw_snapshot_id", pgtype.UUID{Bytes: rawSnapshotID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("storage_location", "s3://lakehouse/raw/snapshot.parquet"))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusReady.String()))
		})

		It("returns raw snapshot not found when no row is updated", func() {
			poolMock.NextRowsAffected = 0

			err := repository.MarkRawReady(ctx, &model.RawSnapshot{RawSnapshotID: rawSnapshotID})

			Expect(errors.Is(err, domain.ErrRawSnapshotNotFound)).To(BeTrue())
		})
	})

	Describe("SavePendingFeatureSnapshot", func() {
		It("reads the raw snapshot and inserts a pending feature snapshot", func() {
			poolMock.NextRows = []pgx.Row{
				newRawSnapshotRow(rawSnapshotID, datasetID, userID),
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID),
			}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(featureSnapshot.FeatureSnapshotID).To(Equal(featureID))
			Expect(featureSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.raw_snapshots WHERE raw_snapshot_id = @raw_snapshot_id"))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("INSERT INTO test_db.feature_snapshots"))

			args := namedArgs(poolMock.QueryArgs[1])
			Expect(args).To(HaveKeyWithValue("raw_snapshot_id", pgtype.UUID{Bytes: rawSnapshotID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("table_name", "movies"))
			Expect(args).To(HaveKeyWithValue("processing_profile", model.ProcessingProfileTextRAG.String()))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusPending.String()))
		})

		It("returns a domain idempotency error when feature snapshot insert is replayed", func() {
			readyRow := newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				newRawSnapshotRow(rawSnapshotID, datasetID, userID),
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)

			Expect(featureSnapshot).To(BeNil())
			existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err)
			Expect(ok).To(BeTrue())
			Expect(existing.FeatureSnapshotID).To(Equal(featureID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(3))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("FROM test_db.feature_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("reopens failed feature snapshots for retry", func() {
			failedRow := newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID)
			failedRow.Status = model.SnapshotStatusFailed.String()
			failedRow.FailureReason = "builder failed"
			reopenedRow := failedRow
			reopenedRow.Status = model.SnapshotStatusPending.String()
			reopenedRow.FailureReason = ""
			poolMock.NextRows = []pgx.Row{
				newRawSnapshotRow(rawSnapshotID, datasetID, userID),
				errorRow{err: pgx.ErrNoRows},
				failedRow,
				reopenedRow,
			}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(featureSnapshot.FeatureSnapshotID).To(Equal(featureID))
			Expect(featureSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(featureSnapshot.FailureReason).To(BeEmpty())
			Expect(poolMock.QueryRowCalledCount).To(Equal(4))
			Expect(poolMock.QueryCalls[3]).To(ContainSubstring("UPDATE test_db.feature_snapshots"))
		})

		It("does not insert when the raw snapshot is missing", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)

			Expect(featureSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrRawSnapshotNotFound)).To(BeTrue())
			Expect(poolMock.QueryRowCalledCount).To(Equal(1))
		})
	})

	Describe("MarkFeatureFailed", func() {
		It("marks the feature snapshot failed with a reason", func() {
			err := repository.MarkFeatureFailed(ctx, featureID, "build failed")

			Expect(err).NotTo(HaveOccurred())
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("UPDATE test_db.feature_snapshots"))
			args := namedArgs(poolMock.ExecArgs[0])
			Expect(args).To(HaveKeyWithValue("feature_snapshot_id", pgtype.UUID{Bytes: featureID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("failure_reason", "build failed"))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusFailed.String()))
		})
	})

	Describe("SavePendingEmbeddingSnapshot", func() {
		It("reads the feature snapshot and inserts a pending embedding snapshot", func() {
			strategy := validEmbeddingStrategy()
			poolMock.NextRows = []pgx.Row{
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID),
				newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID),
			}

			embeddingSnapshot, err := repository.SavePendingEmbeddingSnapshot(ctx, featureID, idempotencyKey, strategy)

			Expect(err).NotTo(HaveOccurred())
			Expect(embeddingSnapshot.EmbeddingSnapshotID).To(Equal(embeddingID))
			Expect(embeddingSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(embeddingSnapshot.StrategyVersion).To(Equal(strategy.StrategyVersion))
			Expect(embeddingSnapshot.EmbeddingProvider).To(Equal(strategy.EmbeddingProvider))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.feature_snapshots WHERE feature_snapshot_id = @feature_snapshot_id"))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("INSERT INTO test_db.embedding_snapshots"))

			args := namedArgs(poolMock.QueryArgs[1])
			Expect(args).To(HaveKeyWithValue("feature_snapshot_id", pgtype.UUID{Bytes: featureID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("strategy_version", strategy.StrategyVersion))
			Expect(args).To(HaveKeyWithValue("chunker_name", strategy.ChunkerName))
			Expect(args).To(HaveKeyWithValue("chunker_version", strategy.ChunkerVersion))
			Expect(args).To(HaveKeyWithValue("chunk_size", strategy.ChunkSize))
			Expect(args).To(HaveKeyWithValue("chunk_overlap", strategy.ChunkOverlap))
			Expect(args).To(HaveKeyWithValue("embedding_provider", strategy.EmbeddingProvider))
			Expect(args).To(HaveKeyWithValue("embedding_model", strategy.EmbeddingModel))
			Expect(args).To(HaveKeyWithValue("embedding_dimensions", strategy.EmbeddingDimensions))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusPending.String()))
		})

		It("returns a domain idempotency error when embedding insert is replayed", func() {
			readyRow := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID),
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			embeddingSnapshot, err := repository.SavePendingEmbeddingSnapshot(ctx, featureID, idempotencyKey, model.EmbeddingStrategy{})

			Expect(embeddingSnapshot).To(BeNil())
			existing, ok := domain.IsEmbeddingsAlreadyMaterialized(err)
			Expect(ok).To(BeTrue())
			Expect(existing.EmbeddingSnapshotID).To(Equal(embeddingID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(3))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("FROM test_db.embedding_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("reopens failed embedding snapshots for retry", func() {
			failedRow := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID)
			failedRow.Status = model.SnapshotStatusFailed.String()
			failedRow.FailureReason = "embedding writer failed"
			reopenedRow := failedRow
			reopenedRow.Status = model.SnapshotStatusPending.String()
			reopenedRow.FailureReason = ""
			poolMock.NextRows = []pgx.Row{
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID),
				errorRow{err: pgx.ErrNoRows},
				failedRow,
				reopenedRow,
			}

			embeddingSnapshot, err := repository.SavePendingEmbeddingSnapshot(ctx, featureID, idempotencyKey, model.EmbeddingStrategy{})

			Expect(err).NotTo(HaveOccurred())
			Expect(embeddingSnapshot.EmbeddingSnapshotID).To(Equal(embeddingID))
			Expect(embeddingSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(embeddingSnapshot.FailureReason).To(BeEmpty())
			Expect(poolMock.QueryRowCalledCount).To(Equal(4))
			Expect(poolMock.QueryCalls[3]).To(ContainSubstring("UPDATE test_db.embedding_snapshots"))
		})
	})

	Describe("MarkEmbeddingReady", func() {
		It("marks the embedding snapshot ready", func() {
			err := repository.MarkEmbeddingReady(ctx, &model.EmbeddingSnapshot{
				EmbeddingSnapshotID: embeddingID,
				VectorStore:         "pgvector",
				CollectionName:      "movies",
				EmbeddingDimensions: 384,
				EmbeddingCount:      3,
				StrategyVersion:     model.DefaultEmbeddingStrategyVersion,
				ChunkerName:         model.DefaultChunkerName,
				ChunkerVersion:      model.DefaultChunkerVersion,
				ChunkSize:           model.DefaultChunkSize,
				ChunkOverlap:        model.DefaultChunkOverlap,
				EmbeddingProvider:   "ollama",
				EmbeddingModel:      model.DefaultEmbeddingModel,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("UPDATE test_db.embedding_snapshots"))
			args := namedArgs(poolMock.ExecArgs[0])
			Expect(args).To(HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: embeddingID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("vector_store", "pgvector"))
			Expect(args).To(HaveKeyWithValue("collection_name", "movies"))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusReady.String()))
			Expect(args).To(HaveKeyWithValue("embedding_provider", "ollama"))
		})

		It("returns embedding snapshot not found when no row is updated", func() {
			poolMock.NextRowsAffected = 0

			err := repository.MarkEmbeddingReady(ctx, &model.EmbeddingSnapshot{EmbeddingSnapshotID: embeddingID})

			Expect(errors.Is(err, domain.ErrEmbeddingSnapshotNotFound)).To(BeTrue())
		})
	})
})

func namedArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func newRawSnapshotRow(rawSnapshotID, datasetID, userID uuid.UUID) rawSnapshotRow {
	return rawSnapshotRow{
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         datasetID,
		UserID:            userID,
		StorageLocation:   "s3://local/raw/file.csv",
		ContentType:       "text/csv",
		FileExtension:     "csv",
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		ProcessingProfile: model.ProcessingProfileTextRAG.String(),
		SchemaVersion:     1,
		SchemaMetadata:    "{}",
		Status:            model.SnapshotStatusPending.String(),
	}
}

func newFeatureSnapshotRow(featureSnapshotID, rawSnapshotID, datasetID, userID uuid.UUID) featureSnapshotRow {
	return featureSnapshotRow{
		FeatureSnapshotID: featureSnapshotID,
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         datasetID,
		UserID:            userID,
		StorageLocation:   "s3://lakehouse/features/snapshot.parquet",
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		CatalogProvider:   "LOCAL",
		ProcessingProfile: model.ProcessingProfileTextRAG.String(),
		SchemaVersion:     1,
		SchemaMetadata:    "{}",
		Status:            model.SnapshotStatusPending.String(),
	}
}

func newEmbeddingSnapshotRow(embeddingSnapshotID, featureSnapshotID, datasetID, userID uuid.UUID) embeddingSnapshotRow {
	strategy := validEmbeddingStrategy()
	return embeddingSnapshotRow{
		EmbeddingSnapshotID: embeddingSnapshotID,
		FeatureSnapshotID:   featureSnapshotID,
		DatasetID:           datasetID,
		UserID:              userID,
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		EmbeddingDimensions: strategy.EmbeddingDimensions,
		EmbeddingCount:      3,
		StrategyVersion:     strategy.StrategyVersion,
		ChunkerName:         strategy.ChunkerName,
		ChunkerVersion:      strategy.ChunkerVersion,
		ChunkSize:           strategy.ChunkSize,
		ChunkOverlap:        strategy.ChunkOverlap,
		EmbeddingProvider:   strategy.EmbeddingProvider,
		EmbeddingModel:      strategy.EmbeddingModel,
		Status:              model.SnapshotStatusPending.String(),
	}
}

func validEmbeddingStrategy() model.EmbeddingStrategy {
	return model.NormalizeEmbeddingStrategy(model.EmbeddingStrategy{
		StrategyVersion:     "rag-v1",
		ChunkerName:         "go-token-window",
		ChunkerVersion:      "v1",
		ChunkSize:           128,
		ChunkOverlap:        16,
		EmbeddingProvider:   "ollama",
		EmbeddingModel:      model.DefaultEmbeddingModel,
		EmbeddingDimensions: 384,
	})
}
