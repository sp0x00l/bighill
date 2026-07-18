package db_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featuredb "feature_materializer_service/pkg/infra/repo/db"
	"lib/shared_lib/ctxutil"
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
	QueryCalled         bool
	ExecCalled          bool
	ExecCalledCount     int
	QueryCalls          []string
	QueryArgs           [][]any
	ExecCalls           []string
	ExecArgs            [][]any
	NextRows            []pgx.Row
	NextQueryRows       pgx.Rows
	NextQueryRowsQueue  []pgx.Rows
	NextRowsAffected    int64
	NextError           error
	NextExecErrors      []error
	CommitCalled        bool
	RollbackCalled      bool
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

func (p *testConnectionPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.QueryCalled = true
	p.QueryCalls = append(p.QueryCalls, sql)
	p.QueryArgs = append(p.QueryArgs, args)
	if len(p.NextQueryRowsQueue) > 0 {
		nextRows := p.NextQueryRowsQueue[0]
		p.NextQueryRowsQueue = p.NextQueryRowsQueue[1:]
		return nextRows, p.NextError
	}
	if p.NextQueryRows != nil {
		return p.NextQueryRows, p.NextError
	}
	return &testRows{}, p.NextError
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
	if p.NextError != nil {
		return nil, p.NextError
	}
	return &testTx{pool: p}, nil
}

type testTx struct {
	pool *testConnectionPool
}

func (tx *testTx) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *testTx) Commit(context.Context) error {
	tx.pool.CommitCalled = true
	return nil
}

func (tx *testTx) Rollback(context.Context) error {
	tx.pool.RollbackCalled = true
	return nil
}

func (tx *testTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *testTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *testTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *testTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *testTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return tx.pool.Exec(ctx, sql, arguments...)
}

func (tx *testTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.pool.Query(ctx, sql, args...)
}

func (tx *testTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.pool.QueryRow(ctx, sql, args...)
}

func (tx *testTx) Conn() *pgx.Conn {
	return nil
}

type testRows struct {
	rows   []pgx.Row
	index  int
	err    error
	closed bool
}

func (r *testRows) Close() {
	r.closed = true
}

func (r *testRows) Err() error {
	return r.err
}

func (r *testRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT 0")
}

func (r *testRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *testRows) Next() bool {
	if r.index >= len(r.rows) {
		r.Close()
		return false
	}
	r.index++
	return true
}

func (r *testRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.rows) {
		return pgx.ErrNoRows
	}
	return r.rows[r.index-1].Scan(dest...)
}

func (r *testRows) Values() ([]any, error) {
	return nil, nil
}

func (r *testRows) RawValues() [][]byte {
	return nil
}

func (r *testRows) Conn() *pgx.Conn {
	return nil
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(...any) error {
	return r.err
}

type rawSnapshotRow struct {
	RawSnapshotID           uuid.UUID
	DatasetID               uuid.UUID
	UserID                  uuid.UUID
	OrgID                   uuid.UUID
	MaterializationEventSeq int64
	StorageLocation         string
	ContentType             string
	FileExtension           string
	TableNamespace          string
	TableName               string
	TableFormat             string
	CatalogProvider         string
	ProcessingProfile       string
	SchemaVersion           int
	SchemaMetadata          string
	Status                  string
	FailureReason           string
}

func (r rawSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.RawSnapshotID.String()
	*(dest[1].(*string)) = r.DatasetID.String()
	*(dest[2].(*string)) = r.UserID.String()
	*(dest[3].(*string)) = r.OrgID.String()
	*(dest[4].(*int64)) = r.MaterializationEventSeq
	*(dest[5].(*string)) = r.StorageLocation
	*(dest[6].(*string)) = r.ContentType
	*(dest[7].(*string)) = r.FileExtension
	*(dest[8].(*string)) = r.TableNamespace
	*(dest[9].(*string)) = r.TableName
	*(dest[10].(*string)) = r.TableFormat
	*(dest[11].(*string)) = r.CatalogProvider
	*(dest[12].(*string)) = r.ProcessingProfile
	*(dest[13].(*int)) = r.SchemaVersion
	*(dest[14].(*string)) = r.SchemaMetadata
	*(dest[15].(*string)) = r.Status
	*(dest[16].(*string)) = r.FailureReason
	return nil
}

type featureSnapshotRow struct {
	FeatureSnapshotID       uuid.UUID
	RawSnapshotID           uuid.UUID
	DatasetID               uuid.UUID
	UserID                  uuid.UUID
	OrgID                   uuid.UUID
	MaterializationEventSeq int64
	StorageLocation         string
	TableNamespace          string
	TableName               string
	TableFormat             string
	CatalogProvider         string
	ProcessingProfile       string
	SchemaVersion           int
	SchemaMetadata          string
	Status                  string
	FailureReason           string
}

func (r featureSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.FeatureSnapshotID.String()
	*(dest[1].(*string)) = r.RawSnapshotID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*string)) = r.UserID.String()
	*(dest[4].(*string)) = r.OrgID.String()
	*(dest[5].(*int64)) = r.MaterializationEventSeq
	*(dest[6].(*string)) = r.StorageLocation
	*(dest[7].(*string)) = r.TableNamespace
	*(dest[8].(*string)) = r.TableName
	*(dest[9].(*string)) = r.TableFormat
	*(dest[10].(*string)) = r.CatalogProvider
	*(dest[11].(*string)) = r.ProcessingProfile
	*(dest[12].(*int)) = r.SchemaVersion
	*(dest[13].(*string)) = r.SchemaMetadata
	*(dest[14].(*string)) = r.Status
	*(dest[15].(*string)) = r.FailureReason
	return nil
}

type embeddingSnapshotRow struct {
	EmbeddingSnapshotID     uuid.UUID
	FeatureSnapshotID       uuid.UUID
	DatasetID               uuid.UUID
	UserID                  uuid.UUID
	OrgID                   uuid.UUID
	MaterializationEventSeq int64
	VectorStore             string
	CollectionName          string
	EmbeddingDimensions     int
	EmbeddingCount          int64
	StrategyVersion         string
	ExtractorName           string
	ExtractorVersion        string
	CleanerName             string
	CleanerVersion          string
	ChunkerName             string
	ChunkerVersion          string
	ChunkSize               int
	ChunkOverlap            int
	EmbeddingProvider       string
	EmbeddingModel          string
	ActiveForRetrieval      bool
	Status                  string
	FailureReason           string
}

func (r embeddingSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EmbeddingSnapshotID.String()
	*(dest[1].(*string)) = r.FeatureSnapshotID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*string)) = r.UserID.String()
	*(dest[4].(*string)) = r.OrgID.String()
	*(dest[5].(*int64)) = r.MaterializationEventSeq
	*(dest[6].(*string)) = r.VectorStore
	*(dest[7].(*string)) = r.CollectionName
	*(dest[8].(*int)) = r.EmbeddingDimensions
	*(dest[9].(*int64)) = r.EmbeddingCount
	*(dest[10].(*string)) = r.StrategyVersion
	*(dest[11].(*string)) = r.ExtractorName
	*(dest[12].(*string)) = r.ExtractorVersion
	*(dest[13].(*string)) = r.CleanerName
	*(dest[14].(*string)) = r.CleanerVersion
	*(dest[15].(*string)) = r.ChunkerName
	*(dest[16].(*string)) = r.ChunkerVersion
	*(dest[17].(*int)) = r.ChunkSize
	*(dest[18].(*int)) = r.ChunkOverlap
	*(dest[19].(*string)) = r.EmbeddingProvider
	*(dest[20].(*string)) = r.EmbeddingModel
	*(dest[21].(*bool)) = r.ActiveForRetrieval
	*(dest[22].(*string)) = r.Status
	*(dest[23].(*string)) = r.FailureReason
	return nil
}

type graphSnapshotRow struct {
	GraphSnapshotID         uuid.UUID
	FeatureSnapshotID       uuid.UUID
	EmbeddingSnapshotID     uuid.UUID
	DatasetID               uuid.UUID
	UserID                  uuid.UUID
	OrgID                   uuid.UUID
	MaterializationEventSeq int64
	IdempotencyKey          uuid.UUID
	ProvenanceHash          string
	ExtractionModel         string
	ExtractionPromptVersion string
	ExtractionSchemaVersion string
	ChunkCount              int64
	ChunksProcessed         int64
	EntityCount             int64
	EdgeCount               int64
	ActiveForRetrieval      bool
	Status                  string
	FailureReason           string
}

func (r graphSnapshotRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.GraphSnapshotID.String()
	*(dest[1].(*string)) = r.FeatureSnapshotID.String()
	*(dest[2].(*string)) = r.EmbeddingSnapshotID.String()
	*(dest[3].(*string)) = r.DatasetID.String()
	*(dest[4].(*string)) = r.UserID.String()
	*(dest[5].(*string)) = r.OrgID.String()
	*(dest[6].(*int64)) = r.MaterializationEventSeq
	*(dest[7].(*string)) = r.IdempotencyKey.String()
	*(dest[8].(*string)) = r.ProvenanceHash
	*(dest[9].(*string)) = r.ExtractionModel
	*(dest[10].(*string)) = r.ExtractionPromptVersion
	*(dest[11].(*string)) = r.ExtractionSchemaVersion
	*(dest[12].(*int64)) = r.ChunkCount
	*(dest[13].(*int64)) = r.ChunksProcessed
	*(dest[14].(*int64)) = r.EntityCount
	*(dest[15].(*int64)) = r.EdgeCount
	*(dest[16].(*bool)) = r.ActiveForRetrieval
	*(dest[17].(*string)) = r.Status
	*(dest[18].(*string)) = r.FailureReason
	return nil
}

type graphChunkRow struct {
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	OrgID               uuid.UUID
	ChunkIndex          int
	SourceText          string
}

func (r graphChunkRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EmbeddingRecordID.String()
	*(dest[1].(*string)) = r.EmbeddingSnapshotID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*string)) = r.UserID.String()
	*(dest[4].(*string)) = r.OrgID.String()
	*(dest[5].(*int)) = r.ChunkIndex
	*(dest[6].(*string)) = r.SourceText
	return nil
}

type uuidTextRow struct {
	value uuid.UUID
	err   error
}

func (r uuidTextRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*string)) = r.value.String()
	return nil
}

type int64Row struct {
	value int64
	err   error
}

func (r int64Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*int64)) = r.value
	return nil
}

type embeddingRecordSearchRow struct {
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	ChunkIndex          int
	SourceText          string
	Distance            float64
}

func (r embeddingRecordSearchRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EmbeddingRecordID.String()
	*(dest[1].(*string)) = r.EmbeddingSnapshotID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*int)) = r.ChunkIndex
	*(dest[4].(*string)) = r.SourceText
	*(dest[5].(*float64)) = r.Distance
	return nil
}

type graphRetrievedContextRow struct {
	GraphNodeChunkID    uuid.UUID
	GraphNodeID         uuid.UUID
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	DatasetID           uuid.UUID
	ChunkIndex          int
	SourceText          string
	Score               float64
	OrgID               uuid.UUID
}

func (r graphRetrievedContextRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.GraphNodeChunkID.String()
	*(dest[1].(*string)) = r.GraphNodeID.String()
	*(dest[2].(*string)) = r.EmbeddingRecordID.String()
	*(dest[3].(*string)) = r.EmbeddingSnapshotID.String()
	*(dest[4].(*string)) = r.DatasetID.String()
	*(dest[5].(*int)) = r.ChunkIndex
	*(dest[6].(*string)) = r.SourceText
	*(dest[7].(*float64)) = r.Score
	*(dest[8].(*string)) = r.OrgID.String()
	return nil
}

type graphMatchedEntityRow struct {
	GraphNodeID uuid.UUID
	Name        string
	Type        string
	Description string
	Score       float64
}

func (r graphMatchedEntityRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.GraphNodeID.String()
	*(dest[1].(*string)) = r.Name
	*(dest[2].(*string)) = r.Type
	*(dest[3].(*string)) = r.Description
	*(dest[4].(*float64)) = r.Score
	return nil
}

type graphPathRow struct {
	GraphNodeIDs  string
	RelationTypes string
	Score         float64
}

func (r graphPathRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.GraphNodeIDs
	*(dest[1].(*string)) = r.RelationTypes
	*(dest[2].(*float64)) = r.Score
	return nil
}

var _ = Describe("SnapshotRepository", func() {
	var (
		ctx            context.Context
		poolMock       *testConnectionPool
		tx             pgx.Tx
		repository     *featuredb.SnapshotRepository
		datasetID      uuid.UUID
		userID         uuid.UUID
		orgID          uuid.UUID
		rawSnapshotID  uuid.UUID
		featureID      uuid.UUID
		embeddingID    uuid.UUID
		idempotencyKey uuid.UUID
		datasetFile    *model.DatasetFile
	)

	BeforeEach(func() {
		poolMock = &testConnectionPool{NextRowsAffected: 1}
		tx = &testTx{pool: poolMock}
		dbCore := coreDB.NewDatabase(poolMock, "test_db")
		repository = featuredb.NewSnapshotRepository(dbCore)

		datasetID = uuid.New()
		userID = uuid.New()
		orgID = uuid.New()
		ctx = ctxutil.WithActorOrg(context.Background(), userID, orgID)
		rawSnapshotID = uuid.New()
		featureID = uuid.New()
		embeddingID = uuid.New()
		idempotencyKey = uuid.New()
		datasetFile = &model.DatasetFile{
			DatasetID:         datasetID,
			UserID:            userID,
			OrgID:             orgID,
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
			poolMock.NextRows = []pgx.Row{newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID)}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, tx, datasetFile, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(rawSnapshot.RawSnapshotID).To(Equal(rawSnapshotID))
			Expect(rawSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(poolMock.QueryRowCalled).To(BeTrue())
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.raw_snapshots"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ON CONFLICT (idempotency_key) DO NOTHING"))

			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}))
			Expect(args).To(HaveKeyWithValue("storage_location", datasetFile.StorageLocation))
			Expect(args).To(HaveKeyWithValue("table_name", datasetFile.TableName))
			Expect(args).To(HaveKeyWithValue("processing_profile", model.ProcessingProfileTextRAG.String()))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusPending.String()))
		})

		It("returns a domain idempotency error when the insert is replayed", func() {
			readyRow := newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, tx, datasetFile, idempotencyKey)

			Expect(rawSnapshot).To(BeNil())
			existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err)
			Expect(ok).To(BeTrue())
			Expect(existing.RawSnapshotID).To(Equal(rawSnapshotID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("FROM test_db.raw_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("reopens failed raw snapshots so Temporal can retry the activity body", func() {
			failedRow := newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID)
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

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, tx, datasetFile, idempotencyKey)

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
				newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID),
			}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, tx, datasetFile, idempotencyKey)

			Expect(rawSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrRawSnapshotInProgress)).To(BeTrue())
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
		})

		It("wraps non-idempotency insert errors", func() {
			expectedErr := errors.New("database failed")
			poolMock.NextRows = []pgx.Row{errorRow{err: expectedErr}}

			rawSnapshot, err := repository.SavePendingRawSnapshot(ctx, tx, datasetFile, idempotencyKey)

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
			poolMock.NextRows = []pgx.Row{int64Row{value: 1}}
			rawSnapshot := &model.RawSnapshot{
				RawSnapshotID:   rawSnapshotID,
				DatasetID:       datasetID,
				OrgID:           orgID,
				StorageLocation: "s3://lakehouse/raw/snapshot.parquet",
				TableFormat:     "PARQUET",
				SchemaVersion:   1,
				SchemaMetadata:  "{}",
			}

			err := repository.MarkRawReady(ctx, tx, rawSnapshot)

			Expect(err).NotTo(HaveOccurred())
			Expect(rawSnapshot.MaterializationEventSeq).To(Equal(int64(1)))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.dataset_materialization_event_state"))
			Expect(poolMock.ExecCalled).To(BeTrue())
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("UPDATE test_db.raw_snapshots"))
			args := namedArgs(poolMock.ExecArgs[0])
			Expect(args).To(HaveKeyWithValue("raw_snapshot_id", pgtype.UUID{Bytes: rawSnapshotID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("materialization_event_seq", int64(1)))
			Expect(args).To(HaveKeyWithValue("storage_location", "s3://lakehouse/raw/snapshot.parquet"))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusReady.String()))
		})

		It("returns raw snapshot not found when no row is updated", func() {
			poolMock.NextRows = []pgx.Row{int64Row{value: 1}}
			poolMock.NextRowsAffected = 0

			err := repository.MarkRawReady(ctx, tx, &model.RawSnapshot{RawSnapshotID: rawSnapshotID, DatasetID: datasetID, OrgID: orgID})

			Expect(errors.Is(err, domain.ErrRawSnapshotNotFound)).To(BeTrue())
		})
	})

	Describe("SavePendingFeatureSnapshot", func() {
		It("reads the raw snapshot and inserts a pending feature snapshot", func() {
			poolMock.NextRows = []pgx.Row{
				newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID),
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID, orgID),
			}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, tx, rawSnapshotID, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(featureSnapshot.FeatureSnapshotID).To(Equal(featureID))
			Expect(featureSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.raw_snapshots WHERE raw_snapshot_id = @raw_snapshot_id"))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("INSERT INTO test_db.feature_snapshots"))

			args := namedArgs(poolMock.QueryArgs[1])
			Expect(args).To(HaveKeyWithValue("raw_snapshot_id", pgtype.UUID{Bytes: rawSnapshotID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("table_name", "movies"))
			Expect(args).To(HaveKeyWithValue("processing_profile", model.ProcessingProfileTextRAG.String()))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusPending.String()))
		})

		It("returns a domain idempotency error when feature snapshot insert is replayed", func() {
			readyRow := newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID, orgID)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID),
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, tx, rawSnapshotID, idempotencyKey)

			Expect(featureSnapshot).To(BeNil())
			existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err)
			Expect(ok).To(BeTrue())
			Expect(existing.FeatureSnapshotID).To(Equal(featureID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(3))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("FROM test_db.feature_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("reopens failed feature snapshots for retry", func() {
			failedRow := newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID, orgID)
			failedRow.Status = model.SnapshotStatusFailed.String()
			failedRow.FailureReason = "builder failed"
			reopenedRow := failedRow
			reopenedRow.Status = model.SnapshotStatusPending.String()
			reopenedRow.FailureReason = ""
			poolMock.NextRows = []pgx.Row{
				newRawSnapshotRow(rawSnapshotID, datasetID, userID, orgID),
				errorRow{err: pgx.ErrNoRows},
				failedRow,
				reopenedRow,
			}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, tx, rawSnapshotID, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(featureSnapshot.FeatureSnapshotID).To(Equal(featureID))
			Expect(featureSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(featureSnapshot.FailureReason).To(BeEmpty())
			Expect(poolMock.QueryRowCalledCount).To(Equal(4))
			Expect(poolMock.QueryCalls[3]).To(ContainSubstring("UPDATE test_db.feature_snapshots"))
		})

		It("does not insert when the raw snapshot is missing", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			featureSnapshot, err := repository.SavePendingFeatureSnapshot(ctx, tx, rawSnapshotID, idempotencyKey)

			Expect(featureSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrRawSnapshotNotFound)).To(BeTrue())
			Expect(poolMock.QueryRowCalledCount).To(Equal(1))
		})
	})

	Describe("MarkFeatureReady", func() {
		It("marks the feature snapshot ready", func() {
			poolMock.NextRows = []pgx.Row{int64Row{value: 2}}
			featureSnapshot := &model.FeatureSnapshot{
				FeatureSnapshotID: featureID,
				DatasetID:         datasetID,
				OrgID:             orgID,
				StorageLocation:   "s3://lakehouse/features/snapshot.parquet",
				TableFormat:       "PARQUET",
				SchemaVersion:     2,
				SchemaMetadata:    "{}",
			}

			err := repository.MarkFeatureReady(ctx, tx, featureSnapshot)

			Expect(err).NotTo(HaveOccurred())
			Expect(featureSnapshot.MaterializationEventSeq).To(Equal(int64(2)))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.dataset_materialization_event_state"))
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("UPDATE test_db.feature_snapshots"))
			args := namedArgs(poolMock.ExecArgs[0])
			Expect(args).To(HaveKeyWithValue("feature_snapshot_id", pgtype.UUID{Bytes: featureID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("materialization_event_seq", int64(2)))
			Expect(args).To(HaveKeyWithValue("storage_location", "s3://lakehouse/features/snapshot.parquet"))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusReady.String()))
		})
	})

	Describe("MarkFeatureFailed", func() {
		It("marks the feature snapshot failed with a reason", func() {
			err := repository.MarkFeatureFailed(ctx, tx, featureID, "build failed")

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
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID, orgID),
				newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID),
			}

			embeddingSnapshot, err := repository.SavePendingEmbeddingSnapshot(ctx, tx, featureID, idempotencyKey, strategy)

			Expect(err).NotTo(HaveOccurred())
			Expect(embeddingSnapshot.EmbeddingSnapshotID).To(Equal(embeddingID))
			Expect(embeddingSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(embeddingSnapshot.StrategyVersion).To(Equal(strategy.StrategyVersion))
			Expect(embeddingSnapshot.ExtractorName).To(Equal(strategy.ExtractorName))
			Expect(embeddingSnapshot.CleanerName).To(Equal(strategy.CleanerName))
			Expect(embeddingSnapshot.EmbeddingProvider).To(Equal(strategy.EmbeddingProvider))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.feature_snapshots WHERE feature_snapshot_id = @feature_snapshot_id"))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("INSERT INTO test_db.embedding_snapshots"))

			args := namedArgs(poolMock.QueryArgs[1])
			Expect(args).To(HaveKeyWithValue("feature_snapshot_id", pgtype.UUID{Bytes: featureID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("strategy_version", strategy.StrategyVersion))
			Expect(args).To(HaveKeyWithValue("extractor_name", strategy.ExtractorName))
			Expect(args).To(HaveKeyWithValue("extractor_version", strategy.ExtractorVersion))
			Expect(args).To(HaveKeyWithValue("cleaner_name", strategy.CleanerName))
			Expect(args).To(HaveKeyWithValue("cleaner_version", strategy.CleanerVersion))
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
			readyRow := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID, orgID),
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			embeddingSnapshot, err := repository.SavePendingEmbeddingSnapshot(ctx, tx, featureID, idempotencyKey, validEmbeddingStrategy())

			Expect(embeddingSnapshot).To(BeNil())
			existing, ok := domain.IsEmbeddingsAlreadyMaterialized(err)
			Expect(ok).To(BeTrue())
			Expect(existing.EmbeddingSnapshotID).To(Equal(embeddingID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(3))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("FROM test_db.embedding_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("reopens failed embedding snapshots for retry", func() {
			failedRow := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID)
			failedRow.Status = model.SnapshotStatusFailed.String()
			failedRow.FailureReason = "embedding writer failed"
			reopenedRow := failedRow
			reopenedRow.Status = model.SnapshotStatusPending.String()
			reopenedRow.FailureReason = ""
			poolMock.NextRows = []pgx.Row{
				newFeatureSnapshotRow(featureID, rawSnapshotID, datasetID, userID, orgID),
				errorRow{err: pgx.ErrNoRows},
				failedRow,
				reopenedRow,
			}

			embeddingSnapshot, err := repository.SavePendingEmbeddingSnapshot(ctx, tx, featureID, idempotencyKey, validEmbeddingStrategy())

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
			poolMock.NextRows = []pgx.Row{int64Row{value: 3}}
			embeddingSnapshot := &model.EmbeddingSnapshot{
				EmbeddingSnapshotID: embeddingID,
				DatasetID:           datasetID,
				OrgID:               orgID,
				VectorStore:         "pgvector",
				CollectionName:      "movies",
				EmbeddingDimensions: 384,
				EmbeddingCount:      3,
				StrategyVersion:     model.DefaultEmbeddingStrategyVersion,
				ExtractorName:       model.DefaultExtractorName,
				ExtractorVersion:    model.DefaultExtractorVersion,
				CleanerName:         model.DefaultCleanerName,
				CleanerVersion:      model.DefaultCleanerVersion,
				ChunkerName:         model.DefaultChunkerName,
				ChunkerVersion:      model.DefaultChunkerVersion,
				ChunkSize:           model.DefaultChunkSize,
				ChunkOverlap:        model.DefaultChunkOverlap,
				EmbeddingProvider:   "ollama",
				EmbeddingModel:      model.DefaultEmbeddingModel,
			}

			err := repository.MarkEmbeddingReady(ctx, tx, embeddingSnapshot)

			Expect(err).NotTo(HaveOccurred())
			Expect(embeddingSnapshot.MaterializationEventSeq).To(Equal(int64(3)))
			Expect(poolMock.ExecCalls).To(HaveLen(2))
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("active_for_retrieval = false"))
			Expect(poolMock.ExecCalls[1]).To(ContainSubstring("UPDATE test_db.embedding_snapshots"))
			Expect(poolMock.ExecCalls[1]).To(ContainSubstring("active_for_retrieval = true"))
			args := namedArgs(poolMock.ExecArgs[1])
			Expect(args).To(HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: embeddingID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("materialization_event_seq", int64(3)))
			Expect(args).To(HaveKeyWithValue("vector_store", "pgvector"))
			Expect(args).To(HaveKeyWithValue("collection_name", "movies"))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusReady.String()))
			Expect(args).To(HaveKeyWithValue("extractor_name", model.DefaultExtractorName))
			Expect(args).To(HaveKeyWithValue("cleaner_name", model.DefaultCleanerName))
			Expect(args).To(HaveKeyWithValue("embedding_provider", "ollama"))
		})

		It("returns embedding snapshot not found when no row is updated", func() {
			poolMock.NextRows = []pgx.Row{int64Row{value: 3}}
			poolMock.NextRowsAffected = 0

			err := repository.MarkEmbeddingReady(ctx, tx, &model.EmbeddingSnapshot{EmbeddingSnapshotID: embeddingID, DatasetID: datasetID, OrgID: orgID})

			Expect(errors.Is(err, domain.ErrEmbeddingSnapshotNotFound)).To(BeTrue())
		})
	})

	Describe("ReadActiveEmbeddingSnapshot", func() {
		It("reads the ready active snapshot for a dataset", func() {
			activeRow := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID)
			activeRow.Status = model.SnapshotStatusReady.String()
			activeRow.ActiveForRetrieval = true
			activeRow.EmbeddingProvider = " OLLAMA "
			poolMock.NextRows = []pgx.Row{activeRow}

			embeddingSnapshot, err := repository.ReadActiveEmbeddingSnapshot(ctx, userID, datasetID)

			Expect(err).NotTo(HaveOccurred())
			Expect(embeddingSnapshot.EmbeddingSnapshotID).To(Equal(embeddingID))
			Expect(embeddingSnapshot.ActiveForRetrieval).To(BeTrue())
			Expect(embeddingSnapshot.EmbeddingProvider).To(Equal("ollama"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("active_for_retrieval = true"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("status", model.SnapshotStatusReady.String()))
		})

		It("rejects active embedding snapshots with invalid strategy metadata at the repository boundary", func() {
			activeRow := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID)
			activeRow.Status = model.SnapshotStatusReady.String()
			activeRow.ActiveForRetrieval = true
			activeRow.CleanerName = ""
			poolMock.NextRows = []pgx.Row{activeRow}

			embeddingSnapshot, err := repository.ReadActiveEmbeddingSnapshot(ctx, userID, datasetID)

			Expect(embeddingSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("cleaner_name is required"))
		})
	})

	Describe("SearchEmbeddingRecords", func() {
		It("scopes search to the active embedding snapshot and uses the dimension-cast HNSW query shape", func() {
			poolMock.NextQueryRows = &testRows{rows: []pgx.Row{
				embeddingRecordSearchRow{
					EmbeddingRecordID:   uuid.New(),
					EmbeddingSnapshotID: embeddingID,
					DatasetID:           datasetID,
					ChunkIndex:          2,
					SourceText:          "nearest chunk",
					Distance:            0.25,
				},
			}}
			activeSnapshot := newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID)

			records, err := repository.SearchEmbeddingRecords(ctx, &model.EmbeddingSnapshot{
				EmbeddingSnapshotID: activeSnapshot.EmbeddingSnapshotID,
				UserID:              activeSnapshot.UserID,
				OrgID:               activeSnapshot.OrgID,
				DatasetID:           activeSnapshot.DatasetID,
				EmbeddingDimensions: activeSnapshot.EmbeddingDimensions,
			}, make([]float32, activeSnapshot.EmbeddingDimensions), 3)

			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1))
			Expect(records[0].Distance).To(Equal(0.25))
			Expect(records[0].Similarity).To(Equal(0.75))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("embedding_snapshot_id = @embedding_snapshot_id"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("embedding::vector(384)"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("vector_dims(embedding) = 384"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("double precision AS distance"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: embeddingID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: activeSnapshot.OrgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("limit", 3))
		})
	})

	Describe("SavePendingGraphSnapshot", func() {
		It("reads the embedding snapshot and inserts a pending graph snapshot", func() {
			graphSnapshotID := uuid.New()
			strategy := model.GraphExtractionStrategy{
				ExtractionModel:         "llm-graph-extractor",
				ExtractionPromptVersion: "graph-prompt-v2",
				ExtractionSchemaVersion: "graph-schema-v2",
			}
			poolMock.NextRows = []pgx.Row{
				newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID),
				newGraphSnapshotRow(graphSnapshotID, featureID, embeddingID, datasetID, userID, orgID, idempotencyKey),
			}

			graphSnapshot, err := repository.SavePendingGraphSnapshot(ctx, tx, embeddingID, idempotencyKey, strategy)

			Expect(err).NotTo(HaveOccurred())
			Expect(graphSnapshot.GraphSnapshotID).To(Equal(graphSnapshotID))
			Expect(graphSnapshot.Status).To(Equal(model.SnapshotStatusPending))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.embedding_snapshots WHERE embedding_snapshot_id = @embedding_snapshot_id"))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("INSERT INTO test_db.graph_snapshots"))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("ON CONFLICT (idempotency_key) DO NOTHING"))
			args := namedArgs(poolMock.QueryArgs[1])
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("feature_snapshot_id", pgtype.UUID{Bytes: featureID, Valid: true}),
				HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: embeddingID, Valid: true}),
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}),
				HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}),
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
				HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}),
				HaveKeyWithValue("extraction_model", "llm-graph-extractor"),
				HaveKeyWithValue("extraction_prompt_version", "graph-prompt-v2"),
				HaveKeyWithValue("extraction_schema_version", "graph-schema-v2"),
				HaveKeyWithValue("status", model.SnapshotStatusPending.String()),
			))
		})

		It("returns a domain idempotency error when graph snapshot insert is replayed", func() {
			readyRow := newGraphSnapshotRow(uuid.New(), featureID, embeddingID, datasetID, userID, orgID, idempotencyKey)
			readyRow.Status = model.SnapshotStatusReady.String()
			poolMock.NextRows = []pgx.Row{
				newEmbeddingSnapshotRow(embeddingID, featureID, datasetID, userID, orgID),
				errorRow{err: pgx.ErrNoRows},
				readyRow,
			}

			graphSnapshot, err := repository.SavePendingGraphSnapshot(ctx, tx, embeddingID, idempotencyKey, model.GraphExtractionStrategy{})

			Expect(graphSnapshot).To(BeNil())
			existing, ok := domain.IsGraphAlreadyMaterialized(err)
			Expect(ok).To(BeTrue())
			Expect(existing.GraphSnapshotID).To(Equal(readyRow.GraphSnapshotID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(3))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("FROM test_db.graph_snapshots WHERE idempotency_key = @idempotency_key"))
		})

		It("maps missing embedding snapshots before inserting graph snapshots", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			graphSnapshot, err := repository.SavePendingGraphSnapshot(ctx, tx, embeddingID, idempotencyKey, model.GraphExtractionStrategy{})

			Expect(graphSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrEmbeddingSnapshotNotFound)).To(BeTrue())
			Expect(poolMock.QueryRowCalledCount).To(Equal(1))
		})
	})

	Describe("ReadEmbeddingChunks", func() {
		It("reads chunks for graph extraction from embedding records", func() {
			recordID := uuid.New()
			poolMock.NextQueryRows = &testRows{rows: []pgx.Row{graphChunkRow{
				EmbeddingRecordID:   recordID,
				EmbeddingSnapshotID: embeddingID,
				DatasetID:           datasetID,
				UserID:              userID,
				OrgID:               orgID,
				ChunkIndex:          4,
				SourceText:          "Alice introduced Beta Corp.",
			}}}

			chunks, err := repository.ReadEmbeddingChunks(ctx, embeddingID)

			Expect(err).NotTo(HaveOccurred())
			Expect(chunks).To(HaveLen(1))
			Expect(chunks[0].EmbeddingRecordID).To(Equal(recordID))
			Expect(chunks[0].ChunkIndex).To(Equal(4))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.embedding_records"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ORDER BY chunk_index"))
			Expect(namedArgs(poolMock.QueryArgs[0])).To(HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: embeddingID, Valid: true}))
		})

		It("surfaces embedding chunk iterator errors", func() {
			rows := &testRows{err: errors.New("cursor failed")}
			poolMock.NextQueryRows = rows

			chunks, err := repository.ReadEmbeddingChunks(ctx, embeddingID)

			Expect(chunks).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("read embedding chunks rows")))
		})
	})

	Describe("SaveGraphMaterialization", func() {
		It("persists graph nodes, node chunks, and edges with tenant-scoped args", func() {
			graphSnapshotID := uuid.New()
			nodeA := uuid.New()
			nodeB := uuid.New()
			recordID := uuid.New()
			poolMock.NextRows = []pgx.Row{uuidTextRow{value: nodeA}, uuidTextRow{value: nodeB}}
			materialization := &model.GraphMaterialization{
				Snapshot: &model.GraphSnapshot{
					GraphSnapshotID: graphSnapshotID,
				},
				Nodes: []model.GraphNode{
					{DatasetID: datasetID, UserID: userID, OrgID: orgID, EntityKey: "person:alice", Name: "Alice", Type: "person", Description: "Alice entity", MentionCount: 2},
					{DatasetID: datasetID, UserID: userID, OrgID: orgID, EntityKey: "company:beta", Name: "Beta Corp", Type: "company", Description: "Beta entity", MentionCount: 1},
				},
				NodeChunks: []model.GraphNodeChunk{{
					EntityKey:           "person:alice",
					EmbeddingRecordID:   recordID,
					EmbeddingSnapshotID: embeddingID,
					DatasetID:           datasetID,
					UserID:              userID,
					OrgID:               orgID,
					ChunkIndex:          0,
					SourceText:          "Alice founded Beta Corp.",
				}},
				Edges: []model.GraphEdge{{
					DatasetID:       datasetID,
					UserID:          userID,
					OrgID:           orgID,
					SourceEntityKey: "person:alice",
					TargetEntityKey: "company:beta",
					RelationType:    "",
					Description:     "founded",
					Weight:          0.8,
				}},
			}

			err := repository.SaveGraphMaterialization(ctx, tx, materialization)

			Expect(err).NotTo(HaveOccurred())
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.ExecCalls).To(HaveLen(2))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.graph_nodes"))
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("INSERT INTO test_db.graph_node_chunks"))
			Expect(poolMock.ExecCalls[1]).To(ContainSubstring("INSERT INTO test_db.graph_edges"))
			chunkArgs := namedArgs(poolMock.ExecArgs[0])
			Expect(chunkArgs).To(SatisfyAll(
				HaveKeyWithValue("graph_snapshot_id", pgtype.UUID{Bytes: graphSnapshotID, Valid: true}),
				HaveKeyWithValue("graph_node_id", pgtype.UUID{Bytes: nodeA, Valid: true}),
				HaveKeyWithValue("embedding_record_id", pgtype.UUID{Bytes: recordID, Valid: true}),
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			))
			edgeArgs := namedArgs(poolMock.ExecArgs[1])
			Expect(edgeArgs).To(SatisfyAll(
				HaveKeyWithValue("source_node_id", pgtype.UUID{Bytes: nodeA, Valid: true}),
				HaveKeyWithValue("target_node_id", pgtype.UUID{Bytes: nodeB, Valid: true}),
				HaveKeyWithValue("relation_type", "RELATED_TO"),
			))
		})

		It("rejects nil graph materializations before querying", func() {
			err := repository.SaveGraphMaterialization(ctx, tx, nil)

			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(poolMock.QueryRowCalled).To(BeFalse())
			Expect(poolMock.ExecCalled).To(BeFalse())
		})
	})

	Describe("MarkGraphReady", func() {
		It("marks the graph snapshot ready and activates it for retrieval", func() {
			graphSnapshotID := uuid.New()
			poolMock.NextRows = []pgx.Row{int64Row{value: 4}}
			graphSnapshot := &model.GraphSnapshot{
				GraphSnapshotID:         graphSnapshotID,
				DatasetID:               datasetID,
				OrgID:                   orgID,
				ProvenanceHash:          "sha256:graph",
				ExtractionModel:         "llm-graph-extractor",
				ExtractionPromptVersion: "graph-prompt-v2",
				ExtractionSchemaVersion: "graph-schema-v2",
				ChunkCount:              3,
				ChunksProcessed:         3,
				EntityCount:             2,
				EdgeCount:               1,
			}

			err := repository.MarkGraphReady(ctx, tx, graphSnapshot)

			Expect(err).NotTo(HaveOccurred())
			Expect(graphSnapshot.MaterializationEventSeq).To(Equal(int64(4)))
			Expect(graphSnapshot.ActiveForRetrieval).To(BeTrue())
			Expect(poolMock.ExecCalls).To(HaveLen(2))
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("active_for_retrieval = false"))
			Expect(poolMock.ExecCalls[1]).To(ContainSubstring("UPDATE test_db.graph_snapshots"))
			args := namedArgs(poolMock.ExecArgs[1])
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("graph_snapshot_id", pgtype.UUID{Bytes: graphSnapshotID, Valid: true}),
				HaveKeyWithValue("materialization_event_seq", int64(4)),
				HaveKeyWithValue("provenance_hash", "sha256:graph"),
				HaveKeyWithValue("status", model.SnapshotStatusReady.String()),
			))
		})

		It("returns graph snapshot not found when no row is updated", func() {
			poolMock.NextRows = []pgx.Row{int64Row{value: 4}}
			poolMock.NextRowsAffected = 0

			err := repository.MarkGraphReady(ctx, tx, &model.GraphSnapshot{GraphSnapshotID: uuid.New(), DatasetID: datasetID, OrgID: orgID})

			Expect(errors.Is(err, domain.ErrGraphSnapshotNotFound)).To(BeTrue())
		})
	})

	Describe("MarkGraphFailed", func() {
		It("marks the graph snapshot failed with a reason", func() {
			graphSnapshotID := uuid.New()

			err := repository.MarkGraphFailed(ctx, tx, graphSnapshotID, "graph extraction failed")

			Expect(err).NotTo(HaveOccurred())
			Expect(poolMock.ExecCalls[0]).To(ContainSubstring("UPDATE test_db.graph_snapshots"))
			args := namedArgs(poolMock.ExecArgs[0])
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("graph_snapshot_id", pgtype.UUID{Bytes: graphSnapshotID, Valid: true}),
				HaveKeyWithValue("failure_reason", "graph extraction failed"),
				HaveKeyWithValue("status", model.SnapshotStatusFailed.String()),
			))
		})
	})

	Describe("ReadActiveGraphSnapshot", func() {
		It("reads the ready active graph snapshot for a dataset", func() {
			graphSnapshotID := uuid.New()
			row := newGraphSnapshotRow(graphSnapshotID, featureID, embeddingID, datasetID, userID, orgID, idempotencyKey)
			row.Status = model.SnapshotStatusReady.String()
			row.ActiveForRetrieval = true
			poolMock.NextRows = []pgx.Row{row}

			graphSnapshot, err := repository.ReadActiveGraphSnapshot(ctx, userID, datasetID)

			Expect(err).NotTo(HaveOccurred())
			Expect(graphSnapshot.GraphSnapshotID).To(Equal(graphSnapshotID))
			Expect(graphSnapshot.ActiveForRetrieval).To(BeTrue())
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("active_for_retrieval = true"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}),
				HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}),
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
				HaveKeyWithValue("status", model.SnapshotStatusReady.String()),
			))
		})

		It("returns graph snapshot not found when no active graph snapshot exists", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			graphSnapshot, err := repository.ReadActiveGraphSnapshot(ctx, userID, datasetID)

			Expect(graphSnapshot).To(BeNil())
			Expect(errors.Is(err, domain.ErrGraphSnapshotNotFound)).To(BeTrue())
		})
	})

	Describe("SearchGraph", func() {
		It("returns contexts, matched entities, and recursive graph paths", func() {
			graphSnapshotID := uuid.New()
			nodeA := uuid.New()
			nodeB := uuid.New()
			nodeC := uuid.New()
			nodeChunkID := uuid.New()
			recordID := uuid.New()
			poolMock.NextQueryRowsQueue = []pgx.Rows{
				&testRows{rows: []pgx.Row{
					graphRetrievedContextRow{
						GraphNodeChunkID:    nodeChunkID,
						GraphNodeID:         nodeC,
						EmbeddingRecordID:   recordID,
						EmbeddingSnapshotID: embeddingID,
						DatasetID:           datasetID,
						ChunkIndex:          7,
						SourceText:          "Alice introduced Beta Corp through Acme Corp.",
						Score:               0.8,
						OrgID:               orgID,
					},
				}},
				&testRows{rows: []pgx.Row{
					graphMatchedEntityRow{
						GraphNodeID: nodeA,
						Name:        "Alice",
						Type:        "person",
						Description: "Alice entity",
						Score:       1,
					},
				}},
				&testRows{rows: []pgx.Row{
					graphPathRow{
						GraphNodeIDs:  nodeA.String() + "," + nodeB.String() + "," + nodeC.String(),
						RelationTypes: "WORKS_WITH,INTRODUCED_TO",
						Score:         0.7,
					},
				}},
			}

			result, err := repository.SearchGraph(ctx, &model.GraphSnapshot{
				GraphSnapshotID:     graphSnapshotID,
				FeatureSnapshotID:   featureID,
				EmbeddingSnapshotID: embeddingID,
				DatasetID:           datasetID,
				OrgID:               orgID,
			}, "Alice", 5, 2)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.Contexts).To(HaveLen(1))
			Expect(result.Contexts[0].GraphNodeChunkID).To(Equal(nodeChunkID))
			Expect(result.Contexts[0].SourceText).To(ContainSubstring("Beta Corp"))
			Expect(result.MatchedEntities).To(HaveLen(1))
			Expect(result.MatchedEntities[0].GraphNodeID).To(Equal(nodeA))
			Expect(result.Paths).To(HaveLen(1))
			Expect(result.Paths[0].GraphNodeIDs).To(Equal([]uuid.UUID{nodeA, nodeB, nodeC}))
			Expect(result.Paths[0].RelationTypes).To(Equal([]string{"WORKS_WITH", "INTRODUCED_TO"}))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("WITH RECURSIVE seed"))
			Expect(poolMock.QueryCalls[2]).To(ContainSubstring("array_to_string(path, ',')"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("graph_snapshot_id", pgtype.UUID{Bytes: graphSnapshotID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("needle", "%alice%"))
			Expect(args).To(HaveKeyWithValue("max_hops", 2))
		})
	})
})

func namedArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func newRawSnapshotRow(rawSnapshotID, datasetID, userID uuid.UUID, orgIDs ...uuid.UUID) rawSnapshotRow {
	return rawSnapshotRow{
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         datasetID,
		UserID:            userID,
		OrgID:             selectedOrgID(orgIDs),
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

func newFeatureSnapshotRow(featureSnapshotID, rawSnapshotID, datasetID, userID uuid.UUID, orgIDs ...uuid.UUID) featureSnapshotRow {
	return featureSnapshotRow{
		FeatureSnapshotID: featureSnapshotID,
		RawSnapshotID:     rawSnapshotID,
		DatasetID:         datasetID,
		UserID:            userID,
		OrgID:             selectedOrgID(orgIDs),
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

func newEmbeddingSnapshotRow(embeddingSnapshotID, featureSnapshotID, datasetID, userID uuid.UUID, orgIDs ...uuid.UUID) embeddingSnapshotRow {
	strategy := validEmbeddingStrategy()
	return embeddingSnapshotRow{
		EmbeddingSnapshotID: embeddingSnapshotID,
		FeatureSnapshotID:   featureSnapshotID,
		DatasetID:           datasetID,
		UserID:              userID,
		OrgID:               selectedOrgID(orgIDs),
		VectorStore:         "pgvector",
		CollectionName:      "movies",
		EmbeddingDimensions: strategy.EmbeddingDimensions,
		EmbeddingCount:      3,
		StrategyVersion:     strategy.StrategyVersion,
		ExtractorName:       strategy.ExtractorName,
		ExtractorVersion:    strategy.ExtractorVersion,
		CleanerName:         strategy.CleanerName,
		CleanerVersion:      strategy.CleanerVersion,
		ChunkerName:         strategy.ChunkerName,
		ChunkerVersion:      strategy.ChunkerVersion,
		ChunkSize:           strategy.ChunkSize,
		ChunkOverlap:        strategy.ChunkOverlap,
		EmbeddingProvider:   strategy.EmbeddingProvider,
		EmbeddingModel:      strategy.EmbeddingModel,
		Status:              model.SnapshotStatusPending.String(),
	}
}

func newGraphSnapshotRow(graphSnapshotID, featureSnapshotID, embeddingSnapshotID, datasetID, userID, orgID, idempotencyKey uuid.UUID) graphSnapshotRow {
	return graphSnapshotRow{
		GraphSnapshotID:         graphSnapshotID,
		FeatureSnapshotID:       featureSnapshotID,
		EmbeddingSnapshotID:     embeddingSnapshotID,
		DatasetID:               datasetID,
		UserID:                  userID,
		OrgID:                   orgID,
		MaterializationEventSeq: 0,
		IdempotencyKey:          idempotencyKey,
		ProvenanceHash:          "",
		ExtractionModel:         "llm-graph-extractor",
		ExtractionPromptVersion: "graph-prompt-v2",
		ExtractionSchemaVersion: "graph-schema-v2",
		ChunkCount:              0,
		ChunksProcessed:         0,
		EntityCount:             0,
		EdgeCount:               0,
		ActiveForRetrieval:      false,
		Status:                  model.SnapshotStatusPending.String(),
		FailureReason:           "",
	}
}

func selectedOrgID(orgIDs []uuid.UUID) uuid.UUID {
	if len(orgIDs) > 0 && orgIDs[0] != uuid.Nil {
		return orgIDs[0]
	}
	return uuid.New()
}

func validEmbeddingStrategy() model.EmbeddingStrategy {
	return model.ApplyEmbeddingStrategyDefaults(model.EmbeddingStrategy{
		StrategyVersion:     "rag-v1",
		ExtractorName:       model.DefaultExtractorName,
		ExtractorVersion:    "v1",
		CleanerName:         "go-basic-text-cleaner",
		CleanerVersion:      "v1",
		ChunkerName:         "go-token-window",
		ChunkerVersion:      "v1",
		ChunkSize:           128,
		ChunkOverlap:        16,
		EmbeddingProvider:   "ollama",
		EmbeddingModel:      model.DefaultEmbeddingModel,
		EmbeddingDimensions: 384,
	})
}
