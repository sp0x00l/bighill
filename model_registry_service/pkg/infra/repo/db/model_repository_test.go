package db_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	modeldb "model_registry_service/pkg/infra/repo/db"

	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry db unit test suite")
}

type testConnectionPool struct {
	CloseCalled         bool
	QueryRowCalledCount int
	QueryCalls          []string
	QueryArgs           [][]any
	ExecCalls           []string
	ExecArgs            [][]any
	NextRows            []pgx.Row
	NextRowsAffected    int64
	NextError           error
	CommitCalled        bool
	RollbackCalled      bool
}

func (p *testConnectionPool) Close() {
	p.CloseCalled = true
}

func (p *testConnectionPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
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
	return nil, nil
}

func (p *testConnectionPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ExecCalls = append(p.ExecCalls, sql)
	p.ExecArgs = append(p.ExecArgs, args)
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.NextRowsAffected)), p.NextError
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

func (tx *testTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.pool.Exec(ctx, sql, args...)
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

type errorRow struct {
	err error
}

func (r errorRow) Scan(...any) error {
	return r.err
}

type modelRow struct {
	ModelID           uuid.UUID
	TrainingRunID     uuid.UUID
	DatasetID         uuid.UUID
	Name              string
	ModelVersion      int
	BaseModel         string
	ArtifactLocation  string
	ArtifactFormat    string
	ArtifactChecksum  string
	ArtifactSizeBytes int64
	MetricsMetadata   string
	Status            string
	FailureReason     string
}

func (r modelRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.ModelID.String()
	*(dest[1].(*string)) = r.TrainingRunID.String()
	*(dest[2].(*string)) = r.DatasetID.String()
	*(dest[3].(*string)) = r.Name
	*(dest[4].(*int)) = r.ModelVersion
	*(dest[5].(*string)) = r.BaseModel
	*(dest[6].(*string)) = r.ArtifactLocation
	*(dest[7].(*string)) = r.ArtifactFormat
	*(dest[8].(*string)) = r.ArtifactChecksum
	*(dest[9].(*int64)) = r.ArtifactSizeBytes
	*(dest[10].(*string)) = r.MetricsMetadata
	*(dest[11].(*string)) = r.Status
	*(dest[12].(*string)) = r.FailureReason
	return nil
}

type orderedOutboxStub struct {
	tx      pgx.Tx
	message msgConn.OutboundMessage
	err     error
	calls   int
}

func (s *orderedOutboxStub) EnqueueTx(_ context.Context, tx pgx.Tx, msg msgConn.OutboundMessage) error {
	s.tx = tx
	s.message = msg
	s.calls++
	return s.err
}

var _ = Describe("ModelRepository", func() {
	var (
		ctx            context.Context
		poolMock       *testConnectionPool
		repository     *modeldb.ModelRepository
		modelID        uuid.UUID
		trainingRunID  uuid.UUID
		datasetID      uuid.UUID
		idempotencyKey uuid.UUID
		registered     *model.Model
	)

	BeforeEach(func() {
		ctx = context.Background()
		poolMock = &testConnectionPool{NextRowsAffected: 1}
		dbCore := coreDB.NewDatabase(poolMock, "test_db")
		repository = modeldb.NewModelRepository(dbCore)

		modelID = uuid.New()
		trainingRunID = uuid.New()
		datasetID = uuid.New()
		idempotencyKey = uuid.New()
		registered = &model.Model{
			ModelID:           modelID,
			TrainingRunID:     trainingRunID,
			DatasetID:         datasetID,
			Name:              "movie-ranker",
			ModelVersion:      7,
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/run",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			MetricsMetadata:   `{"eval_loss":0.12}`,
			Status:            model.ModelStatusReady,
		}
	})

	It("wraps the shared database with the configured schema name", func() {
		Expect(repository.Name).To(Equal("test_db"))
	})

	Describe("Create", func() {
		It("inserts a model with named args", func() {
			poolMock.NextRows = []pgx.Row{newModelRow(registered)}

			modelRecord, err := repository.Create(ctx, registered, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.ModelID).To(Equal(modelID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(1))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.models"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("RETURNING model_id::text"))

			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("training_run_id", pgtype.UUID{Bytes: trainingRunID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("name", registered.Name))
			Expect(args).To(HaveKeyWithValue("model_version", registered.ModelVersion))
			Expect(args).To(HaveKeyWithValue("artifact_format", registered.ArtifactFormat))
			Expect(args).To(HaveKeyWithValue("metrics_metadata", registered.MetricsMetadata))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusReady.String()))
		})

		It("returns the model-exists domain error for idempotency conflicts", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: &pgconn.PgError{Code: pgerrcode.UniqueViolation}}}

			modelRecord, err := repository.Create(ctx, registered, idempotencyKey)

			Expect(modelRecord).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelExists)).To(BeTrue())
		})

		It("enqueues model updates in the same transaction when an outbox is configured", func() {
			outbox := &orderedOutboxStub{}
			signalCount := 0
			dbCore := coreDB.NewDatabase(poolMock, "test_db")
			repository = modeldb.NewModelRepository(dbCore,
				modeldb.WithTransactionalOutbox(outbox, "model_registry"),
				modeldb.WithOutboxSignal(func() { signalCount++ }),
			)
			poolMock.NextRows = []pgx.Row{newModelRow(registered)}

			modelRecord, err := repository.Create(ctx, registered, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.ModelID).To(Equal(modelID))
			Expect(poolMock.CommitCalled).To(BeTrue())
			Expect(outbox.calls).To(Equal(1))
			Expect(outbox.tx).NotTo(BeNil())
			Expect(outbox.message.Topic).To(Equal("model_registry"))
			Expect(outbox.message.Message.ResourceKey).To(Equal(modelID))
			Expect(outbox.message.DispatchKey).To(ContainSubstring("model_updated:"))
			Expect(signalCount).To(Equal(1))
		})
	})

	Describe("ReadByID", func() {
		It("reads a model by model id", func() {
			poolMock.NextRows = []pgx.Row{newModelRow(registered)}

			modelRecord, err := repository.ReadByID(ctx, modelID)

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.ModelID).To(Equal(modelID))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("FROM test_db.models WHERE model_id = @model_id"))
			Expect(namedArgs(poolMock.QueryArgs[0])).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
		})

		It("returns a domain not-found error", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			modelRecord, err := repository.ReadByID(ctx, modelID)

			Expect(modelRecord).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})
	})

	Describe("UpdateStatus", func() {
		It("updates model status and returns the updated model", func() {
			ready := *registered
			ready.Status = model.ModelStatusReady
			ready.ArtifactLocation = "s3://local-dev-bucket/models/ready"
			poolMock.NextRows = []pgx.Row{newModelRow(&ready)}

			modelRecord, err := repository.UpdateStatus(ctx, modelID, model.ModelStatusReady, ready.ArtifactLocation, "")

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.Status).To(Equal(model.ModelStatusReady))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("UPDATE test_db.models"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusReady.String()))
			Expect(args).To(HaveKeyWithValue("artifact_location", ready.ArtifactLocation))
		})
	})
})

func newModelRow(modelRecord *model.Model) modelRow {
	return modelRow{
		ModelID:           modelRecord.ModelID,
		TrainingRunID:     modelRecord.TrainingRunID,
		DatasetID:         modelRecord.DatasetID,
		Name:              modelRecord.Name,
		ModelVersion:      modelRecord.ModelVersion,
		BaseModel:         modelRecord.BaseModel,
		ArtifactLocation:  modelRecord.ArtifactLocation,
		ArtifactFormat:    modelRecord.ArtifactFormat,
		ArtifactChecksum:  modelRecord.ArtifactChecksum,
		ArtifactSizeBytes: modelRecord.ArtifactSizeBytes,
		MetricsMetadata:   modelRecord.MetricsMetadata,
		Status:            modelRecord.Status.String(),
		FailureReason:     modelRecord.FailureReason,
	}
}

func namedArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}
