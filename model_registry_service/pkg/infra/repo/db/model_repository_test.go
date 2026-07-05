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
	ModelID            uuid.UUID
	UserID             uuid.UUID
	TrainingRunID      uuid.UUID
	DatasetID          uuid.UUID
	ModelKind          string
	Source             string
	SourceURI          string
	SourceMetadata     string
	Name               string
	ModelVersion       int
	BaseModel          string
	ArtifactLocation   string
	ArtifactFormat     string
	ArtifactChecksum   string
	ArtifactSizeBytes  int64
	AdapterURI         string
	ServingTarget      string
	ServingModel       string
	ServingLoadStatus  string
	MetricsMetadata    string
	PromotionReportURI string
	PromotionDeltas    string
	PromotionDecision  string
	Status             string
	FailureReason      string
}

func (r modelRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.ModelID.String()
	*(dest[1].(*string)) = r.UserID.String()
	*(dest[2].(*string)) = r.TrainingRunID.String()
	*(dest[3].(*string)) = r.DatasetID.String()
	*(dest[4].(*string)) = r.ModelKind
	*(dest[5].(*string)) = r.Source
	*(dest[6].(*string)) = r.SourceURI
	*(dest[7].(*string)) = r.SourceMetadata
	*(dest[8].(*string)) = r.Name
	*(dest[9].(*int)) = r.ModelVersion
	*(dest[10].(*string)) = r.BaseModel
	*(dest[11].(*string)) = r.ArtifactLocation
	*(dest[12].(*string)) = r.ArtifactFormat
	*(dest[13].(*string)) = r.ArtifactChecksum
	*(dest[14].(*int64)) = r.ArtifactSizeBytes
	*(dest[15].(*string)) = r.AdapterURI
	*(dest[16].(*string)) = r.ServingTarget
	*(dest[17].(*string)) = r.ServingModel
	*(dest[18].(*string)) = r.ServingLoadStatus
	*(dest[19].(*string)) = r.MetricsMetadata
	*(dest[20].(*string)) = r.PromotionReportURI
	*(dest[21].(*string)) = r.PromotionDeltas
	*(dest[22].(*string)) = r.PromotionDecision
	*(dest[23].(*string)) = r.Status
	*(dest[24].(*string)) = r.FailureReason
	return nil
}

var _ = Describe("ModelRepository", func() {
	var (
		ctx            context.Context
		poolMock       *testConnectionPool
		tx             pgx.Tx
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
		tx = &testTx{pool: poolMock}
		dbCore := coreDB.NewDatabase(poolMock, "test_db")
		repository = modeldb.NewModelRepository(dbCore)

		modelID = uuid.New()
		userID := uuid.New()
		trainingRunID = uuid.New()
		datasetID = uuid.New()
		idempotencyKey = uuid.New()
		registered = &model.Model{
			ModelID:           modelID,
			UserID:            userID,
			TrainingRunID:     trainingRunID,
			DatasetID:         datasetID,
			ModelKind:         model.ModelKindFineTuned,
			Source:            model.ModelSourceTraining,
			SourceMetadata:    "{}",
			Name:              "movie-ranker",
			ModelVersion:      7,
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/run",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterURI:        "s3://local-dev-bucket/models/run",
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v7",
			ServingLoadStatus: model.ModelLoadStatusLoaded,
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

			modelRecord, err := repository.Create(ctx, tx, registered, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.ModelID).To(Equal(modelID))
			Expect(poolMock.QueryRowCalledCount).To(Equal(1))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.models"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("RETURNING model_id::text"))

			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: registered.UserID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("training_run_id", pgtype.UUID{Bytes: trainingRunID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("model_kind", model.ModelKindFineTuned.String()))
			Expect(args).To(HaveKeyWithValue("source", model.ModelSourceTraining.String()))
			Expect(args).To(HaveKeyWithValue("source_metadata", "{}"))
			Expect(args).To(HaveKeyWithValue("name", registered.Name))
			Expect(args).To(HaveKeyWithValue("model_version", registered.ModelVersion))
			Expect(args).To(HaveKeyWithValue("artifact_format", registered.ArtifactFormat))
			Expect(args).To(HaveKeyWithValue("adapter_uri", registered.AdapterURI))
			Expect(args).To(HaveKeyWithValue("serving_target", registered.ServingTarget))
			Expect(args).To(HaveKeyWithValue("serving_model", registered.ServingModel))
			Expect(args).To(HaveKeyWithValue("serving_load_status", model.ModelLoadStatusLoaded.String()))
			Expect(args).To(HaveKeyWithValue("metrics_metadata", registered.MetricsMetadata))
			Expect(args).To(HaveKeyWithValue("promotion_deltas", "{}"))
			Expect(args).To(HaveKeyWithValue("promotion_decision", registered.PromotionDecision))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusReady.String()))
		})

		It("returns the model-exists domain error for idempotency conflicts", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: &pgconn.PgError{Code: pgerrcode.UniqueViolation}}}

			modelRecord, err := repository.Create(ctx, tx, registered, idempotencyKey)

			Expect(modelRecord).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelExists)).To(BeTrue())
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

	Describe("ReadChampion", func() {
		It("reads the loaded ready model with the highest version for a lineage", func() {
			poolMock.NextRows = []pgx.Row{newModelRow(registered)}

			modelRecord, err := repository.ReadChampion(ctx, model.Lineage{UserID: registered.UserID, Name: registered.Name})

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.ModelID).To(Equal(modelID))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ORDER BY model_version DESC"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("LIMIT 1"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: registered.UserID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("name", registered.Name))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusReady.String()))
			Expect(args).To(HaveKeyWithValue("serving_load_status", model.ModelLoadStatusLoaded.String()))
		})

		It("returns a domain not-found error when no champion exists", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			modelRecord, err := repository.ReadChampion(ctx, model.Lineage{UserID: registered.UserID, Name: registered.Name})

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

			modelRecord, err := repository.UpdateStatus(ctx, tx, modelID, model.ModelStatusReady, ready.ArtifactLocation, "")

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.Status).To(Equal(model.ModelStatusReady))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("UPDATE test_db.models"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusReady.String()))
			Expect(args).To(HaveKeyWithValue("artifact_location", ready.ArtifactLocation))
		})
	})

	Describe("UpdateServingStatus", func() {
		It("updates serving status and returns the updated model", func() {
			ready := *registered
			ready.Status = model.ModelStatusReady
			ready.ServingLoadStatus = model.ModelLoadStatusLoaded
			ready.PromotionDecision = model.PromotionDecisionReason(model.PromotionDecisionOutcomeAccepted, "candidate beats champion gate")
			poolMock.NextRows = []pgx.Row{newModelRow(&ready)}

			modelRecord, changed, err := repository.UpdateServingStatus(ctx, tx, modelID, model.ModelStatusReady, model.ModelLoadStatusLoaded, ready.ServingTarget, ready.ServingModel, "", idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeTrue())
			Expect(modelRecord.Status).To(Equal(model.ModelStatusReady))
			Expect(modelRecord.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
			Expect(modelRecord.PromotionDecision).To(Equal(ready.PromotionDecision))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("UPDATE test_db.models"))
			Expect(poolMock.QueryCalls[0]).NotTo(ContainSubstring("promotion_decision ="))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("serving_status_idempotency_key IS DISTINCT FROM @serving_status_idempotency_key"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusReady.String()))
			Expect(args).To(HaveKeyWithValue("serving_load_status", model.ModelLoadStatusLoaded.String()))
			Expect(args).To(HaveKeyWithValue("serving_target", ready.ServingTarget))
			Expect(args).To(HaveKeyWithValue("serving_model", ready.ServingModel))
			Expect(args).To(HaveKeyWithValue("serving_status_idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}))
		})

		It("returns unchanged when serving status already matches", func() {
			poolMock.NextRows = []pgx.Row{
				errorRow{err: pgx.ErrNoRows},
				newModelRow(registered),
			}

			modelRecord, changed, err := repository.UpdateServingStatus(ctx, tx, modelID, registered.Status, registered.ServingLoadStatus, registered.ServingTarget, registered.ServingModel, "", idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.ModelID).To(Equal(modelID))
			Expect(changed).To(BeFalse())
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("SELECT model_id::text"))
		})
	})

	Describe("UpdatePromotionDecision", func() {
		It("updates promotion evidence and returns the updated model", func() {
			ready := *registered
			ready.Status = model.ModelStatusEvaluated
			ready.PromotionReportURI = "s3://local-dev-bucket/promotion/model.json"
			ready.PromotionDeltas = `{"faithfulness":0.1}`
			ready.PromotionDecision = model.PromotionDecisionReason(model.PromotionDecisionOutcomeAccepted, "candidate beats champion gate")
			poolMock.NextRows = []pgx.Row{newModelRow(&ready)}

			modelRecord, err := repository.UpdatePromotionDecision(ctx, tx, modelID, model.ModelStatusEvaluated, ready.PromotionReportURI, ready.PromotionDeltas, ready.PromotionDecision, "")

			Expect(err).NotTo(HaveOccurred())
			Expect(modelRecord.Status).To(Equal(model.ModelStatusEvaluated))
			Expect(modelRecord.PromotionReportURI).To(Equal(ready.PromotionReportURI))
			Expect(modelRecord.PromotionDeltas).To(MatchJSON(ready.PromotionDeltas))
			Expect(modelRecord.PromotionDecision).To(Equal(ready.PromotionDecision))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("promotion_report_uri = @promotion_report_uri"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("promotion_decision = @promotion_decision"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("status", model.ModelStatusEvaluated.String()))
			Expect(args).To(HaveKeyWithValue("promotion_report_uri", ready.PromotionReportURI))
			Expect(args).To(HaveKeyWithValue("promotion_deltas", ready.PromotionDeltas))
			Expect(args).To(HaveKeyWithValue("promotion_decision", ready.PromotionDecision))
		})
	})
})

func newModelRow(modelRecord *model.Model) modelRow {
	return modelRow{
		ModelID:            modelRecord.ModelID,
		UserID:             modelRecord.UserID,
		TrainingRunID:      modelRecord.TrainingRunID,
		DatasetID:          modelRecord.DatasetID,
		ModelKind:          modelRecord.ModelKind.String(),
		Source:             modelRecord.Source.String(),
		SourceURI:          modelRecord.SourceURI,
		SourceMetadata:     modelRecord.SourceMetadata,
		Name:               modelRecord.Name,
		ModelVersion:       modelRecord.ModelVersion,
		BaseModel:          modelRecord.BaseModel,
		ArtifactLocation:   modelRecord.ArtifactLocation,
		ArtifactFormat:     modelRecord.ArtifactFormat,
		ArtifactChecksum:   modelRecord.ArtifactChecksum,
		ArtifactSizeBytes:  modelRecord.ArtifactSizeBytes,
		AdapterURI:         modelRecord.AdapterURI,
		ServingTarget:      modelRecord.ServingTarget,
		ServingModel:       modelRecord.ServingModel,
		ServingLoadStatus:  modelRecord.ServingLoadStatus.String(),
		MetricsMetadata:    modelRecord.MetricsMetadata,
		PromotionReportURI: modelRecord.PromotionReportURI,
		PromotionDeltas:    withDefaultJSONForTest(modelRecord.PromotionDeltas),
		PromotionDecision:  modelRecord.PromotionDecision,
		Status:             modelRecord.Status.String(),
		FailureReason:      modelRecord.FailureReason,
	}
}

func withDefaultJSONForTest(value string) string {
	if value == "" {
		return "{}"
	}
	return value
}

func namedArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}
