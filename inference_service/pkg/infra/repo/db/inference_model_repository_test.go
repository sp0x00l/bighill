package db_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestInferenceRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service db unit test suite")
}

type connectionPoolStub struct {
	nextRows      []pgx.Row
	nextQueryRows []pgx.Rows
	nextQueryErr  error
	nextExecErr   error
	execTag       pgconn.CommandTag

	queryRowCalled bool
	queryCalled    bool
	execCalled     bool
	closeCalled    bool
	commitCalled   bool
	rollbackCalled bool

	lastQuery string
	lastArgs  []any
	queries   []string
	args      [][]any
}

func (p *connectionPoolStub) Close() {
	p.closeCalled = true
}

func (p *connectionPoolStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.queryRowCalled = true
	p.capture(sql, args...)
	if len(p.nextRows) == 0 {
		return &repositoryRow{err: pgx.ErrNoRows}
	}
	row := p.nextRows[0]
	p.nextRows = p.nextRows[1:]
	return row
}

func (p *connectionPoolStub) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.queryCalled = true
	p.capture(sql, args...)
	if p.nextQueryErr != nil {
		return nil, p.nextQueryErr
	}
	if len(p.nextQueryRows) == 0 {
		return &repositoryRows{}, nil
	}
	rows := p.nextQueryRows[0]
	p.nextQueryRows = p.nextQueryRows[1:]
	return rows, nil
}

func (p *connectionPoolStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execCalled = true
	p.capture(sql, args...)
	if p.nextExecErr != nil {
		return pgconn.CommandTag{}, p.nextExecErr
	}
	if p.execTag.String() == "" {
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	return p.execTag, nil
}

func (p *connectionPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return &inferenceTxStub{pool: p}, nil
}

type inferenceTxStub struct {
	pool *connectionPoolStub
}

func (tx *inferenceTxStub) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *inferenceTxStub) Commit(context.Context) error {
	tx.pool.commitCalled = true
	return nil
}

func (tx *inferenceTxStub) Rollback(context.Context) error {
	tx.pool.rollbackCalled = true
	return nil
}

func (tx *inferenceTxStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *inferenceTxStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *inferenceTxStub) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *inferenceTxStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *inferenceTxStub) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.pool.Exec(ctx, sql, args...)
}

func (tx *inferenceTxStub) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.pool.Query(ctx, sql, args...)
}

func (tx *inferenceTxStub) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.pool.QueryRow(ctx, sql, args...)
}

func (tx *inferenceTxStub) Conn() *pgx.Conn {
	return nil
}

func (p *connectionPoolStub) capture(sql string, args ...any) {
	p.lastQuery = sql
	p.lastArgs = args
	p.queries = append(p.queries, sql)
	p.args = append(p.args, args)
}

type repositoryRow struct {
	values []any
	err    error
}

type repositoryRows struct {
	rows   [][]any
	index  int
	closed bool
	err    error
}

func (r *repositoryRows) Close() {
	r.closed = true
}

func (r *repositoryRows) Err() error {
	return r.err
}

func (r *repositoryRows) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT")
}

func (r *repositoryRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *repositoryRows) Next() bool {
	if r.index >= len(r.rows) {
		r.closed = true
		return false
	}
	r.index++
	return true
}

func (r *repositoryRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.rows) {
		return errors.New("repository rows scan before next")
	}
	values := r.rows[r.index-1]
	Expect(dest).To(HaveLen(len(values)))
	for i, value := range values {
		assignScanValue(dest[i], value)
	}
	return nil
}

func (r *repositoryRows) Values() ([]any, error) {
	if r.index == 0 || r.index > len(r.rows) {
		return nil, errors.New("repository rows values before next")
	}
	return r.rows[r.index-1], nil
}

func (r *repositoryRows) RawValues() [][]byte {
	return nil
}

func (r *repositoryRows) Conn() *pgx.Conn {
	return nil
}

func (r *repositoryRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	Expect(dest).To(HaveLen(len(r.values)))
	for i, value := range r.values {
		assignScanValue(dest[i], value)
	}
	return nil
}

func assignScanValue(dest any, value any) {
	switch typed := dest.(type) {
	case *string:
		*typed = value.(string)
	case *int:
		*typed = value.(int)
	case *int64:
		switch v := value.(type) {
		case int:
			*typed = int64(v)
		case int64:
			*typed = v
		default:
			Fail(fmt.Sprintf("unsupported int64 scan value %T", value))
		}
	case *bool:
		*typed = value.(bool)
	case *time.Time:
		*typed = value.(time.Time)
	default:
		Fail(fmt.Sprintf("unsupported scan destination %T", dest))
	}
}

func namedArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func validInferenceModel() *model.InferenceModel {
	return &model.InferenceModel{
		ModelID:           uuid.New(),
		UserID:            uuid.New(),
		OrgID:             uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		ModelKind:         model.ModelKindFineTuned,
		Source:            model.ModelSourceTraining,
		SourceMetadata:    "{}",
		Name:              "fraud-rag-ranker",
		LineageName:       "fraud-rag",
		ModelVersion:      7,
		BaseModel:         "bge-small-en-v1.5",
		ArtifactLocation:  "s3://models/fraud-rag-ranker/7/model.onnx",
		ArtifactFormat:    "ONNX",
		ArtifactChecksum:  "sha256:model",
		ArtifactSizeBytes: 9216,
		AdapterURI:        "s3://models/fraud-rag-ranker/7",
		ServingTarget:     "vllm-local",
		ServingModel:      "fraud-rag-ranker-v7",
		ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
		ServingLoadStatus: model.ModelLoadStatusLoaded,
		EffectiveBaseID:   "sha256-effective-base",
		MetricsMetadata:   `{"accuracy":0.93}`,
		Status:            model.ModelStatusReady,
		FailureReason:     "",
	}
}

func inferenceModelRow(inferenceModel *model.InferenceModel) pgx.Row {
	return &repositoryRow{values: []any{
		inferenceModel.ModelID.String(),
		inferenceModel.UserID.String(),
		optionalInferenceModelUUIDString(inferenceModel.OrgID),
		optionalInferenceModelUUIDString(inferenceModel.TrainingRunID),
		optionalInferenceModelUUIDString(inferenceModel.DatasetID),
		inferenceModel.ModelKind.String(),
		inferenceModel.Source.String(),
		inferenceModel.SourceURI,
		inferenceModel.SourceMetadata,
		inferenceModel.Name,
		inferenceModel.LineageName,
		inferenceModel.ModelVersion,
		inferenceModel.BaseModel,
		inferenceModel.ArtifactLocation,
		inferenceModel.ArtifactFormat,
		inferenceModel.ArtifactChecksum,
		inferenceModel.ArtifactSizeBytes,
		inferenceModel.AdapterURI,
		inferenceModel.ServingTarget,
		inferenceModel.ServingModel,
		inferenceModel.ServingProtocol.String(),
		inferenceModel.ServingLoadStatus.String(),
		inferenceModel.EffectiveBaseID,
		inferenceModel.MetricsMetadata,
		inferenceModel.Status.String(),
		inferenceModel.FailureReason,
	}}
}

func optionalInferenceModelUUIDString(value uuid.UUID) string {
	if value == uuid.Nil {
		return ""
	}
	return value.String()
}

var _ = Describe("InferenceModelRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.InferenceModelRepository
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewInferenceModelRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	Describe("UpsertModel", func() {
		It("upserts a model projection and scans the saved row", func() {
			inferenceModel := validInferenceModel()
			idempotencyKey := uuid.New()
			pool.nextRows = []pgx.Row{inferenceModelRow(inferenceModel)}

			record, err := repository.UpsertModel(ctx, inferenceModel, idempotencyKey)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(inferenceModel))
			Expect(pool.queryRowCalled).To(BeTrue())
			Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.inference_models"))
			Expect(pool.lastQuery).To(ContainSubstring("ON CONFLICT (model_id) DO UPDATE SET"))
			Expect(pool.lastQuery).To(ContainSubstring("RETURNING model_id::text"))
			args := namedArgs(pool.lastArgs)
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("model_id", pgtype.UUID{Bytes: inferenceModel.ModelID, Valid: true}),
				HaveKeyWithValue("user_id", pgtype.UUID{Bytes: inferenceModel.UserID, Valid: true}),
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: inferenceModel.OrgID, Valid: true}),
				HaveKeyWithValue("training_run_id", pgtype.UUID{Bytes: inferenceModel.TrainingRunID, Valid: true}),
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: inferenceModel.DatasetID, Valid: true}),
				HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}),
				HaveKeyWithValue("model_kind", model.ModelKindFineTuned.String()),
				HaveKeyWithValue("source", model.ModelSourceTraining.String()),
				HaveKeyWithValue("source_metadata", "{}"),
				HaveKeyWithValue("name", inferenceModel.Name),
				HaveKeyWithValue("lineage_name", inferenceModel.LineageName),
				HaveKeyWithValue("model_version", inferenceModel.ModelVersion),
				HaveKeyWithValue("adapter_uri", inferenceModel.AdapterURI),
				HaveKeyWithValue("serving_target", inferenceModel.ServingTarget),
				HaveKeyWithValue("serving_model", inferenceModel.ServingModel),
				HaveKeyWithValue("serving_protocol", inferenceModel.ServingProtocol.String()),
				HaveKeyWithValue("serving_load_status", model.ModelLoadStatusLoaded.String()),
				HaveKeyWithValue("effective_base_id", inferenceModel.EffectiveBaseID),
				HaveKeyWithValue("metrics_metadata", inferenceModel.MetricsMetadata),
				HaveKeyWithValue("status", model.ModelStatusReady.String()),
			))
		})

		It("upserts a base model with nullable training and dataset ids", func() {
			inferenceModel := validInferenceModel()
			inferenceModel.UserID = uuid.Nil
			inferenceModel.OrgID = uuid.Nil
			inferenceModel.TrainingRunID = uuid.Nil
			inferenceModel.DatasetID = uuid.Nil
			inferenceModel.ModelKind = model.ModelKindBase
			inferenceModel.Source = model.ModelSourceUpload
			inferenceModel.SourceURI = "s3://local-dev-bucket/models/base"
			pool.nextRows = []pgx.Row{inferenceModelRow(inferenceModel)}

			record, err := repository.UpsertModel(ctx, inferenceModel, uuid.New())

			Expect(err).NotTo(HaveOccurred())
			Expect(record.ModelKind).To(Equal(model.ModelKindBase))
			Expect(record.TrainingRunID).To(Equal(uuid.Nil))
			Expect(record.DatasetID).To(Equal(uuid.Nil))
			args := namedArgs(pool.lastArgs)
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("user_id", pgtype.UUID{}),
				HaveKeyWithValue("org_id", pgtype.UUID{}),
				HaveKeyWithValue("training_run_id", pgtype.UUID{}),
				HaveKeyWithValue("dataset_id", pgtype.UUID{}),
				HaveKeyWithValue("model_kind", model.ModelKindBase.String()),
				HaveKeyWithValue("source", model.ModelSourceUpload.String()),
				HaveKeyWithValue("source_uri", "s3://local-dev-bucket/models/base"),
			))
		})

		It("wraps scan failures", func() {
			pool.nextRows = []pgx.Row{&repositoryRow{err: errors.New("scan failed")}}

			record, err := repository.UpsertModel(ctx, validInferenceModel(), uuid.New())

			Expect(err).To(MatchError(ContainSubstring("upsert inference model: scan failed")))
			Expect(record).To(BeNil())
		})
	})

	Describe("ReadByID", func() {
		It("reads a model by id", func() {
			inferenceModel := validInferenceModel()
			pool.nextRows = []pgx.Row{inferenceModelRow(inferenceModel)}

			record, err := repository.ReadByID(ctx, inferenceModel.OrgID, inferenceModel.ModelID)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(inferenceModel))
			Expect(pool.lastQuery).To(ContainSubstring("SELECT model_id::text"))
			Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.inference_models WHERE model_id = @model_id AND (org_id = @org_id OR org_id IS NULL)"))
			Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: inferenceModel.ModelID, Valid: true}))
			Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: inferenceModel.OrgID, Valid: true}))
		})

		It("reads a shared base model for any tenant", func() {
			inferenceModel := validInferenceModel()
			inferenceModel.UserID = uuid.Nil
			inferenceModel.OrgID = uuid.Nil
			inferenceModel.TrainingRunID = uuid.Nil
			inferenceModel.DatasetID = uuid.Nil
			inferenceModel.ModelKind = model.ModelKindBase
			row := inferenceModelRow(inferenceModel).(*repositoryRow)
			row.values[1] = ""
			row.values[2] = ""
			pool.nextRows = []pgx.Row{row}

			record, err := repository.ReadByID(ctx, uuid.New(), inferenceModel.ModelID)

			Expect(err).NotTo(HaveOccurred())
			Expect(record.UserID).To(Equal(uuid.Nil))
			Expect(record.OrgID).To(Equal(uuid.Nil))
			Expect(record.ModelKind).To(Equal(model.ModelKindBase))
			Expect(record.DatasetID).To(Equal(uuid.Nil))
			Expect(record.TrainingRunID).To(Equal(uuid.Nil))
			Expect(pool.lastQuery).To(ContainSubstring("org_id IS NULL"))
		})

		It("returns a domain not-found error when no model row exists", func() {
			record, err := repository.ReadByID(ctx, uuid.New(), uuid.New())

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})

		It("surfaces invalid persisted status values", func() {
			inferenceModel := validInferenceModel()
			row := inferenceModelRow(inferenceModel).(*repositoryRow)
			row.values[24] = "BROKEN"
			pool.nextRows = []pgx.Row{row}

			record, err := repository.ReadByID(ctx, inferenceModel.OrgID, inferenceModel.ModelID)

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring(`invalid model status "BROKEN"`)))
		})

		It("surfaces invalid persisted serving load status values", func() {
			inferenceModel := validInferenceModel()
			row := inferenceModelRow(inferenceModel).(*repositoryRow)
			row.values[21] = "BROKEN"
			pool.nextRows = []pgx.Row{row}

			record, err := repository.ReadByID(ctx, inferenceModel.OrgID, inferenceModel.ModelID)

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring(`invalid model load status "BROKEN"`)))
		})
	})
})

func capabilityReportRow(report *model.CapabilityReport) pgx.Row {
	return &repositoryRow{values: []any{
		report.CapabilityReportID.String(),
		report.EffectiveBaseID,
		report.SupportsChat,
		report.SupportsToolCalls,
		report.SupportsSystemPrompt,
		report.CreatedAt,
	}}
}

var _ = Describe("CapabilityReportRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.CapabilityReportRepository
		report     *model.CapabilityReport
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewCapabilityReportRepository(coreDB.NewDatabase(pool, "test_db"))
		report = &model.CapabilityReport{
			CapabilityReportID:   uuid.New(),
			EffectiveBaseID:      "sha256-effective-base",
			SupportsChat:         true,
			SupportsToolCalls:    true,
			SupportsSystemPrompt: true,
			CreatedAt:            time.Now().UTC(),
		}
	})

	It("records artifact-intrinsic capabilities keyed by effective base", func() {
		pool.nextRows = []pgx.Row{capabilityReportRow(report)}

		record, err := repository.RecordCapabilityReport(ctx, report)

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(report))
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.capability_reports"))
		Expect(pool.lastQuery).To(ContainSubstring("ON CONFLICT (effective_base_id) DO UPDATE SET"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("org_id"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("model_id"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("max_output_tokens"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("context_window_tokens"))
		Expect(namedArgs(pool.lastArgs)).To(SatisfyAll(
			HaveKeyWithValue("effective_base_id", "sha256-effective-base"),
			HaveKeyWithValue("supports_chat", true),
			HaveKeyWithValue("supports_tool_calls", true),
			HaveKeyWithValue("supports_system_prompt", true),
		))
	})

	It("reads capability by effective base digest", func() {
		pool.nextRows = []pgx.Row{capabilityReportRow(report)}

		record, err := repository.ReadCapabilityReportForEffectiveBase(ctx, "sha256-effective-base")

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(report))
		Expect(pool.lastQuery).To(ContainSubstring("WHERE effective_base_id = @effective_base_id"))
		Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("effective_base_id", "sha256-effective-base"))
	})

	It("fails closed when no measured capability exists for the effective base", func() {
		record, err := repository.ReadCapabilityReportForEffectiveBase(ctx, "sha256-missing-base")

		Expect(record).To(BeNil())
		Expect(errors.Is(err, domain.ErrModelNotReady)).To(BeTrue())
	})
})
