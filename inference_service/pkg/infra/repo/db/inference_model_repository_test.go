package db_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
	nextRows     []pgx.Row
	nextQueryErr error
	nextExecErr  error
	execTag      pgconn.CommandTag

	queryRowCalled bool
	queryCalled    bool
	execCalled     bool
	closeCalled    bool

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
	return nil, nil
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
	return nil, nil
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
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		Name:              "fraud-rag-ranker",
		ModelVersion:      7,
		BaseModel:         "bge-small-en-v1.5",
		ArtifactLocation:  "s3://models/fraud-rag-ranker/7/model.onnx",
		ArtifactFormat:    "ONNX",
		ArtifactChecksum:  "sha256:model",
		ArtifactSizeBytes: 9216,
		MetricsMetadata:   `{"accuracy":0.93}`,
		Status:            model.ModelStatusReady,
		FailureReason:     "",
	}
}

func inferenceModelRow(inferenceModel *model.InferenceModel) pgx.Row {
	return &repositoryRow{values: []any{
		inferenceModel.ModelID.String(),
		inferenceModel.TrainingRunID.String(),
		inferenceModel.DatasetID.String(),
		inferenceModel.Name,
		inferenceModel.ModelVersion,
		inferenceModel.BaseModel,
		inferenceModel.ArtifactLocation,
		inferenceModel.ArtifactFormat,
		inferenceModel.ArtifactChecksum,
		inferenceModel.ArtifactSizeBytes,
		inferenceModel.MetricsMetadata,
		inferenceModel.Status.String(),
		inferenceModel.FailureReason,
	}}
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
				HaveKeyWithValue("training_run_id", pgtype.UUID{Bytes: inferenceModel.TrainingRunID, Valid: true}),
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: inferenceModel.DatasetID, Valid: true}),
				HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}),
				HaveKeyWithValue("name", inferenceModel.Name),
				HaveKeyWithValue("model_version", inferenceModel.ModelVersion),
				HaveKeyWithValue("metrics_metadata", inferenceModel.MetricsMetadata),
				HaveKeyWithValue("status", model.ModelStatusReady.String()),
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

			record, err := repository.ReadByID(ctx, inferenceModel.ModelID)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(inferenceModel))
			Expect(pool.lastQuery).To(ContainSubstring("SELECT model_id::text"))
			Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.inference_models WHERE model_id = @model_id"))
			Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: inferenceModel.ModelID, Valid: true}))
		})

		It("returns a domain not-found error when no model row exists", func() {
			record, err := repository.ReadByID(ctx, uuid.New())

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})

		It("surfaces invalid persisted status values", func() {
			inferenceModel := validInferenceModel()
			row := inferenceModelRow(inferenceModel).(*repositoryRow)
			row.values[11] = "BROKEN"
			pool.nextRows = []pgx.Row{row}

			record, err := repository.ReadByID(ctx, inferenceModel.ModelID)

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring(`invalid model status "BROKEN"`)))
		})
	})
})
