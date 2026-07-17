package db_test

import (
	"context"
	"errors"
	"time"

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

type effectiveBaseRow struct {
	EffectiveBaseID        uuid.UUID
	ModelID                uuid.UUID
	OrgID                  uuid.UUID
	BaseModel              string
	SourceArtifactLocation string
	SourceArtifactFormat   string
	SourceArtifactChecksum string
	ServingTarget          string
	ServingModel           string
	ServingProtocol        string
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

func (r effectiveBaseRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EffectiveBaseID.String()
	*(dest[1].(*string)) = r.ModelID.String()
	*(dest[2].(*string)) = r.OrgID.String()
	*(dest[3].(*string)) = r.BaseModel
	*(dest[4].(*string)) = r.SourceArtifactLocation
	*(dest[5].(*string)) = r.SourceArtifactFormat
	*(dest[6].(*string)) = r.SourceArtifactChecksum
	*(dest[7].(*string)) = r.ServingTarget
	*(dest[8].(*string)) = r.ServingModel
	*(dest[9].(*string)) = r.ServingProtocol
	*(dest[10].(*time.Time)) = r.CreatedAt
	*(dest[11].(*time.Time)) = r.UpdatedAt
	return nil
}

var _ = Describe("EffectiveBaseRepository", func() {
	var (
		ctx        context.Context
		poolMock   *testConnectionPool
		tx         pgx.Tx
		repository *modeldb.EffectiveBaseRepository
		modelID    uuid.UUID
		orgID      uuid.UUID
		record     *model.EffectiveBaseVersion
		row        effectiveBaseRow
	)

	BeforeEach(func() {
		ctx = context.Background()
		poolMock = &testConnectionPool{NextRowsAffected: 1}
		tx = &testTx{pool: poolMock}
		dbCore := coreDB.NewDatabase(poolMock, "test_db")
		repository = modeldb.NewEffectiveBaseRepository(dbCore)
		modelID = uuid.New()
		orgID = uuid.New()
		now := time.Now().UTC()
		record = &model.EffectiveBaseVersion{
			ModelID:                modelID,
			OrgID:                  orgID,
			BaseModel:              "mistral-7b",
			SourceArtifactLocation: "s3://local-dev-bucket/models/base.gguf",
			SourceArtifactFormat:   "GGUF",
			SourceArtifactChecksum: "sha256:base",
			ServingTarget:          "http://vllm-runtime",
			ServingModel:           "base-mistral",
			ServingProtocol:        model.ServingProtocolOpenAIChatCompletions,
		}
		row = effectiveBaseRow{
			EffectiveBaseID:        uuid.New(),
			ModelID:                modelID,
			OrgID:                  orgID,
			BaseModel:              record.BaseModel,
			SourceArtifactLocation: record.SourceArtifactLocation,
			SourceArtifactFormat:   record.SourceArtifactFormat,
			SourceArtifactChecksum: record.SourceArtifactChecksum,
			ServingTarget:          record.ServingTarget,
			ServingModel:           record.ServingModel,
			ServingProtocol:        record.ServingProtocol.String(),
			CreatedAt:              now,
			UpdatedAt:              now,
		}
	})

	Describe("RecordEffectiveBase", func() {
		It("records an effective base with a database issued id", func() {
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.EffectiveBaseID).To(Equal(row.EffectiveBaseID))
			Expect(result.ModelID).To(Equal(modelID))
			Expect(result.OrgID).To(Equal(orgID))
			Expect(result.SourceArtifactChecksum).To(Equal("sha256:base"))
			Expect(result.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.effective_base_versions"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ON CONFLICT (model_id, source_artifact_checksum, serving_target, serving_model, serving_protocol)"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("source_artifact_checksum", "sha256:base"))
			Expect(args).To(HaveKeyWithValue("serving_protocol", model.ServingProtocolOpenAIChatCompletions.String()))
		})

		It("maps a missing model foreign key to a validation error", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: &pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}}}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(result).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(err).To(MatchError(ContainSubstring("effective base references an unknown model")))
		})

		It("does not return a record when row decoding fails", func() {
			row.ServingProtocol = "NOT_A_PROTOCOL"
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("record effective base")))
			Expect(err).To(MatchError(ContainSubstring("invalid serving protocol")))
		})
	})

	Describe("ReadLatestByModelID", func() {
		It("reads the latest effective base for a model", func() {
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.ReadLatestByModelID(ctx, modelID)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.EffectiveBaseID).To(Equal(row.EffectiveBaseID))
			Expect(result.ServingModel).To(Equal("base-mistral"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ORDER BY updated_at DESC, created_at DESC, effective_base_id DESC"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
		})

		It("returns the domain not-found error when no effective base exists", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			result, err := repository.ReadLatestByModelID(ctx, modelID)

			Expect(result).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})

		It("wraps scan failures", func() {
			row.ServingProtocol = "NOT_A_PROTOCOL"
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.ReadLatestByModelID(ctx, modelID)

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("read effective base")))
			Expect(err).To(MatchError(ContainSubstring("invalid serving protocol")))
		})
	})
})
