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
	EffectiveBaseID         string
	FoundationModelID       uuid.UUID
	DescriptorSchemaVersion int
	FoundationChecksum      string
	Descriptor              string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func (r effectiveBaseRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EffectiveBaseID
	*(dest[1].(*string)) = r.FoundationModelID.String()
	*(dest[2].(*int)) = r.DescriptorSchemaVersion
	*(dest[3].(*string)) = r.FoundationChecksum
	*(dest[4].(*string)) = r.Descriptor
	*(dest[5].(*time.Time)) = r.CreatedAt
	*(dest[6].(*time.Time)) = r.UpdatedAt
	return nil
}

var _ = Describe("EffectiveBaseRepository", func() {
	var (
		ctx        context.Context
		poolMock   *testConnectionPool
		tx         pgx.Tx
		repository *modeldb.EffectiveBaseRepository
		modelID    uuid.UUID
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
		now := time.Now().UTC()
		descriptor := `{"descriptor_schema_version":1,"foundation_model_id":"` + modelID.String() + `","artifact_uri":"s3://local-dev-bucket/models/base.gguf","artifact_format":"GGUF","foundation_checksum":"sha256:base","serving_protocol":"OPENAI_CHAT_COMPLETIONS","serving_model":"base-mistral"}`
		record = &model.EffectiveBaseVersion{
			EffectiveBaseID:         "sha256digest",
			FoundationModelID:       modelID,
			DescriptorSchemaVersion: model.EffectiveBaseDescriptorSchemaVersion,
			FoundationChecksum:      "sha256:base",
			Descriptor:              descriptor,
		}
		row = effectiveBaseRow{
			EffectiveBaseID:         record.EffectiveBaseID,
			FoundationModelID:       modelID,
			DescriptorSchemaVersion: record.DescriptorSchemaVersion,
			FoundationChecksum:      record.FoundationChecksum,
			Descriptor:              record.Descriptor,
			CreatedAt:               now,
			UpdatedAt:               now,
		}
	})

	Describe("RecordEffectiveBase", func() {
		It("records an effective base by deterministic digest", func() {
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.EffectiveBaseID).To(Equal(row.EffectiveBaseID))
			Expect(result.FoundationModelID).To(Equal(modelID))
			Expect(result.DescriptorSchemaVersion).To(Equal(model.EffectiveBaseDescriptorSchemaVersion))
			Expect(result.FoundationChecksum).To(Equal("sha256:base"))
			Expect(result.Descriptor).To(MatchJSON(record.Descriptor))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("INSERT INTO test_db.effective_base_versions"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ON CONFLICT (effective_base_id) DO NOTHING"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("effective_base_id", "sha256digest"))
			Expect(args).To(HaveKeyWithValue("foundation_model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("descriptor_schema_version", model.EffectiveBaseDescriptorSchemaVersion))
			Expect(args).To(HaveKeyWithValue("foundation_checksum", "sha256:base"))
			Expect(args).To(HaveKeyWithValue("descriptor", record.Descriptor))
		})

		It("returns the existing immutable row when the digest already exists", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}, row}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.EffectiveBaseID).To(Equal("sha256digest"))
			Expect(poolMock.QueryRowCalledCount).To(Equal(2))
			Expect(poolMock.QueryCalls[1]).To(ContainSubstring("WHERE effective_base_id = @effective_base_id"))
			Expect(namedArgs(poolMock.QueryArgs[1])).To(HaveKeyWithValue("effective_base_id", "sha256digest"))
		})

		It("maps a missing model foreign key to a validation error", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: &pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}}}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(result).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(err).To(MatchError(ContainSubstring("effective base references an unknown model")))
		})

		It("does not return a record when row decoding fails", func() {
			badRow := errorRow{err: errors.New("scan failed")}
			poolMock.NextRows = []pgx.Row{badRow}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("record effective base")))
			Expect(err).To(MatchError(ContainSubstring("scan failed")))
		})

		It("wraps malformed persisted identifiers instead of panicking", func() {
			badRow := row
			poolMock.NextRows = []pgx.Row{effectiveBaseMalformedFoundationRow{effectiveBaseRow: badRow}}

			result, err := repository.RecordEffectiveBase(ctx, tx, record)

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("parse foundation model id")))
		})
	})

	Describe("ReadByID", func() {
		It("reads an effective base by digest", func() {
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.ReadByID(ctx, "sha256digest")

			Expect(err).NotTo(HaveOccurred())
			Expect(result.EffectiveBaseID).To(Equal("sha256digest"))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("WHERE effective_base_id = @effective_base_id"))
			Expect(namedArgs(poolMock.QueryArgs[0])).To(HaveKeyWithValue("effective_base_id", "sha256digest"))
		})
	})

	Describe("ReadLatestByFoundationModelID", func() {
		It("reads the latest effective base for a model", func() {
			poolMock.NextRows = []pgx.Row{row}

			result, err := repository.ReadLatestByFoundationModelID(ctx, modelID)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.EffectiveBaseID).To(Equal(row.EffectiveBaseID))
			Expect(poolMock.QueryCalls[0]).To(ContainSubstring("ORDER BY updated_at DESC, created_at DESC, effective_base_id DESC"))
			args := namedArgs(poolMock.QueryArgs[0])
			Expect(args).To(HaveKeyWithValue("foundation_model_id", pgtype.UUID{Bytes: modelID, Valid: true}))
		})

		It("returns the domain not-found error when no effective base exists", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: pgx.ErrNoRows}}

			result, err := repository.ReadLatestByFoundationModelID(ctx, modelID)

			Expect(result).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})

		It("wraps scan failures", func() {
			poolMock.NextRows = []pgx.Row{errorRow{err: errors.New("scan failed")}}

			result, err := repository.ReadLatestByFoundationModelID(ctx, modelID)

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("read effective base")))
			Expect(err).To(MatchError(ContainSubstring("scan failed")))
		})
	})
})

type effectiveBaseMalformedFoundationRow struct {
	effectiveBaseRow
}

func (r effectiveBaseMalformedFoundationRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.EffectiveBaseID
	*(dest[1].(*string)) = "not-a-uuid"
	*(dest[2].(*int)) = r.DescriptorSchemaVersion
	*(dest[3].(*string)) = r.FoundationChecksum
	*(dest[4].(*string)) = r.Descriptor
	*(dest[5].(*time.Time)) = r.CreatedAt
	*(dest[6].(*time.Time)) = r.UpdatedAt
	return nil
}
