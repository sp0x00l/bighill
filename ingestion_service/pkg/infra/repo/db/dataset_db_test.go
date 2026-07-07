package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	coreDb "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatasetDB", func() {
	var (
		ctx     context.Context
		pool    *datasetPoolStub
		repo    *DatasetDB
		dataset *model.Dataset
	)

	BeforeEach(func() {
		dataset = validDatasetProjection()
		ctx = ctxutil.WithActorOrg(context.Background(), dataset.UserID, dataset.OrgID)
		pool = &datasetPoolStub{rowsAffected: 1}
		repo = NewDatasetDB(coreDb.NewDatabase(pool, "test_db"))
	})

	Describe("Upsert", func() {
		It("stores the dataset projection", func() {
			err := repo.Upsert(ctx, dataset)

			Expect(err).NotTo(HaveOccurred())
			Expect(pool.execCalled).To(BeTrue())
			Expect(pool.lastSQL).To(ContainSubstring("INSERT INTO test_db.datasets"))
			Expect(pool.lastSQL).To(ContainSubstring("ON CONFLICT (dataset_id) DO UPDATE"))
			args := namedDatasetArgs(pool.lastArgs)
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: dataset.DatasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: dataset.UserID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: dataset.OrgID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("table_name", dataset.TableName))
		})

		It("returns a domain validation error when the tenant projection is missing", func() {
			pool.err = &pgconn.PgError{Code: "23503"}

			err := repo.Upsert(ctx, dataset)

			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		})

		It("wraps database errors", func() {
			pool.err = errors.New("db unavailable")

			err := repo.Upsert(ctx, dataset)

			Expect(err).To(MatchError(ContainSubstring("database error. Failed to upsert dataset")))
			Expect(err).To(MatchError(ContainSubstring("db unavailable")))
		})
	})

	Describe("BlacklistDataset", func() {
		It("blacklists a dataset for the owning user", func() {
			err := repo.BlacklistDataset(ctx, dataset.DatasetID, dataset.UserID)

			Expect(err).NotTo(HaveOccurred())
			Expect(pool.execCalled).To(BeTrue())
			Expect(pool.lastSQL).To(ContainSubstring("SET blacklisted = true"))
			args := namedDatasetArgs(pool.lastArgs)
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: dataset.DatasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: dataset.UserID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: dataset.OrgID, Valid: true}))
		})

		It("returns not found when no row is updated", func() {
			pool.rowsAffected = 0

			err := repo.BlacklistDataset(ctx, dataset.DatasetID, dataset.UserID)

			Expect(errors.Is(err, domain.ErrResourceNotFound)).To(BeTrue())
		})

		It("wraps database errors", func() {
			pool.err = errors.New("update failed")

			err := repo.BlacklistDataset(ctx, dataset.DatasetID, dataset.UserID)

			Expect(err).To(MatchError(ContainSubstring("database error. Failed to set dataset as blacklisted")))
			Expect(err).To(MatchError(ContainSubstring("update failed")))
		})
	})

	Describe("DeleteDataset", func() {
		It("deletes a dataset for the owning user", func() {
			err := repo.DeleteDataset(ctx, dataset.DatasetID, dataset.UserID)

			Expect(err).NotTo(HaveOccurred())
			Expect(pool.execCalled).To(BeTrue())
			Expect(pool.lastSQL).To(ContainSubstring("DELETE FROM test_db.datasets"))
			args := namedDatasetArgs(pool.lastArgs)
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: dataset.DatasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: dataset.UserID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: dataset.OrgID, Valid: true}))
		})

		It("returns not found when no row is deleted", func() {
			pool.rowsAffected = 0

			err := repo.DeleteDataset(ctx, dataset.DatasetID, dataset.UserID)

			Expect(errors.Is(err, domain.ErrResourceNotFound)).To(BeTrue())
		})

		It("wraps database errors", func() {
			pool.err = errors.New("delete failed")

			err := repo.DeleteDataset(ctx, dataset.DatasetID, dataset.UserID)

			Expect(err).To(MatchError(ContainSubstring("database error. Failed to delete dataset")))
			Expect(err).To(MatchError(ContainSubstring("delete failed")))
		})
	})

	Describe("ReadForUpload", func() {
		It("reads an uploadable dataset projection", func() {
			pool.row = datasetRowStub{values: []any{
				dataset.DatasetID.String(),
				dataset.UserID.String(),
				dataset.OrgID.String(),
				dataset.StorageLocation,
				dataset.TableNamespace,
				dataset.TableName,
				dataset.TableFormat,
				dataset.CatalogProvider,
				dataset.ProcessingProfile,
				dataset.SchemaVersion,
				dataset.SchemaMetadata,
			}}

			record, err := repo.ReadForUpload(ctx, dataset.DatasetID, dataset.UserID)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(dataset))
			Expect(pool.queryRowCalled).To(BeTrue())
			Expect(pool.lastSQL).To(ContainSubstring("WHERE dataset_id = @dataset_id AND org_id = @org_id AND blacklisted = false"))
			args := namedDatasetArgs(pool.lastArgs)
			Expect(args).To(HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: dataset.DatasetID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("user_id", pgtype.UUID{Bytes: dataset.UserID, Valid: true}))
			Expect(args).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: dataset.OrgID, Valid: true}))
		})

		It("returns not found when the dataset is missing", func() {
			pool.row = datasetRowStub{err: pgx.ErrNoRows}

			record, err := repo.ReadForUpload(ctx, dataset.DatasetID, dataset.UserID)

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrResourceNotFound)).To(BeTrue())
		})

		It("wraps scan errors", func() {
			pool.row = datasetRowStub{err: errors.New("scan failed")}

			record, err := repo.ReadForUpload(ctx, dataset.DatasetID, dataset.UserID)

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("database error. Failed to read dataset for upload")))
			Expect(err).To(MatchError(ContainSubstring("scan failed")))
		})
	})
})

type datasetPoolStub struct {
	execCalled     bool
	queryRowCalled bool
	lastSQL        string
	lastArgs       []any
	rowsAffected   int64
	err            error
	row            pgx.Row
}

func (p *datasetPoolStub) Close() {}

func (p *datasetPoolStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.queryRowCalled = true
	p.lastSQL = compactTestSQL(sql)
	p.lastArgs = args
	if p.row != nil {
		return p.row
	}
	return datasetRowStub{err: pgx.ErrNoRows}
}

func (p *datasetPoolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (p *datasetPoolStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execCalled = true
	p.lastSQL = compactTestSQL(sql)
	p.lastArgs = args
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.rowsAffected)), p.err
}

func (p *datasetPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, nil
}

func validDatasetProjection() *model.Dataset {
	return &model.Dataset{
		DatasetID:         uuid.New(),
		UserID:            uuid.New(),
		OrgID:             uuid.New(),
		StorageLocation:   "s3://local-dev-bucket/raw/movies.parquet",
		TableNamespace:    "raw",
		TableName:         "movies",
		TableFormat:       "ICEBERG",
		CatalogProvider:   "POLARIS",
		ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
		SchemaVersion:     3,
		SchemaMetadata:    `{"fields":[{"name":"title","type":"string"}]}`,
	}
}

func namedDatasetArgs(args []any) pgx.NamedArgs {
	Expect(args).To(HaveLen(1))
	named, ok := args[0].(pgx.NamedArgs)
	Expect(ok).To(BeTrue())
	return named
}

func compactTestSQL(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}
