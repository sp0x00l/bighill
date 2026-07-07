package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"ingestion_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion repository DB unit test suite")
}

var _ = Describe("Dataset DAO helpers", func() {
	It("maps dataset projections to database arguments", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		dataset := &model.Dataset{
			DatasetID:         datasetID,
			UserID:            userID,
			OrgID:             orgID,
			StorageLocation:   "s3://bucket/raw/movies.parquet",
			TableNamespace:    "raw",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
		}

		args := ToDAO(dataset)

		Expect(args["dataset_id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["org_id"]).To(Equal(pgtype.UUID{Bytes: orgID, Valid: true}))
		Expect(args["storage_location"]).To(Equal("s3://bucket/raw/movies.parquet"))
		Expect(args["schema_version"]).To(Equal(1))
	})

	It("maps dataset ids to database arguments", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		ctx := ctxutil.WithActorOrg(context.Background(), userID, orgID)

		args := IDsToDAO(ctx, datasetID, userID)

		Expect(args["dataset_id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["org_id"]).To(Equal(pgtype.UUID{Bytes: orgID, Valid: true}))
	})

	It("scans dataset projections from database rows", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()

		dataset, err := scanDataset(datasetRowStub{values: []any{
			datasetID.String(),
			userID.String(),
			orgID.String(),
			"s3://bucket/raw/movies.parquet",
			"raw",
			"movies",
			"PARQUET",
			"LOCAL",
			"TEXT_RAG_PROCESSING_PROFILE",
			1,
			"{}",
		}})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.DatasetID).To(Equal(datasetID))
		Expect(dataset.UserID).To(Equal(userID))
		Expect(dataset.OrgID).To(Equal(orgID))
		Expect(dataset.TableName).To(Equal("movies"))
	})

	It("returns scan errors for corrupt dataset rows", func() {
		_, err := scanDataset(datasetRowStub{err: pgx.ErrNoRows})

		Expect(errors.Is(err, pgx.ErrNoRows)).To(BeTrue())
	})
})

var _ = Describe("Upload session DAO helpers", func() {
	It("maps upload sessions to database arguments with defaults", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		session := &model.UploadSession{
			UploadID:            uuid.New(),
			DatasetID:           datasetID,
			UserID:              userID,
			OrgID:               orgID,
			FileName:            "movies.csv",
			DeclaredFormat:      "csv",
			DeclaredContentType: "text/csv",
		}

		args := uploadSessionDAO(session)

		Expect(args["resource_type"]).To(Equal(string(model.UploadResourceDataFile)))
		Expect(args["resource_id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["org_id"]).To(Equal(pgtype.UUID{Bytes: orgID, Valid: true}))
		Expect(args["source"]).To(Equal("UPLOAD"))
	})

	It("maps upload session ids to database arguments", func() {
		uploadID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		ctx := ctxutil.WithActorOrg(context.Background(), userID, orgID)

		args := uploadSessionIDsDAO(ctx, uploadID, userID)

		Expect(args["upload_id"]).To(Equal(pgtype.UUID{Bytes: uploadID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["org_id"]).To(Equal(pgtype.UUID{Bytes: orgID, Valid: true}))
	})

	It("scans upload sessions from database rows", func() {
		uploadID := uuid.New()
		resourceID := uuid.New()
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		createdAt := time.Now().UTC()
		expiresAt := createdAt.Add(15 * time.Minute)

		session, err := scanUploadSession(uploadSessionRowStub{values: []any{
			uploadID.String(),
			string(model.UploadResourceDataFile),
			resourceID.String(),
			datasetID.String(),
			userID.String(),
			orgID.String(),
			"nonce",
			"movies.csv",
			"staging/movies.csv",
			"raw/movies.csv",
			"s3://bucket/raw/movies.csv",
			"csv",
			"text/csv",
			int64(100),
			int64(99),
			"sha256",
			string(model.UploadSessionPromoted),
			"raw",
			"movies",
			"PARQUET",
			"LOCAL",
			"TEXT_RAG_PROCESSING_PROFILE",
			"",
			"",
			"",
			"",
			"UPLOAD",
			"",
			"",
			"",
			"",
			"",
			createdAt,
			expiresAt,
		}})

		Expect(err).NotTo(HaveOccurred())
		Expect(session.UploadID).To(Equal(uploadID))
		Expect(session.ResourceID).To(Equal(resourceID))
		Expect(session.DatasetID).To(Equal(datasetID))
		Expect(session.UserID).To(Equal(userID))
		Expect(session.OrgID).To(Equal(orgID))
		Expect(session.Status).To(Equal(model.UploadSessionPromoted))
	})

	It("returns scan errors for corrupt upload session rows", func() {
		_, err := scanUploadSession(uploadSessionRowStub{err: pgx.ErrNoRows})

		Expect(errors.Is(err, pgx.ErrNoRows)).To(BeTrue())
	})
})

type datasetRowStub struct {
	values []any
	err    error
}

func (r datasetRowStub) Scan(dest ...any) error {
	return scanValues(r.values, r.err, dest...)
}

type uploadSessionRowStub struct {
	values []any
	err    error
}

func (r uploadSessionRowStub) Scan(dest ...any) error {
	return scanValues(r.values, r.err, dest...)
}

func scanValues(values []any, err error, dest ...any) error {
	if err != nil {
		return err
	}
	for i := range dest {
		switch target := dest[i].(type) {
		case *string:
			*target = values[i].(string)
		case *int:
			*target = values[i].(int)
		case *int64:
			*target = values[i].(int64)
		case *time.Time:
			*target = values[i].(time.Time)
		default:
			Fail("unexpected scan target")
		}
	}
	return nil
}
