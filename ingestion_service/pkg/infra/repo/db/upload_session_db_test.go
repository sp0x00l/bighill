package db

import (
	"errors"
	"testing"
	"time"

	"ingestion_service/pkg/domain/model"
	ingestionpb "lib/data_contracts_lib/ingestion"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion repository DB unit test suite")
}

var _ = Describe("Dataset DAO helpers", func() {
	It("maps dataset projections to database arguments", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		dataset := &model.Dataset{
			DatasetID:         datasetID,
			UserID:            userID,
			StorageLocation:   "s3://bucket/raw/movies.parquet",
			TableNamespace:    "raw",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
		}

		args := ToDAO(dataset)

		Expect(args["dataset_id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["storage_location"]).To(Equal("s3://bucket/raw/movies.parquet"))
		Expect(args["schema_version"]).To(Equal(1))
	})

	It("maps dataset ids to database arguments", func() {
		datasetID := uuid.New()
		userID := uuid.New()

		args := IDsToDAO(datasetID, userID)

		Expect(args["dataset_id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
	})

	It("scans dataset projections from database rows", func() {
		datasetID := uuid.New()
		userID := uuid.New()

		dataset, err := scanDataset(datasetRowStub{values: []any{
			datasetID.String(),
			userID.String(),
			"s3://bucket/raw/movies.parquet",
			"raw",
			"movies",
			"PARQUET",
			"LOCAL",
			"TEXT_RAG",
			1,
			"{}",
		}})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.DatasetID).To(Equal(datasetID))
		Expect(dataset.UserID).To(Equal(userID))
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
		session := &model.UploadSession{
			UploadID:            uuid.New(),
			DatasetID:           datasetID,
			UserID:              userID,
			FileName:            "movies.csv",
			DeclaredFormat:      "csv",
			DeclaredContentType: "text/csv",
		}

		args := uploadSessionDAO(session)

		Expect(args["resource_type"]).To(Equal(string(model.UploadResourceDataFile)))
		Expect(args["resource_id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["source"]).To(Equal("upload"))
	})

	It("maps upload session ids to database arguments", func() {
		uploadID := uuid.New()
		userID := uuid.New()

		args := uploadSessionIDsDAO(uploadID, userID)

		Expect(args["upload_id"]).To(Equal(pgtype.UUID{Bytes: uploadID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
	})

	It("scans upload sessions from database rows", func() {
		uploadID := uuid.New()
		resourceID := uuid.New()
		datasetID := uuid.New()
		userID := uuid.New()
		createdAt := time.Now().UTC()
		expiresAt := createdAt.Add(15 * time.Minute)

		session, err := scanUploadSession(uploadSessionRowStub{values: []any{
			uploadID.String(),
			string(model.UploadResourceDataFile),
			resourceID.String(),
			datasetID.String(),
			userID.String(),
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
			"TEXT_RAG",
			"",
			"",
			"",
			"",
			"upload",
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
		Expect(session.Status).To(Equal(model.UploadSessionPromoted))
	})

	It("returns scan errors for corrupt upload session rows", func() {
		_, err := scanUploadSession(uploadSessionRowStub{err: pgx.ErrNoRows})

		Expect(errors.Is(err, pgx.ErrNoRows)).To(BeTrue())
	})
})

var _ = Describe("Upload session outbox messages", func() {
	It("builds dataset-file-uploaded events with user ids", func() {
		session := &model.UploadSession{
			UploadID:            uuid.New(),
			DatasetID:           uuid.New(),
			UserID:              uuid.New(),
			StorageLocation:     "s3://bucket/raw/movies.csv",
			DeclaredContentType: "text/csv",
			DeclaredFormat:      "csv",
			TableNamespace:      "raw",
			TableName:           "movies",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG",
		}

		message := datasetFileUploadedMessage("ingestion", session)

		Expect(message.Topic).To(Equal("ingestion"))
		Expect(message.Message.MsgType).To(Equal(msgConn.MsgTypeDatasetFileUploaded))
		Expect(message.DispatchKey).To(Equal("dataset_file_uploaded:" + session.UploadID.String()))
		var event ingestionpb.DatasetFileUploadedEvent
		Expect(proto.Unmarshal(message.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(session.DatasetID.String()))
		Expect(event.UserId).To(Equal(session.UserID.String()))
		Expect(event.SourceType).To(Equal("upload"))
	})

	It("builds model-artifact-ingested events with user ids and source metadata", func() {
		createdAt := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
		session := &model.UploadSession{
			UploadID:            uuid.New(),
			ResourceID:          uuid.New(),
			DatasetID:           uuid.New(),
			UserID:              uuid.New(),
			FileName:            "model.tar",
			StorageLocation:     "s3://bucket/models/model.tar",
			ManifestLocation:    "s3://bucket/models/manifest.json",
			DeclaredFormat:      "HF_MODEL",
			DeclaredContentType: "application/x-tar",
			ActualSizeBytes:     1000,
			Checksum:            "sha256",
			ArtifactType:        "BASE_MODEL",
			ModelName:           "llama",
			ModelVersion:        "1",
			BaseModel:           "meta-llama/Llama",
			Source:              "hugging_face",
			SourceURI:           "hf://meta-llama/Llama",
			HFRepoID:            "meta-llama/Llama",
			HFRevision:          "main",
			HFCommitSHA:         "abc123",
			CreatedAt:           createdAt,
		}

		message := modelArtifactIngestedMessage("ingestion", session)

		Expect(message.Message.MsgType).To(Equal(msgConn.MsgTypeModelArtifactIngested))
		Expect(message.DispatchKey).To(Equal("model_artifact_ingested:" + session.UploadID.String()))
		var event ingestionpb.ModelArtifactIngestedEvent
		Expect(proto.Unmarshal(message.Message.Payload, &event)).To(Succeed())
		Expect(event.ArtifactId).To(Equal(session.ResourceID.String()))
		Expect(event.UserId).To(Equal(session.UserID.String()))
		Expect(event.DatasetId).To(Equal(session.DatasetID.String()))
		Expect(event.Source).To(Equal("hugging_face"))
		Expect(event.HfCommitSha).To(Equal("abc123"))
		Expect(event.CreatedAt).To(Equal(createdAt.Format(time.RFC3339)))
		Expect(event.SourceMetadata).To(ContainSubstring(session.UploadID.String()))
	})

	It("defaults empty model artifact source to upload", func() {
		Expect(sourceOrDefault("")).To(Equal("upload"))
		Expect(sourceOrDefault("hugging_face")).To(Equal("hugging_face"))
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
