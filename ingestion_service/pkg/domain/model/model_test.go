package model

import (
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion model unit test suite")
}

var _ = Describe("Upload session model", func() {
	It("carries data-file upload metadata", func() {
		uploadID := uuid.New()
		datasetID := uuid.New()
		userID := uuid.New()
		expiresAt := time.Now().UTC().Add(15 * time.Minute)

		session := UploadSession{
			UploadID:            uploadID,
			ResourceType:        UploadResourceDataFile,
			ResourceID:          datasetID,
			DatasetID:           datasetID,
			UserID:              userID,
			ClientNonce:         "browser-retry-key",
			FileName:            "movies.parquet",
			DeclaredFormat:      "PARQUET",
			DeclaredContentType: "application/octet-stream",
			DeclaredSizeBytes:   1024,
			Status:              UploadSessionPending,
			ExpiresAt:           expiresAt,
		}

		Expect(session.UploadID).To(Equal(uploadID))
		Expect(session.ResourceType).To(Equal(UploadResourceDataFile))
		Expect(session.DatasetID).To(Equal(datasetID))
		Expect(session.UserID).To(Equal(userID))
		Expect(session.Status).To(Equal(UploadSessionPending))
		Expect(session.ExpiresAt).To(Equal(expiresAt))
	})

	It("carries model artifact onboarding metadata", func() {
		resourceID := uuid.New()
		request := OnboardHuggingFaceModelRequest{
			ResourceID:       resourceID,
			UserID:           uuid.New(),
			RepoID:           "meta-llama/Llama-3.1-8B-Instruct",
			Revision:         "main",
			ModelName:        "llama",
			ModelVersion:     "1",
			BaseModel:        "meta-llama/Llama-3.1-8B-Instruct",
			ArtifactType:     "BASE_MODEL",
			ArtifactFormat:   "HF_MODEL",
			HuggingFaceToken: "hf_token",
		}

		Expect(request.ResourceID).To(Equal(resourceID))
		Expect(request.RepoID).To(Equal("meta-llama/Llama-3.1-8B-Instruct"))
		Expect(request.HuggingFaceToken).NotTo(BeEmpty())
	})

	It("distinguishes promoted and rejected session states", func() {
		Expect(UploadSessionPromoted).NotTo(Equal(UploadSessionRejected))
		Expect(UploadResourceModelArtifact).NotTo(Equal(UploadResourceDataFile))
	})
})

var _ = Describe("Data file and dataset models", func() {
	It("carries uploaded data-file metadata", func() {
		file := DataFile{
			DatasetID:         uuid.New(),
			UserID:            uuid.New(),
			ContentType:       "text/csv",
			Extension:         "csv",
			TableNamespace:    "raw",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
		}

		Expect(file.DatasetID).NotTo(Equal(uuid.Nil))
		Expect(file.UserID).NotTo(Equal(uuid.Nil))
		Expect(file.Extension).To(Equal("csv"))
	})

	It("carries registry dataset projection metadata", func() {
		dataset := Dataset{
			DatasetID:         uuid.New(),
			UserID:            uuid.New(),
			StorageLocation:   "s3://bucket/raw/movies.parquet",
			TableNamespace:    "raw",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
		}

		Expect(dataset.DatasetID).NotTo(Equal(uuid.Nil))
		Expect(dataset.UserID).NotTo(Equal(uuid.Nil))
		Expect(dataset.SchemaMetadata).To(Equal("{}"))
	})
})
