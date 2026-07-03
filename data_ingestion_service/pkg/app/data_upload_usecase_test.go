package app_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	usecase "data_ingestion_service/pkg/app"
	"data_ingestion_service/pkg/domain"
	"data_ingestion_service/pkg/domain/model"
	servicerest "data_ingestion_service/pkg/infra/network/rest"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAppUseCases(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data ingestion app unit test suite")
}

type stubBlobRepository struct {
	receivedUpload *model.DataFile
	location       string
	saveErr        error
	signErr        error
	headErr        error
	readErr        error
	promoteErr     error
	deleted        bool
	headInfo       *model.ObjectInfo
	prefix         []byte
	object         []byte
	signed         *model.PresignedUploadPost
	promotedURI    string
	signedMaxBytes int64
}

func (s *stubBlobRepository) Save(_ context.Context, upload *model.DataFile) (string, error) {
	s.receivedUpload = upload
	if s.location == "" {
		s.location = "s3://local-dev-bucket/raw/file.csv"
	}
	return s.location, s.saveErr
}

func (s *stubBlobRepository) SignUploadPost(_ context.Context, session *model.UploadSession, maxBytes int64, _ time.Duration) (*model.PresignedUploadPost, error) {
	s.signedMaxBytes = maxBytes
	if s.signed == nil {
		s.signed = &model.PresignedUploadPost{
			URL:       "local-s3://bucket",
			Fields:    map[string]string{"key": session.StagingKey},
			ExpiresAt: session.ExpiresAt,
		}
	}
	return s.signed, s.signErr
}

func (s *stubBlobRepository) HeadStagingObject(_ context.Context, _ *model.UploadSession) (*model.ObjectInfo, error) {
	if s.headInfo == nil {
		size := int64(len(s.prefix))
		if len(s.object) > 0 {
			size = int64(len(s.object))
		}
		s.headInfo = &model.ObjectInfo{Size: size, ContentType: "text/csv", Checksum: "checksum"}
	}
	return s.headInfo, s.headErr
}

func (s *stubBlobRepository) ReadStagingRange(_ context.Context, _ *model.UploadSession, offset, maxBytes int64) ([]byte, error) {
	if len(s.object) > 0 {
		if offset >= int64(len(s.object)) {
			return nil, s.readErr
		}
		end := offset + maxBytes
		if end > int64(len(s.object)) {
			end = int64(len(s.object))
		}
		return s.object[offset:end], s.readErr
	}
	return s.prefix, s.readErr
}

func (s *stubBlobRepository) PromoteStagedObject(_ context.Context, _ *model.UploadSession, _ string) (string, error) {
	if s.promotedURI == "" {
		s.promotedURI = "s3://local-dev-bucket/raw/file.csv"
	}
	return s.promotedURI, s.promoteErr
}

func (s *stubBlobRepository) DeleteStagedObject(_ context.Context, _ *model.UploadSession) error {
	s.deleted = true
	return nil
}

type stubUploadSessionRepository struct {
	created          *model.UploadSession
	completed        *model.UploadSession
	recordedUpload   *model.DataFile
	recordedLocation string
	recordedUploadID uuid.UUID
	rejected         bool
	expired          bool
	readSession      *model.UploadSession
	createErr        error
	readErr          error
	promoteErr       error
	rejectErr        error
	expireErr        error
	recordErr        error
}

func (s *stubUploadSessionRepository) CreateUploadSession(_ context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	s.created = session
	return session, s.createErr
}

func (s *stubUploadSessionRepository) ReadUploadSessionForComplete(_ context.Context, uploadID, userID uuid.UUID) (*model.UploadSession, error) {
	if s.readSession == nil {
		s.readSession = &model.UploadSession{
			UploadID:            uploadID,
			DatasetID:           uuid.New(),
			UserID:              userID,
			StagingKey:          "staging/file.csv",
			FinalKey:            "raw/file.csv",
			DeclaredFormat:      "csv",
			DeclaredContentType: "text/csv",
			DeclaredSizeBytes:   100,
			Status:              model.UploadSessionPending,
			ExpiresAt:           time.Now().Add(time.Minute),
			TableNamespace:      "features",
			TableName:           "movies",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG",
		}
	}
	return s.readSession, s.readErr
}

func (s *stubUploadSessionRepository) PromoteUploadSession(_ context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	s.completed = session
	session.Status = model.UploadSessionPromoted
	return session, s.promoteErr
}

func (s *stubUploadSessionRepository) RejectUploadSession(context.Context, uuid.UUID, uuid.UUID) error {
	s.rejected = true
	return s.rejectErr
}

func (s *stubUploadSessionRepository) ExpireUploadSession(context.Context, uuid.UUID, uuid.UUID) error {
	s.expired = true
	return s.expireErr
}

func (s *stubUploadSessionRepository) RecordUploadedFile(_ context.Context, upload *model.DataFile, location string, uploadID uuid.UUID) error {
	s.recordedUpload = upload
	s.recordedLocation = location
	s.recordedUploadID = uploadID
	return s.recordErr
}

var _ = Describe("DataUploadUseCase", func() {
	It("uploads a file through the blob repository", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo, usecase.WithUploadSessionRepository(sessions))
		upload := &model.DataFile{
			DatasetID:   uuid.New(),
			UserID:      uuid.New(),
			ContentType: "text/csv",
			Extension:   ".csv",
		}

		Expect(uc.UploadFile(context.Background(), upload)).To(Succeed())
		Expect(repo.receivedUpload).To(Equal(upload))
		Expect(sessions.recordedUpload).To(Equal(upload))
		Expect(sessions.recordedLocation).To(Equal("s3://local-dev-bucket/raw/file.csv"))
		Expect(sessions.recordedUploadID).NotTo(Equal(uuid.Nil))
	})

	It("returns repository errors", func() {
		expectedErr := errors.New("upload failed")
		repo := &stubBlobRepository{saveErr: expectedErr}
		uc := usecase.NewDataUploadUseCase(repo, usecase.WithUploadSessionRepository(&stubUploadSessionRepository{}))

		Expect(uc.UploadFile(context.Background(), &model.DataFile{})).To(MatchError(expectedErr))
	})

	It("requires upload session storage before moving multipart bytes", func() {
		repo := &stubBlobRepository{}
		uc := usecase.NewDataUploadUseCase(repo)

		err := uc.UploadFile(context.Background(), &model.DataFile{})

		Expect(err).To(MatchError(ContainSubstring("upload session repository is required")))
		Expect(repo.receivedUpload).To(BeNil())
	})

	It("initiates an upload session with deterministic staging keys", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadPolicy(1024, time.Minute, 512),
		)
		datasetID := uuid.New()
		userID := uuid.New()

		result, err := uc.InitiateUploadSession(context.Background(), model.InitiateUploadSessionRequest{
			DatasetID:           datasetID,
			UserID:              userID,
			ClientNonce:         "retry-token",
			FileName:            "../dataset.csv",
			DeclaredFormat:      "csv",
			DeclaredContentType: "text/csv",
			DeclaredSizeBytes:   100,
			TableNamespace:      "features",
			TableName:           "movies",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.URL).To(Equal("local-s3://bucket"))
		Expect(result.Fields).To(HaveKeyWithValue("key", sessions.created.StagingKey))
		Expect(repo.signedMaxBytes).To(Equal(int64(100)))
		Expect(sessions.created.UploadID).To(Equal(result.UploadID))
		Expect(sessions.created.StagingKey).To(ContainSubstring("/dataset.csv"))
		Expect(sessions.created.FinalKey).To(HavePrefix("raw/" + datasetID.String() + "/" + result.UploadID.String() + "/"))
	})

	It("requires a client nonce for idempotent upload sessions", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadPolicy(1024, time.Minute, 512),
		)

		_, err := uc.InitiateUploadSession(context.Background(), model.InitiateUploadSessionRequest{
			DatasetID:           uuid.New(),
			UserID:              uuid.New(),
			FileName:            "dataset.csv",
			DeclaredFormat:      "csv",
			DeclaredContentType: "text/csv",
			DeclaredSizeBytes:   100,
			TableNamespace:      "features",
			TableName:           "movies",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG",
		})

		Expect(err).To(MatchError(domain.ErrValidationFailed.Extend("client nonce is required")))
		Expect(sessions.created).To(BeNil())
	})

	It("promotes a valid staged upload", func() {
		repo := &stubBlobRepository{prefix: []byte("a,b\n1,2\n")}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadFileDetector(stubDetector{format: "csv"}),
			usecase.WithUploadPolicy(1024, time.Minute, 512),
		)
		uploadID := uuid.New()
		userID := uuid.New()

		session, err := uc.CompleteUploadSession(context.Background(), model.CompleteUploadSessionRequest{UploadID: uploadID, UserID: userID})

		Expect(err).NotTo(HaveOccurred())
		Expect(session.Status).To(Equal(model.UploadSessionPromoted))
		Expect(sessions.completed.StorageLocation).To(Equal("s3://local-dev-bucket/raw/file.csv"))
		Expect(sessions.completed.ActualSizeBytes).To(Equal(int64(len(repo.prefix))))
		Expect(repo.deleted).To(BeTrue())
	})

	It("promotes large parquet staged uploads by validating both head and tail ranges", func() {
		object := append([]byte("PAR1"), make([]byte, 6*1000*1000)...)
		object = append(object, []byte("PAR1")...)
		repo := &stubBlobRepository{object: object, headInfo: &model.ObjectInfo{
			Size:        int64(len(object)),
			ContentType: "application/vnd.apache.parquet",
			Checksum:    "checksum",
		}}
		sessions := &stubUploadSessionRepository{readSession: &model.UploadSession{
			UploadID:            uuid.New(),
			DatasetID:           uuid.New(),
			UserID:              uuid.New(),
			StagingKey:          "staging/file.parquet",
			FinalKey:            "raw/file.parquet",
			DeclaredFormat:      "parquet",
			DeclaredContentType: "application/vnd.apache.parquet",
			DeclaredSizeBytes:   int64(len(object)),
			Status:              model.UploadSessionPending,
			ExpiresAt:           time.Now().Add(time.Minute),
			TableNamespace:      "features",
			TableName:           "movies",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG",
		}}
		detector := servicerest.NewDetector(map[string]servicerest.FormatValidatorFunc{
			servicerest.FileTypeParquet: servicerest.IsParquet,
		})
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadFileDetector(detector),
			usecase.WithUploadPolicy(int64(len(object)), time.Minute, 5*1000*1000),
		)

		session, err := uc.CompleteUploadSession(context.Background(), model.CompleteUploadSessionRequest{UploadID: sessions.readSession.UploadID, UserID: sessions.readSession.UserID})

		Expect(err).NotTo(HaveOccurred())
		Expect(session.Status).To(Equal(model.UploadSessionPromoted))
		Expect(sessions.completed.ActualSizeBytes).To(Equal(int64(len(object))))
	})

	It("rejects a staged upload when actual bytes do not match the declared format", func() {
		repo := &stubBlobRepository{prefix: []byte("not csv")}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadFileDetector(stubDetector{format: "json"}),
			usecase.WithUploadPolicy(1024, time.Minute, 512),
		)

		_, err := uc.CompleteUploadSession(context.Background(), model.CompleteUploadSessionRequest{UploadID: uuid.New(), UserID: uuid.New()})

		Expect(err).To(MatchError(domain.ErrValidationFailed.Extend("uploaded file format does not match the declared format")))
		Expect(sessions.rejected).To(BeTrue())
		Expect(repo.deleted).To(BeTrue())
	})
})

type stubDetector struct {
	format string
}

func (s stubDetector) DetectFileFormat(_ context.Context, file io.ReadSeeker, _ int, _ []string) string {
	_, _ = file.Seek(0, 0)
	buf := make([]byte, 1)
	_, _ = file.Read(buf)
	return s.format
}

func (s stubDetector) GetContentType(string) string { return "text/csv" }
