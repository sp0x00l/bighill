package app_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	usecase "ingestion_service/pkg/app"
	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	ingestionmessaging "ingestion_service/pkg/infra/network/messaging"
	servicerest "ingestion_service/pkg/infra/network/rest"
	ingestionpb "lib/data_contracts_lib/ingestion"
	sharedDomain "lib/shared_lib/domain"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

func TestAppUseCases(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion app unit test suite")
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
	recordedModel    *model.UploadSession
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

type stubUploadSessionUnitOfWork struct {
	messages []msgConn.OutboundMessage
	err      error
}

func uploadEventBuilder() usecase.UploadEventBuilder {
	return ingestionmessaging.NewUploadEventBuilder("ingestion")
}

func (s *stubUploadSessionUnitOfWork) Do(ctx context.Context, fn shareduow.TxFunc) error {
	if s.err != nil {
		return s.err
	}
	return fn(ctx, nil, func(msg msgConn.OutboundMessage) error {
		s.messages = append(s.messages, msg)
		return nil
	})
}

type stubModelDownloader struct {
	received model.OnboardHuggingFaceModelRequest
	result   *model.OnboardedModelArtifact
	err      error
}

type stubTenantRepository struct {
	tenant *sharedDomain.Tenant
	err    error
}

func (s *stubTenantRepository) Upsert(context.Context, *sharedDomain.Tenant) error {
	return nil
}

func (s *stubTenantRepository) Delete(context.Context, uuid.UUID) error {
	return nil
}

func (s *stubTenantRepository) Read(context.Context, uuid.UUID) (*sharedDomain.Tenant, error) {
	return s.tenant, s.err
}

type stubSecretDecryptor struct {
	ciphertext string
	token      string
	err        error
}

func (s *stubSecretDecryptor) Decrypt(_ context.Context, ciphertext string) (string, error) {
	s.ciphertext = ciphertext
	return s.token, s.err
}

func (s *stubModelDownloader) DownloadHuggingFaceModel(_ context.Context, request model.OnboardHuggingFaceModelRequest) (*model.OnboardedModelArtifact, error) {
	s.received = request
	if s.result == nil {
		s.result = &model.OnboardedModelArtifact{
			ResourceID:        request.ResourceID,
			StorageLocation:   "s3://local-dev-bucket/models/huggingface/" + request.ResourceID.String() + "/snapshot",
			ManifestLocation:  "s3://local-dev-bucket/models/huggingface/" + request.ResourceID.String() + "/manifest.json",
			ArtifactType:      "BASE_MODEL",
			ArtifactFormat:    "HF_MODEL",
			ArtifactSizeBytes: 12,
			ArtifactChecksum:  "sha256:test",
			ModelName:         request.ModelName,
			ModelVersion:      request.ModelVersion,
			BaseModel:         request.BaseModel,
			SourceURI:         "https://huggingface.co/" + request.RepoID,
			HFRepoID:          request.RepoID,
			HFRevision:        request.Revision,
			HFCommitSHA:       "abc123",
		}
	}
	return s.result, s.err
}

func (s *stubUploadSessionRepository) CreateUploadSession(_ context.Context, _ pgx.Tx, session *model.UploadSession) (*model.UploadSession, error) {
	s.created = session
	return session, s.createErr
}

func (s *stubUploadSessionRepository) ReadUploadSessionForComplete(_ context.Context, uploadID, userID uuid.UUID) (*model.UploadSession, error) {
	if s.readSession == nil {
		s.readSession = &model.UploadSession{
			UploadID:            uploadID,
			ResourceType:        model.UploadResourceDataFile,
			DatasetID:           uuid.New(),
			ResourceID:          uuid.New(),
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
			ProcessingProfile:   "TEXT_RAG_PROCESSING_PROFILE",
		}
	}
	return s.readSession, s.readErr
}

func (s *stubUploadSessionRepository) PromoteUploadSession(_ context.Context, _ pgx.Tx, session *model.UploadSession) (*model.UploadSession, bool, error) {
	s.completed = session
	session.Status = model.UploadSessionPromoted
	return session, true, s.promoteErr
}

func (s *stubUploadSessionRepository) RejectUploadSession(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error {
	s.rejected = true
	return s.rejectErr
}

func (s *stubUploadSessionRepository) ExpireUploadSession(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error {
	s.expired = true
	return s.expireErr
}

func (s *stubUploadSessionRepository) RecordUploadedFile(_ context.Context, _ pgx.Tx, upload *model.DataFile, location string, uploadID uuid.UUID) (*model.UploadSession, error) {
	s.recordedUpload = upload
	s.recordedLocation = location
	s.recordedUploadID = uploadID
	return &model.UploadSession{
		UploadID:            uploadID,
		ResourceType:        model.UploadResourceDataFile,
		ResourceID:          upload.DatasetID,
		DatasetID:           upload.DatasetID,
		UserID:              upload.UserID,
		DeclaredFormat:      upload.Extension,
		DeclaredContentType: upload.ContentType,
		StorageLocation:     location,
		Status:              model.UploadSessionPromoted,
	}, s.recordErr
}

func (s *stubUploadSessionRepository) RecordModelArtifact(_ context.Context, _ pgx.Tx, session *model.UploadSession) (*model.UploadSession, error) {
	s.recordedModel = session
	return session, s.recordErr
}

var _ = Describe("DataUploadUseCase", func() {
	It("uploads a file through the blob repository", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		unitOfWork := &stubUploadSessionUnitOfWork{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(unitOfWork, uploadEventBuilder()),
		)
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
		Expect(unitOfWork.messages).To(HaveLen(1))
		Expect(unitOfWork.messages[0].Topic).To(Equal("ingestion"))
		Expect(unitOfWork.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetFileUploaded))
		var event ingestionpb.DatasetFileUploadedEvent
		Expect(proto.Unmarshal(unitOfWork.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(upload.DatasetID.String()))
		Expect(event.UserId).To(Equal(upload.UserID.String()))
		Expect(event.SourceType).To(Equal("upload"))
	})

	It("returns repository errors", func() {
		expectedErr := errors.New("upload failed")
		repo := &stubBlobRepository{saveErr: expectedErr}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(&stubUploadSessionRepository{}),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
		)

		Expect(uc.UploadFile(context.Background(), &model.DataFile{})).To(MatchError(expectedErr))
	})

	It("initiates an upload session with deterministic staging keys", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
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
			ProcessingProfile:   "TEXT_RAG_PROCESSING_PROFILE",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.URL).To(Equal("local-s3://bucket"))
		Expect(result.Fields).To(HaveKeyWithValue("key", sessions.created.StagingKey))
		Expect(repo.signedMaxBytes).To(Equal(int64(100)))
		Expect(sessions.created.UploadID).To(Equal(result.UploadID))
		Expect(sessions.created.StagingKey).To(ContainSubstring("/dataset.csv"))
		Expect(sessions.created.FinalKey).To(HavePrefix("raw/" + datasetID.String() + "/" + result.UploadID.String() + "/"))
	})

	It("initiates a model artifact upload session with deterministic model artifact keys", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
			usecase.WithUploadPolicy(2048, time.Minute, 512),
		)
		userID := uuid.New()
		datasetID := uuid.New()

		result, err := uc.InitiateModelUploadSession(context.Background(), model.InitiateModelUploadSessionRequest{
			DatasetID:           datasetID,
			UserID:              userID,
			ClientNonce:         "model-retry-token",
			FileName:            "../adapter.safetensors",
			ArtifactType:        "lora-adapter",
			ArtifactFormat:      "safetensors",
			DeclaredContentType: "application/octet-stream",
			DeclaredSizeBytes:   1000,
			ModelName:           "movie-twin",
			ModelVersion:        "1",
			BaseModel:           "meta-llama/Llama-3.1-8B",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.URL).To(Equal("local-s3://bucket"))
		Expect(result.ResourceID).NotTo(Equal(uuid.Nil))
		Expect(sessions.created.UploadID).To(Equal(result.UploadID))
		Expect(sessions.created.ResourceType).To(Equal(model.UploadResourceModelArtifact))
		Expect(sessions.created.ResourceID).To(Equal(result.ResourceID))
		Expect(sessions.created.DatasetID).To(Equal(datasetID))
		Expect(sessions.created.ArtifactType).To(Equal("LORA_ADAPTER"))
		Expect(sessions.created.DeclaredFormat).To(Equal("safetensors"))
		Expect(sessions.created.StagingKey).To(ContainSubstring("/adapter.safetensors"))
		Expect(sessions.created.FinalKey).To(HavePrefix("models/artifacts/" + result.ResourceID.String() + "/" + result.UploadID.String() + "/"))
		Expect(repo.signedMaxBytes).To(Equal(int64(1000)))
	})

	It("promotes a valid staged upload", func() {
		repo := &stubBlobRepository{prefix: []byte("a,b\n1,2\n")}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
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

	It("promotes a valid staged model artifact archive without data format detection", func() {
		resourceID := uuid.New()
		datasetID := uuid.New()
		object := loraArchiveObject()
		repo := &stubBlobRepository{headInfo: &model.ObjectInfo{
			Size:        int64(len(object)),
			ContentType: "application/zip",
			Checksum:    "model-checksum",
		}, object: object}
		sessions := &stubUploadSessionRepository{readSession: &model.UploadSession{
			UploadID:            uuid.New(),
			ResourceType:        model.UploadResourceModelArtifact,
			ResourceID:          resourceID,
			DatasetID:           datasetID,
			UserID:              uuid.New(),
			StagingKey:          "staging/model_artifact/" + resourceID.String() + "/adapter.zip",
			FinalKey:            "models/artifacts/" + resourceID.String() + "/adapter.zip",
			DeclaredFormat:      "zip",
			DeclaredContentType: "application/zip",
			DeclaredSizeBytes:   int64(len(object)),
			Status:              model.UploadSessionPending,
			ExpiresAt:           time.Now().Add(time.Minute),
			ArtifactType:        "LORA_ADAPTER",
			ModelName:           "movie-twin",
			ModelVersion:        "1",
			BaseModel:           "meta-llama/Llama-3.1-8B",
		}}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
			usecase.WithUploadFileDetector(stubDetector{format: "unsupported"}),
			usecase.WithUploadPolicy(8192, time.Minute, 512),
		)

		session, err := uc.CompleteUploadSession(context.Background(), model.CompleteUploadSessionRequest{UploadID: sessions.readSession.UploadID, UserID: sessions.readSession.UserID})

		Expect(err).NotTo(HaveOccurred())
		Expect(session.Status).To(Equal(model.UploadSessionPromoted))
		Expect(sessions.completed.ResourceType).To(Equal(model.UploadResourceModelArtifact))
		Expect(sessions.completed.ResourceID).To(Equal(resourceID))
		Expect(sessions.completed.Checksum).To(Equal(sha256String(object)))
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
			ResourceType:        model.UploadResourceDataFile,
			DatasetID:           uuid.New(),
			ResourceID:          uuid.New(),
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
			ProcessingProfile:   "TEXT_RAG_PROCESSING_PROFILE",
		}}
		detector := servicerest.NewDetector(map[string]servicerest.FormatValidatorFunc{
			servicerest.FileTypeParquet: servicerest.IsParquet,
		})
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
			usecase.WithUploadFileDetector(detector),
			usecase.WithUploadPolicy(int64(len(object)), time.Minute, 5*1000*1000),
		)

		session, err := uc.CompleteUploadSession(context.Background(), model.CompleteUploadSessionRequest{UploadID: sessions.readSession.UploadID, UserID: sessions.readSession.UserID})

		Expect(err).NotTo(HaveOccurred())
		Expect(session.Status).To(Equal(model.UploadSessionPromoted))
		Expect(sessions.completed.ActualSizeBytes).To(Equal(int64(len(object))))
	})

	It("records a Hugging Face model artifact through the promoted model path", func() {
		repo := &stubBlobRepository{}
		sessions := &stubUploadSessionRepository{}
		unitOfWork := &stubUploadSessionUnitOfWork{}
		downloader := &stubModelDownloader{}
		tenants := &stubTenantRepository{tenant: &sharedDomain.Tenant{HuggingFaceTokenCiphertext: "ciphertext-1"}}
		decryptor := &stubSecretDecryptor{token: "hf-token"}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(unitOfWork, uploadEventBuilder()),
			usecase.WithUploadTenantsRepository(tenants),
			usecase.WithHuggingFaceTokenDecryptor(decryptor),
			usecase.WithModelArtifactDownloader(downloader),
		)
		userID := uuid.New()

		session, err := uc.OnboardHuggingFaceModel(context.Background(), model.OnboardHuggingFaceModelRequest{
			UserID:       userID,
			ClientNonce:  "hf-1",
			RepoID:       "meta-llama/Llama-3.1-8B",
			Revision:     "main",
			ModelName:    "llama",
			ModelVersion: "1",
			BaseModel:    "meta-llama/Llama-3.1-8B",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(session.Source).To(Equal("HUGGING_FACE"))
		Expect(session.ArtifactType).To(Equal("BASE_MODEL"))
		Expect(session.ManifestLocation).To(ContainSubstring("/manifest.json"))
		Expect(sessions.recordedModel).To(Equal(session))
		Expect(downloader.received.UserID).To(Equal(userID))
		Expect(downloader.received.HuggingFaceToken).To(Equal("hf-token"))
		Expect(decryptor.ciphertext).To(Equal("ciphertext-1"))
		Expect(unitOfWork.messages).To(HaveLen(1))
		Expect(unitOfWork.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeModelArtifactIngested))
		var event ingestionpb.ModelArtifactIngestedEvent
		Expect(proto.Unmarshal(unitOfWork.messages[0].Message.Payload, &event)).To(Succeed())
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.Source).To(Equal("HUGGING_FACE"))
		Expect(event.HfCommitSha).To(Equal("abc123"))
		Expect(event.SourceMetadata).To(ContainSubstring(session.UploadID.String()))
	})

	It("rejects a staged upload when actual bytes do not match the declared format", func() {
		repo := &stubBlobRepository{prefix: []byte("not csv")}
		sessions := &stubUploadSessionRepository{}
		uc := usecase.NewDataUploadUseCase(repo,
			usecase.WithUploadSessionRepository(sessions),
			usecase.WithUploadSessionUnitOfWork(&stubUploadSessionUnitOfWork{}, uploadEventBuilder()),
			usecase.WithUploadFileDetector(stubDetector{format: "json"}),
			usecase.WithUploadPolicy(1024, time.Minute, 512),
		)

		_, err := uc.CompleteUploadSession(context.Background(), model.CompleteUploadSessionRequest{UploadID: uuid.New(), UserID: uuid.New()})

		Expect(err).To(MatchError(domain.ErrValidationFailed.Extend("uploaded file format does not match the declared format")))
		Expect(sessions.rejected).To(BeTrue())
		Expect(repo.deleted).To(BeTrue())
	})
})

func safetensorsObject() []byte {
	header := []byte(`{"weight":{"dtype":"F32","shape":[1],"data_offsets":[0,4]}}`)
	payload := make([]byte, 8+len(header)+4)
	binary.LittleEndian.PutUint64(payload[:8], uint64(len(header)))
	copy(payload[8:], header)
	copy(payload[8+len(header):], []byte{0, 0, 0, 0})
	return payload
}

func loraArchiveObject() []byte {
	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)
	addZipFile(writer, "adapter_config.json", []byte(`{"peft_type":"LORA"}`))
	addZipFile(writer, "adapter_model.safetensors", safetensorsObject())
	Expect(writer.Close()).To(Succeed())
	return buf.Bytes()
}

func addZipFile(writer *zip.Writer, name string, payload []byte) {
	file, err := writer.Create(name)
	Expect(err).NotTo(HaveOccurred())
	_, err = file.Write(payload)
	Expect(err).NotTo(HaveOccurred())
}

func sha256String(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

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
