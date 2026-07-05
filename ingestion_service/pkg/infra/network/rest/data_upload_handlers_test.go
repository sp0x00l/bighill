package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"

	"ingestion_service/pkg/domain/model"
	"ingestion_service/pkg/infra/network/adapter"
	serviceRest "ingestion_service/pkg/infra/network/rest"
	restSupport "ingestion_service/pkg/infra/network/restsupport"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubUploadUseCase struct {
	receivedUpload          *model.DataFile
	receivedInitiateRequest model.InitiateUploadSessionRequest
	receivedModelRequest    model.InitiateModelUploadSessionRequest
	receivedCompleteRequest model.CompleteUploadSessionRequest
	receivedOnboardRequest  model.OnboardHuggingFaceModelRequest
	initiateResult          *model.InitiatedUploadSession
	modelInitiateResult     *model.InitiatedUploadSession
	completeResult          *model.UploadSession
	onboardResult           *model.UploadSession
	err                     error
}

func (s *stubUploadUseCase) UploadFile(_ context.Context, upload *model.DataFile) error {
	s.receivedUpload = upload
	return s.err
}

func (s *stubUploadUseCase) InitiateUploadSession(_ context.Context, request model.InitiateUploadSessionRequest) (*model.InitiatedUploadSession, error) {
	s.receivedInitiateRequest = request
	if s.initiateResult == nil {
		s.initiateResult = &model.InitiatedUploadSession{
			UploadID: request.DatasetID,
			URL:      "local-s3://local-dev-bucket",
			Fields:   map[string]string{"key": "staging/file.csv"},
		}
	}
	return s.initiateResult, s.err
}

func (s *stubUploadUseCase) InitiateModelUploadSession(_ context.Context, request model.InitiateModelUploadSessionRequest) (*model.InitiatedUploadSession, error) {
	s.receivedModelRequest = request
	if s.modelInitiateResult == nil {
		s.modelInitiateResult = &model.InitiatedUploadSession{
			UploadID:   uuid.New(),
			ResourceID: request.ResourceID,
			URL:        "local-s3://local-dev-bucket",
			Fields:     map[string]string{"key": "staging/model_artifact/file.safetensors"},
		}
	}
	return s.modelInitiateResult, s.err
}

func (s *stubUploadUseCase) CompleteUploadSession(_ context.Context, request model.CompleteUploadSessionRequest) (*model.UploadSession, error) {
	s.receivedCompleteRequest = request
	if s.completeResult == nil {
		s.completeResult = &model.UploadSession{
			UploadID:        request.UploadID,
			DatasetID:       uuid.New(),
			UserID:          request.UserID,
			StorageLocation: "s3://local-dev-bucket/raw/file.csv",
			Status:          model.UploadSessionPromoted,
			Checksum:        "checksum",
			ActualSizeBytes: 12,
		}
	}
	return s.completeResult, s.err
}

func (s *stubUploadUseCase) CompleteModelUploadSession(_ context.Context, request model.CompleteUploadSessionRequest) (*model.UploadSession, error) {
	return s.CompleteUploadSession(context.Background(), request)
}

func (s *stubUploadUseCase) OnboardHuggingFaceModel(_ context.Context, request model.OnboardHuggingFaceModelRequest) (*model.UploadSession, error) {
	s.receivedOnboardRequest = request
	if s.onboardResult == nil {
		s.onboardResult = &model.UploadSession{
			UploadID:        uuid.New(),
			ResourceType:    model.UploadResourceModelArtifact,
			ResourceID:      request.ResourceID,
			UserID:          request.UserID,
			StorageLocation: "s3://local-dev-bucket/models/huggingface/" + request.ResourceID.String() + "/snapshot",
			Status:          model.UploadSessionPromoted,
			Checksum:        "sha256:test",
			ActualSizeBytes: 12,
			ArtifactType:    "BASE_MODEL",
			DeclaredFormat:  "HF_MODEL",
			ModelName:       request.ModelName,
			ModelVersion:    request.ModelVersion,
			BaseModel:       request.BaseModel,
		}
	}
	return s.onboardResult, s.err
}

type stubDatasetUsecase struct {
	receivedDatasetID uuid.UUID
	receivedUserID    uuid.UUID
	dataset           *model.Dataset
	err               error
	called            bool
}

func (s *stubDatasetUsecase) DatasetForUpload(_ context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error) {
	s.called = true
	s.receivedDatasetID = datasetID
	s.receivedUserID = userID
	return s.dataset, s.err
}

type stubFileDetector struct {
	fileType     string
	contentType  string
	validFormats []string
}

func (s *stubFileDetector) DetectFileFormat(_ context.Context, _ io.ReadSeeker, _ int, validFormats []string) string {
	s.validFormats = validFormats
	return s.fileType
}

func (s *stubFileDetector) GetContentType(_ string) string {
	return s.contentType
}

type stubAuthenticator struct {
	result serviceRest.AuthResult
	err    error
	called bool
}

func (s *stubAuthenticator) Authenticate(_ context.Context, _ *http.Request) (serviceRest.AuthResult, error) {
	s.called = true
	return s.result, s.err
}

var _ = Describe("DataUploadHandlers", func() {
	var (
		datasetID      uuid.UUID
		userID         uuid.UUID
		uploadUseCase  *stubUploadUseCase
		datasetUseCase *stubDatasetUsecase
		detector       *stubFileDetector
		authenticator  *stubAuthenticator
		handler        *serviceRest.DataUploadHandlers
	)

	BeforeEach(func() {
		datasetID = uuid.New()
		userID = uuid.New()
		uploadUseCase = &stubUploadUseCase{}
		datasetUseCase = &stubDatasetUsecase{dataset: &model.Dataset{
			DatasetID:         datasetID,
			UserID:            userID,
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
			SchemaVersion:     1,
			SchemaMetadata:    "{}",
		}}
		detector = &stubFileDetector{
			fileType:    serviceRest.FileTypeCSV,
			contentType: "text/csv",
		}
		authenticator = &stubAuthenticator{
			result: serviceRest.AuthResult{UserID: userID, ExpUnix: 200},
		}
		handler = serviceRest.NewDataUploadHandlers(uploadUseCase, datasetUseCase, adapter.NewUploadDTOAdapter(serializers.NewJSONSerializer()), detector, authenticator, 1024*1024)
	})

	It("uses the authenticated user id when validating the dataset", func() {
		req := newUploadRequest(datasetID, "dataset.csv", []byte("a,b\n1,2\n"))

		response, err := handler.UploadDataFile(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(authenticator.called).To(BeTrue())
		Expect(datasetUseCase.receivedDatasetID).To(Equal(datasetID))
		Expect(datasetUseCase.receivedUserID).To(Equal(userID))
		Expect(uploadUseCase.receivedUpload).NotTo(BeNil())
		Expect(uploadUseCase.receivedUpload.DatasetID).To(Equal(datasetID))
		Expect(uploadUseCase.receivedUpload.UserID).To(Equal(userID))
		Expect(uploadUseCase.receivedUpload.Extension).To(Equal(serviceRest.FileTypeCSV))
		Expect(uploadUseCase.receivedUpload.ContentType).To(Equal("text/csv"))
		Expect(uploadUseCase.receivedUpload.TableNamespace).To(Equal("features"))
		Expect(uploadUseCase.receivedUpload.TableName).To(Equal("movies"))
		Expect(uploadUseCase.receivedUpload.TableFormat).To(Equal("PARQUET"))
		Expect(uploadUseCase.receivedUpload.CatalogProvider).To(Equal("LOCAL"))
		Expect(uploadUseCase.receivedUpload.ProcessingProfile).To(Equal("TEXT_RAG_PROCESSING_PROFILE"))
	})

	It("initiates a presigned upload session after validating ownership", func() {
		req := newInitiateRequest(datasetID, []byte(`{"file_name":"dataset.csv","declared_format":"csv","content_type":"text/csv","declared_size_bytes":512,"client_nonce":"retry-1"}`))

		response, err := handler.InitiateUploadSession(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(authenticator.called).To(BeTrue())
		Expect(datasetUseCase.receivedDatasetID).To(Equal(datasetID))
		Expect(uploadUseCase.receivedInitiateRequest.DatasetID).To(Equal(datasetID))
		Expect(uploadUseCase.receivedInitiateRequest.UserID).To(Equal(userID))
		Expect(uploadUseCase.receivedInitiateRequest.DeclaredFormat).To(Equal("csv"))
		Expect(uploadUseCase.receivedInitiateRequest.DeclaredContentType).To(Equal("text/csv"))
		Expect(uploadUseCase.receivedInitiateRequest.ClientNonce).To(Equal("retry-1"))
		var body map[string]any
		Expect(json.Unmarshal(response.Payload(), &body)).To(Succeed())
		Expect(body).To(HaveKeyWithValue("url", "local-s3://local-dev-bucket"))
	})

	It("completes an upload session for the authenticated user", func() {
		uploadID := uuid.New()
		req := httptest.NewRequest(http.MethodPost, "/v1/data/uploads/"+uploadID.String()+"/complete", nil)
		req = mux.SetURLVars(req, map[string]string{"id": uploadID.String()})

		response, err := handler.CompleteUploadSession(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uploadUseCase.receivedCompleteRequest.UploadID).To(Equal(uploadID))
		Expect(uploadUseCase.receivedCompleteRequest.UserID).To(Equal(userID))
		var body map[string]any
		Expect(json.Unmarshal(response.Payload(), &body)).To(Succeed())
		Expect(body).To(HaveKeyWithValue("storage_location", "s3://local-dev-bucket/raw/file.csv"))
		Expect(body).To(HaveKeyWithValue("status", string(model.UploadSessionPromoted)))
	})

	It("initiates a model artifact upload session for the authenticated user", func() {
		resourceID := uuid.New()
		uploadID := uuid.New()
		uploadUseCase.modelInitiateResult = &model.InitiatedUploadSession{
			UploadID:   uploadID,
			ResourceID: resourceID,
			URL:        "local-s3://local-dev-bucket",
			Fields:     map[string]string{"key": "staging/model_artifact/file.safetensors"},
		}
		datasetID := uuid.New()
		req := newModelInitiateRequest([]byte(`{"resource_id":"` + resourceID.String() + `","dataset_id":"` + datasetID.String() + `","file_name":"adapter.safetensors","artifact_type":"lora-adapter","artifact_format":"safetensors","content_type":"application/octet-stream","declared_size_bytes":512,"client_nonce":"model-retry-1","model_name":"movie-twin","model_version":"1","base_model":"meta-llama/Llama-3.1-8B"}`))

		response, err := handler.InitiateModelUploadSession(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(authenticator.called).To(BeTrue())
		Expect(datasetUseCase.called).To(BeFalse())
		Expect(uploadUseCase.receivedModelRequest.ResourceID).To(Equal(resourceID))
		Expect(uploadUseCase.receivedModelRequest.DatasetID).To(Equal(datasetID))
		Expect(uploadUseCase.receivedModelRequest.UserID).To(Equal(userID))
		Expect(uploadUseCase.receivedModelRequest.ArtifactType).To(Equal("LORA_ADAPTER"))
		Expect(uploadUseCase.receivedModelRequest.ArtifactFormat).To(Equal("SAFETENSORS"))
		Expect(uploadUseCase.receivedModelRequest.ClientNonce).To(Equal("model-retry-1"))
		var body map[string]any
		Expect(json.Unmarshal(response.Payload(), &body)).To(Succeed())
		Expect(body).To(HaveKeyWithValue("upload_id", uploadID.String()))
		Expect(body).To(HaveKeyWithValue("resource_id", resourceID.String()))
		Expect(body).To(HaveKeyWithValue("url", "local-s3://local-dev-bucket"))
	})

	It("completes a model artifact upload session for the authenticated user", func() {
		uploadID := uuid.New()
		resourceID := uuid.New()
		uploadUseCase.completeResult = &model.UploadSession{
			UploadID:        uploadID,
			ResourceType:    model.UploadResourceModelArtifact,
			ResourceID:      resourceID,
			UserID:          userID,
			StorageLocation: "s3://local-dev-bucket/models/artifacts/adapter.safetensors",
			Status:          model.UploadSessionPromoted,
			Checksum:        "checksum",
			ActualSizeBytes: 512,
			ArtifactType:    "LORA_ADAPTER",
			DeclaredFormat:  "safetensors",
			ModelName:       "movie-twin",
			ModelVersion:    "1",
			BaseModel:       "meta-llama/Llama-3.1-8B",
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/models/uploads/"+uploadID.String()+"/complete", nil)
		req = mux.SetURLVars(req, map[string]string{"id": uploadID.String()})

		response, err := handler.CompleteModelUploadSession(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uploadUseCase.receivedCompleteRequest.UploadID).To(Equal(uploadID))
		Expect(uploadUseCase.receivedCompleteRequest.UserID).To(Equal(userID))
		var body map[string]any
		Expect(json.Unmarshal(response.Payload(), &body)).To(Succeed())
		Expect(body).To(HaveKeyWithValue("resource_id", resourceID.String()))
		Expect(body).To(HaveKeyWithValue("storage_location", "s3://local-dev-bucket/models/artifacts/adapter.safetensors"))
		Expect(body).To(HaveKeyWithValue("artifact_type", "LORA_ADAPTER"))
		Expect(body).To(HaveKeyWithValue("artifact_format", "safetensors"))
	})

	It("onboards a Hugging Face model for the authenticated user", func() {
		resourceID := uuid.New()
		uploadID := uuid.New()
		uploadUseCase.onboardResult = &model.UploadSession{
			UploadID:         uploadID,
			ResourceType:     model.UploadResourceModelArtifact,
			ResourceID:       resourceID,
			UserID:           userID,
			StorageLocation:  "s3://local-dev-bucket/models/huggingface/" + resourceID.String() + "/snapshot",
			ManifestLocation: "s3://local-dev-bucket/models/huggingface/" + resourceID.String() + "/manifest.json",
			Status:           model.UploadSessionPromoted,
			Checksum:         "sha256:test",
			ActualSizeBytes:  12,
			ArtifactType:     "BASE_MODEL",
			DeclaredFormat:   "HF_MODEL",
			ModelName:        "llama",
			ModelVersion:     "1",
			BaseModel:        "meta-llama/Llama-3.1-8B",
			HFRepoID:         "meta-llama/Llama-3.1-8B",
			HFRevision:       "main",
			HFCommitSHA:      "abc123",
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/models/onboard/huggingface", bytes.NewReader([]byte(`{"resource_id":"`+resourceID.String()+`","repo_id":"meta-llama/Llama-3.1-8B","revision":"main","client_nonce":"hf-1","model_name":"llama","model_version":"1","base_model":"meta-llama/Llama-3.1-8B"}`)))

		response, err := handler.OnboardHuggingFaceModel(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(uploadUseCase.receivedOnboardRequest.ResourceID).To(Equal(resourceID))
		Expect(uploadUseCase.receivedOnboardRequest.UserID).To(Equal(userID))
		Expect(uploadUseCase.receivedOnboardRequest.RepoID).To(Equal("meta-llama/Llama-3.1-8B"))
		var body map[string]any
		Expect(json.Unmarshal(response.Payload(), &body)).To(Succeed())
		Expect(body).To(HaveKeyWithValue("resource_id", resourceID.String()))
		Expect(body).To(HaveKeyWithValue("artifact_type", "BASE_MODEL"))
	})

	It("accepts PDF uploads at the REST boundary", func() {
		detector.fileType = serviceRest.FileTypePDF
		detector.contentType = "application/pdf"
		req := newUploadRequest(datasetID, "dataset.pdf", []byte("%PDF-1.7\n1 0 obj\n<<>>\nendobj\n"))

		response, err := handler.UploadDataFile(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(detector.validFormats).To(HaveLen(7))
		Expect(detector.validFormats[0]).To(Equal(serviceRest.FileTypePDF))
		Expect(uploadUseCase.receivedUpload).NotTo(BeNil())
		Expect(uploadUseCase.receivedUpload.Extension).To(Equal(serviceRest.FileTypePDF))
		Expect(uploadUseCase.receivedUpload.ContentType).To(Equal("application/pdf"))
	})

	It("accepts HTML uploads at the REST boundary", func() {
		detector.fileType = serviceRest.FileTypeHTML
		detector.contentType = "text/html"
		req := newUploadRequest(datasetID, "dataset.html", []byte("<html><body><p>hello</p></body></html>"))

		response, err := handler.UploadDataFile(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(detector.validFormats).To(HaveLen(7))
		Expect(detector.validFormats[0]).To(Equal(serviceRest.FileTypeHTML))
		Expect(uploadUseCase.receivedUpload).NotTo(BeNil())
		Expect(uploadUseCase.receivedUpload.Extension).To(Equal(serviceRest.FileTypeHTML))
		Expect(uploadUseCase.receivedUpload.ContentType).To(Equal("text/html"))
	})

	It("accepts plain text uploads at the REST boundary", func() {
		detector.fileType = serviceRest.FileTypeText
		detector.contentType = "text/plain"
		req := newUploadRequest(datasetID, "dataset.txt", []byte("plain text"))

		response, err := handler.UploadDataFile(context.Background(), req)

		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode()).To(Equal(http.StatusCreated))
		Expect(detector.validFormats[0]).To(Equal(serviceRest.FileTypeText))
		Expect(uploadUseCase.receivedUpload).NotTo(BeNil())
		Expect(uploadUseCase.receivedUpload.Extension).To(Equal(serviceRest.FileTypeText))
		Expect(uploadUseCase.receivedUpload.ContentType).To(Equal("text/plain"))
	})

	It("checks PDF before CSV when the upload has no known extension", func() {
		formats := handler.GetSupportedFileFormats("dataset")

		Expect(formats).To(HaveLen(7))
		Expect(formats[0]).To(Equal(serviceRest.FileTypeParquet))
		Expect(formats[1]).To(Equal(serviceRest.FileTypeJSON))
		Expect(formats[2]).To(Equal(serviceRest.FileTypePDF))
		Expect(formats[3]).To(Equal(serviceRest.FileTypeHTML))
		Expect(formats[4]).To(Equal(serviceRest.FileTypeCSV))
		Expect(formats[5]).To(Equal(serviceRest.FileTypeMarkdown))
		Expect(formats[6]).To(Equal(serviceRest.FileTypeText))
	})

	It("returns auth errors before dataset validation", func() {
		authenticator.err = restSupport.ErrUnauthorized().WithMessage("missing authorization header")
		req := newUploadRequest(datasetID, "dataset.csv", []byte("a,b\n1,2\n"))

		response, err := handler.UploadDataFile(context.Background(), req)

		Expect(response).To(BeNil())
		Expect(err).To(MatchError("missing authorization header"))
		Expect(authenticator.called).To(BeTrue())
		Expect(datasetUseCase.called).To(BeFalse())
		Expect(uploadUseCase.receivedUpload).To(BeNil())
	})
})

func newUploadRequest(datasetID uuid.UUID, filename string, content []byte) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filename)
	Expect(err).NotTo(HaveOccurred())
	_, err = part.Write(content)
	Expect(err).NotTo(HaveOccurred())
	Expect(writer.Close()).To(Succeed())

	req := httptest.NewRequest(http.MethodPost, "/v1/data/store/"+datasetID.String(), body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return mux.SetURLVars(req, map[string]string{"id": datasetID.String()})
}

func newInitiateRequest(datasetID uuid.UUID, content []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/data/uploads/"+datasetID.String(), bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/json")
	return mux.SetURLVars(req, map[string]string{"id": datasetID.String()})
}

func newModelInitiateRequest(content []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/models/uploads", bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/json")
	return req
}
