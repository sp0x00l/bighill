package rest_test

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"

	"data_ingestion_service/pkg/domain/model"
	serviceRest "data_ingestion_service/pkg/infra/network/rest"
	restSupport "data_ingestion_service/pkg/infra/network/restsupport"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubUploadUseCase struct {
	receivedUpload *model.DataFile
	err            error
}

func (s *stubUploadUseCase) UploadFile(_ context.Context, upload *model.DataFile) error {
	s.receivedUpload = upload
	return s.err
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
			ProcessingProfile: "TEXT_RAG",
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
		handler = serviceRest.NewDataUploadHandlers(uploadUseCase, datasetUseCase, detector, authenticator, 1024*1024)
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
		Expect(uploadUseCase.receivedUpload.ProcessingProfile).To(Equal("TEXT_RAG"))
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
