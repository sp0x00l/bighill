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
	valid             bool
	err               error
	called            bool
}

func (s *stubDatasetUsecase) IsValidForUpload(_ context.Context, datasetID, userID uuid.UUID) (bool, error) {
	s.called = true
	s.receivedDatasetID = datasetID
	s.receivedUserID = userID
	return s.valid, s.err
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
		datasetUseCase = &stubDatasetUsecase{valid: true}
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
