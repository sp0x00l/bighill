package rest

import (
	"context"
	"data_ingestion_service/pkg/domain/model"
	rest "data_ingestion_service/pkg/infra/network/restsupport"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// Supported file formats prioritized by file extension
var (
	csvPriorityFormats     = []string{FileTypeCSV, FileTypeParquet, FileTypeJSON}
	jsonPriorityFormats    = []string{FileTypeJSON, FileTypeParquet, FileTypeCSV}
	parquetPriorityFormats = []string{FileTypeParquet, FileTypeJSON, FileTypeCSV}
)

type FileDetector interface {
	DetectFileFormat(ctx context.Context, file io.ReadSeeker, fileSize int, validFormats []string) string
	GetContentType(fileType string) string
}

type DataUploadUseCase interface {
	UploadFile(ctx context.Context, upload *model.DataFile) error
}

type DatasetUsecase interface {
	IsValidForUpload(ctx context.Context, datasetID, userID uuid.UUID) (bool, error)
}

type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (AuthResult, error)
}

type DataUploadHandlers struct {
	uploadUseCase           DataUploadUseCase
	datasetsUsecase         DatasetUsecase
	authenticator           Authenticator
	maxFileSizeBytes        int64
	MaxBytesReaderSizeBytes int64
	detector                FileDetector
	supportedFilesFormats   map[string][]string
}

func NewDataUploadHandlers(uploadUseCase DataUploadUseCase, datasetUseCase DatasetUsecase, detector FileDetector, authenticator Authenticator, maxFileSizeBytes int64) *DataUploadHandlers {
	log.Trace("rest NewDataUploadHandlers")

	return &DataUploadHandlers{
		uploadUseCase:           uploadUseCase,
		datasetsUsecase:         datasetUseCase,
		authenticator:           authenticator,
		detector:                detector,
		maxFileSizeBytes:        maxFileSizeBytes,
		MaxBytesReaderSizeBytes: maxFileSizeBytes + 1000, // add extra space for the request
		supportedFilesFormats: map[string][]string{
			FileTypeCSV:     csvPriorityFormats,
			FileTypeJSON:    jsonPriorityFormats,
			FileTypeParquet: parquetPriorityFormats,
		},
	}
}

func (h *DataUploadHandlers) GetRoutes() []rest.Route {
	routes := []rest.Route{
		{
			Path:     "/v1/data/store/{id}",
			Handler:  h.UploadDataFile,
			Method:   http.MethodPost,
			SpanName: "upload-data-file",
		},
	}

	return routes
}

// UploadDataFile uploads a data file for the given id dataset.
func (h *DataUploadHandlers) UploadDataFile(ctx context.Context, r *http.Request) (rest.APIResponse, error) {
	log.Trace("DataUploadHandlers UploadDataFile")

	vars := mux.Vars(r)
	datasetID, err := uuid.Parse(vars["id"])
	if err != nil || datasetID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Error("parse dataset ID, read request failed")
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid dataset ID")
	}
	ctx = context.WithValue(ctx, contextKey("DatasetID"), datasetID.String())

	if h.authenticator == nil {
		log.WithContext(ctx).Error("authenticator is not configured")
		return nil, rest.ErrInternalServer().WithMessage("Authentication is not configured")
	}
	authResult, err := h.authenticator.Authenticate(ctx, r)
	if err != nil {
		return nil, err
	}
	userID := authResult.UserID
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	isDatasetValid, err := h.datasetsUsecase.IsValidForUpload(ctx, datasetID, userID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to validate dataset for upload")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to validate dataset for upload")
	}
	if !isDatasetValid {
		log.WithContext(ctx).Error("no valid dataset found for upload")
		return nil, rest.ErrNotFound().WithMessage("No valid dataset found for upload")
	}

	// Set the maximum file size for the request (this includes the entire request body) to prevent abuse.
	r.Body = http.MaxBytesReader(nil, r.Body, h.MaxBytesReaderSizeBytes)

	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		if err.Error() == (&http.MaxBytesError{}).Error() {
			log.WithContext(ctx).WithError(err).Error("file size is too large")
			return nil, rest.ErrBadRequest().WithMessage("File size is too large")
		}
		log.WithContext(ctx).WithError(err).Error("failed to read form file for data set file upload")
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Failed to read form file for data set file upload")
	}
	defer file.Close()

	if fileHeader.Size > h.maxFileSizeBytes {
		log.WithContext(ctx).Error("file size is too large")
		return nil, rest.ErrBadRequest().WithMessage("File size is too large")
	}

	if fileHeader.Size == 0 {
		log.WithContext(ctx).Error("file is empty")
		return nil, rest.ErrBadRequest().WithMessage("File is empty")
	}

	supportedFileTypes := h.GetSupportedFileFormats(fileHeader.Filename)

	fileFormat := h.detector.DetectFileFormat(ctx, file, int(fileHeader.Size), supportedFileTypes)
	if fileFormat == FileTypeUnsupported {
		log.WithContext(ctx).Error("file format not supported")
		return nil, rest.ErrBadRequest().WithMessage("File format not supported")
	}

	contentType := h.detector.GetContentType(fileFormat)
	if contentType == DefaultContentType {
		log.WithContext(ctx).Error("content type not supported")
		return nil, rest.ErrBadRequest().WithMessage("Content type not supported")
	}

	upload := &model.DataFile{
		DatasetID:   datasetID,
		UserID:      userID,
		File:        file,
		Extension:   fileFormat,
		ContentType: contentType,
	}

	if err = h.uploadUseCase.UploadFile(ctx, upload); err != nil {
		log.WithContext(ctx).WithError(err).Error("upload data set file failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to upload data set file")
	}

	return rest.NewReponse(http.StatusCreated), nil
}

// GetSupportedFileFormats returns the supported files format list.
// The file content will be first validated against the given file extension if provided,
// then against the other supported file formats.
func (h *DataUploadHandlers) GetSupportedFileFormats(fileName string) []string {
	extension := strings.ToLower(strings.TrimPrefix(filepath.Ext(fileName), "."))
	if formats, found := h.supportedFilesFormats[extension]; found {
		return formats
	}
	return parquetPriorityFormats
}
