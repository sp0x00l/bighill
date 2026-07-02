package rest

import (
	"context"
	"data_ingestion_service/pkg/domain"
	"data_ingestion_service/pkg/domain/model"
	rest "data_ingestion_service/pkg/infra/network/restsupport"
	"errors"
	"fmt"
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
	csvPriorityFormats      = []string{FileTypeCSV, FileTypeParquet, FileTypeJSON, FileTypePDF, FileTypeHTML, FileTypeMarkdown, FileTypeText}
	jsonPriorityFormats     = []string{FileTypeJSON, FileTypeParquet, FileTypePDF, FileTypeHTML, FileTypeCSV, FileTypeMarkdown, FileTypeText}
	parquetPriorityFormats  = []string{FileTypeParquet, FileTypeJSON, FileTypePDF, FileTypeHTML, FileTypeCSV, FileTypeMarkdown, FileTypeText}
	pdfPriorityFormats      = []string{FileTypePDF, FileTypeParquet, FileTypeJSON, FileTypeHTML, FileTypeCSV, FileTypeMarkdown, FileTypeText}
	htmlPriorityFormats     = []string{FileTypeHTML, FileTypePDF, FileTypeParquet, FileTypeJSON, FileTypeCSV, FileTypeMarkdown, FileTypeText}
	markdownPriorityFormats = []string{FileTypeMarkdown, FileTypeHTML, FileTypePDF, FileTypeParquet, FileTypeJSON, FileTypeCSV, FileTypeText}
	textPriorityFormats     = []string{FileTypeText, FileTypeHTML, FileTypePDF, FileTypeParquet, FileTypeJSON, FileTypeCSV}
	defaultPriorityFormats  = []string{FileTypeParquet, FileTypeJSON, FileTypePDF, FileTypeHTML, FileTypeCSV, FileTypeMarkdown, FileTypeText}
)

type FileDetector interface {
	DetectFileFormat(ctx context.Context, file io.ReadSeeker, fileSize int, validFormats []string) string
	GetContentType(fileType string) string
}

type DataUploadUseCase interface {
	UploadFile(ctx context.Context, upload *model.DataFile) error
}

type DatasetUsecase interface {
	DatasetForUpload(ctx context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error)
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
			FileTypeCSV:      csvPriorityFormats,
			FileTypeJSON:     jsonPriorityFormats,
			FileTypeParquet:  parquetPriorityFormats,
			FileTypePDF:      pdfPriorityFormats,
			FileTypeHTML:     htmlPriorityFormats,
			"htm":            htmlPriorityFormats,
			FileTypeMarkdown: markdownPriorityFormats,
			"md":             markdownPriorityFormats,
			FileTypeText:     textPriorityFormats,
			"txt":            textPriorityFormats,
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

	dataset, err := h.datasetsUsecase.DatasetForUpload(ctx, datasetID, userID)
	if err != nil {
		if errors.Is(err, domain.ErrResourceNotFound) {
			log.WithContext(ctx).Error("no valid dataset found for upload")
			return nil, rest.ErrNotFound().WithMessage("No valid dataset found for upload")
		}
		log.WithContext(ctx).WithError(err).Error("failed to validate dataset for upload")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to validate dataset for upload")
	}
	if err := requireDatasetUploadMetadata(dataset); err != nil {
		log.WithContext(ctx).WithError(err).Error("dataset materialization metadata is incomplete")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Dataset materialization metadata is incomplete")
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
		DatasetID:         datasetID,
		UserID:            userID,
		File:              file,
		Extension:         fileFormat,
		ContentType:       contentType,
		TableNamespace:    dataset.TableNamespace,
		TableName:         dataset.TableName,
		TableFormat:       dataset.TableFormat,
		CatalogProvider:   dataset.CatalogProvider,
		ProcessingProfile: dataset.ProcessingProfile,
	}

	if err = h.uploadUseCase.UploadFile(ctx, upload); err != nil {
		log.WithContext(ctx).WithError(err).Error("upload data set file failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to upload data set file")
	}

	return rest.NewReponse(http.StatusCreated), nil
}

func requireDatasetUploadMetadata(dataset *model.Dataset) error {
	log.Trace("requireDatasetUploadMetadata")

	if dataset == nil {
		return fmt.Errorf("dataset is required")
	}
	if strings.TrimSpace(dataset.TableNamespace) == "" {
		return fmt.Errorf("table namespace is required")
	}
	if strings.TrimSpace(dataset.TableName) == "" {
		return fmt.Errorf("table name is required")
	}
	if strings.TrimSpace(dataset.TableFormat) == "" {
		return fmt.Errorf("table format is required")
	}
	if strings.TrimSpace(dataset.CatalogProvider) == "" {
		return fmt.Errorf("catalog provider is required")
	}
	if strings.TrimSpace(dataset.ProcessingProfile) == "" {
		return fmt.Errorf("processing profile is required")
	}
	return nil
}

// GetSupportedFileFormats returns the supported files format list.
// The file content will be first validated against the given file extension if provided,
// then against the other supported file formats.
func (h *DataUploadHandlers) GetSupportedFileFormats(fileName string) []string {
	extension := strings.ToLower(strings.TrimPrefix(filepath.Ext(fileName), "."))
	if formats, found := h.supportedFilesFormats[extension]; found {
		return formats
	}
	return defaultPriorityFormats
}
