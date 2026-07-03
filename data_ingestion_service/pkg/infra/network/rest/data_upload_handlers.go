package rest

import (
	"context"
	"data_ingestion_service/pkg/domain"
	"data_ingestion_service/pkg/domain/model"
	rest "data_ingestion_service/pkg/infra/network/restsupport"
	"encoding/json"
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
	InitiateUploadSession(ctx context.Context, request model.InitiateUploadSessionRequest) (*model.InitiatedUploadSession, error)
	CompleteUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest) (*model.UploadSession, error)
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
			Path:     "/v1/data/uploads/{id}",
			Handler:  h.InitiateUploadSession,
			Method:   http.MethodPost,
			SpanName: "initiate-data-upload-session",
		},
		{
			Path:     "/v1/data/uploads/{id}/complete",
			Handler:  h.CompleteUploadSession,
			Method:   http.MethodPost,
			SpanName: "complete-data-upload-session",
		},
		{
			Path:     "/v1/data/store/{id}",
			Handler:  h.UploadDataFile,
			Method:   http.MethodPost,
			SpanName: "upload-data-file",
		},
	}

	return routes
}

type initiateUploadRequest struct {
	FileName          string `json:"file_name"`
	DeclaredFormat    string `json:"declared_format"`
	ContentType       string `json:"content_type"`
	DeclaredSizeBytes int64  `json:"declared_size_bytes"`
	ClientNonce       string `json:"client_nonce"`
}

type initiateUploadResponse struct {
	UploadID  string            `json:"upload_id"`
	URL       string            `json:"url"`
	Fields    map[string]string `json:"fields"`
	ExpiresAt string            `json:"expires_at"`
}

type completeUploadResponse struct {
	UploadID        string `json:"upload_id"`
	DatasetID       string `json:"dataset_id"`
	StorageLocation string `json:"storage_location"`
	Status          string `json:"status"`
	Checksum        string `json:"checksum"`
	ActualSizeBytes int64  `json:"actual_size_bytes"`
}

func (h *DataUploadHandlers) InitiateUploadSession(ctx context.Context, r *http.Request) (rest.APIResponse, error) {
	log.Trace("DataUploadHandlers InitiateUploadSession")

	datasetID, authResult, dataset, err := h.authenticateDatasetForUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	var request initiateUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid upload session request")
	}
	declaredFormat := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(request.DeclaredFormat), "."))
	if declaredFormat == "" {
		declaredFormat = strings.ToLower(strings.TrimPrefix(filepath.Ext(request.FileName), "."))
	}
	if !h.isSupportedFileFormat(declaredFormat, request.FileName) {
		return nil, rest.ErrBadRequest().WithMessage("Declared file format is not supported")
	}
	contentType := strings.TrimSpace(request.ContentType)
	if contentType == "" {
		contentType = h.detector.GetContentType(declaredFormat)
	}
	if contentType == DefaultContentType {
		return nil, rest.ErrBadRequest().WithMessage("Content type not supported")
	}
	result, err := h.uploadUseCase.InitiateUploadSession(ctx, model.InitiateUploadSessionRequest{
		DatasetID:           datasetID,
		UserID:              authResult.UserID,
		ClientNonce:         request.ClientNonce,
		FileName:            request.FileName,
		DeclaredFormat:      declaredFormat,
		DeclaredContentType: contentType,
		DeclaredSizeBytes:   request.DeclaredSizeBytes,
		TableNamespace:      dataset.TableNamespace,
		TableName:           dataset.TableName,
		TableFormat:         dataset.TableFormat,
		CatalogProvider:     dataset.CatalogProvider,
		ProcessingProfile:   dataset.ProcessingProfile,
	})
	if err != nil {
		return nil, h.uploadError(ctx, err, "Failed to initiate upload session")
	}
	return jsonResponse(http.StatusCreated, initiateUploadResponse{
		UploadID:  result.UploadID.String(),
		URL:       result.URL,
		Fields:    result.Fields,
		ExpiresAt: result.ExpiresAt.Format("2006-01-02T15:04:05Z07:00"),
	})
}

func (h *DataUploadHandlers) CompleteUploadSession(ctx context.Context, r *http.Request) (rest.APIResponse, error) {
	log.Trace("DataUploadHandlers CompleteUploadSession")

	vars := mux.Vars(r)
	uploadID, err := uuid.Parse(vars["id"])
	if err != nil || uploadID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Error("parse upload ID failed")
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid upload ID")
	}
	if h.authenticator == nil {
		log.WithContext(ctx).Error("authenticator is not configured")
		return nil, rest.ErrInternalServer().WithMessage("Authentication is not configured")
	}
	authResult, err := h.authenticator.Authenticate(ctx, r)
	if err != nil {
		return nil, err
	}
	session, err := h.uploadUseCase.CompleteUploadSession(ctx, model.CompleteUploadSessionRequest{
		UploadID: uploadID,
		UserID:   authResult.UserID,
	})
	if err != nil {
		return nil, h.uploadError(ctx, err, "Failed to complete upload session")
	}
	return jsonResponse(http.StatusCreated, completeUploadResponse{
		UploadID:        session.UploadID.String(),
		DatasetID:       session.DatasetID.String(),
		StorageLocation: session.StorageLocation,
		Status:          string(session.Status),
		Checksum:        session.Checksum,
		ActualSizeBytes: session.ActualSizeBytes,
	})
}

// UploadDataFile uploads a data file for the given id dataset.
func (h *DataUploadHandlers) UploadDataFile(ctx context.Context, r *http.Request) (rest.APIResponse, error) {
	log.Trace("DataUploadHandlers UploadDataFile")

	datasetID, authResult, dataset, err := h.authenticateDatasetForUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	userID := authResult.UserID

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

func (h *DataUploadHandlers) authenticateDatasetForUpload(ctx context.Context, r *http.Request) (uuid.UUID, AuthResult, *model.Dataset, error) {
	log.Trace("DataUploadHandlers authenticateDatasetForUpload")

	vars := mux.Vars(r)
	datasetID, err := uuid.Parse(vars["id"])
	if err != nil || datasetID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Error("parse dataset ID failed")
		return uuid.Nil, AuthResult{}, nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid dataset ID")
	}
	ctx = context.WithValue(ctx, contextKey("DatasetID"), datasetID.String())
	if h.authenticator == nil {
		log.WithContext(ctx).Error("authenticator is not configured")
		return uuid.Nil, AuthResult{}, nil, rest.ErrInternalServer().WithMessage("Authentication is not configured")
	}
	authResult, err := h.authenticator.Authenticate(ctx, r)
	if err != nil {
		return uuid.Nil, AuthResult{}, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), authResult.UserID.String())
	dataset, err := h.datasetsUsecase.DatasetForUpload(ctx, datasetID, authResult.UserID)
	if err != nil {
		if errors.Is(err, domain.ErrResourceNotFound) {
			log.WithContext(ctx).Error("no valid dataset found for upload")
			return uuid.Nil, AuthResult{}, nil, rest.ErrNotFound().WithMessage("No valid dataset found for upload")
		}
		log.WithContext(ctx).WithError(err).Error("failed to validate dataset for upload")
		return uuid.Nil, AuthResult{}, nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to validate dataset for upload")
	}
	if err := requireDatasetUploadMetadata(dataset); err != nil {
		log.WithContext(ctx).WithError(err).Error("dataset materialization metadata is incomplete")
		return uuid.Nil, AuthResult{}, nil, rest.ErrInternalServer().Wrap(err).WithMessage("Dataset materialization metadata is incomplete")
	}
	return datasetID, authResult, dataset, nil
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

func (h *DataUploadHandlers) isSupportedFileFormat(format, fileName string) bool {
	format = strings.ToLower(strings.TrimSpace(format))
	for _, supported := range h.GetSupportedFileFormats(fileName) {
		if supported == format {
			return true
		}
	}
	return false
}

func (h *DataUploadHandlers) uploadError(ctx context.Context, err error, message string) error {
	if errors.Is(err, domain.ErrResourceNotFound) {
		return rest.ErrNotFound().WithMessage("Upload session not found")
	}
	if errors.Is(err, domain.ErrValidationFailed) {
		return rest.ErrBadRequest().Wrap(err).WithMessage(err.Error())
	}
	log.WithContext(ctx).WithError(err).Error(message)
	return rest.ErrInternalServer().Wrap(err).WithMessage(message)
}

func jsonResponse(statusCode int, body any) (rest.APIResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode response")
	}
	return rest.NewJSONResponse(statusCode, raw), nil
}
