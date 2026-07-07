package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	dtoadapter "ingestion_service/pkg/infra/network/adapter"
	"io"
	"lib/shared_lib/ctxutil"
	"lib/shared_lib/transport"
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
	InitiateModelUploadSession(ctx context.Context, request model.InitiateModelUploadSessionRequest) (*model.InitiatedUploadSession, error)
	CompleteUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest) (*model.UploadSession, error)
	CompleteModelUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest) (*model.UploadSession, error)
	OnboardHuggingFaceModel(ctx context.Context, request model.OnboardHuggingFaceModelRequest) (*model.UploadSession, error)
}

type DatasetUsecase interface {
	DatasetForUpload(ctx context.Context, datasetID, userID uuid.UUID) (*model.Dataset, error)
}

type Authenticator interface {
	Authenticate(ctx context.Context, r *http.Request) (AuthResult, error)
}

type UploadDTOAdapter interface {
	FromInitiateUploadDTO(context.Context, []byte, uuid.UUID, uuid.UUID, *model.Dataset, dtoadapter.UploadFormatResolver, int64) (*model.InitiateUploadSessionRequest, error)
	FromInitiateModelUploadDTO(context.Context, []byte, uuid.UUID, int64) (*model.InitiateModelUploadSessionRequest, error)
	FromOnboardHuggingFaceModelDTO(context.Context, []byte, uuid.UUID) (*model.OnboardHuggingFaceModelRequest, error)
}

type contextKey string

type DataUploadHandlers struct {
	uploadUseCase           DataUploadUseCase
	datasetsUsecase         DatasetUsecase
	uploadDTOAdapter        UploadDTOAdapter
	authenticator           Authenticator
	maxFileSizeBytes        int64
	MaxBytesReaderSizeBytes int64
	detector                FileDetector
	supportedFilesFormats   map[string][]string
}

func NewDataUploadHandlers(uploadUseCase DataUploadUseCase, datasetUseCase DatasetUsecase, uploadDTOAdapter UploadDTOAdapter, detector FileDetector, authenticator Authenticator, maxFileSizeBytes int64) *DataUploadHandlers {
	log.Trace("rest NewDataUploadHandlers")

	return &DataUploadHandlers{
		uploadUseCase:           uploadUseCase,
		datasetsUsecase:         datasetUseCase,
		uploadDTOAdapter:        uploadDTOAdapter,
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

func (h *DataUploadHandlers) GetRoutes() []Route {
	routes := []Route{
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
			Path:     "/v1/models/uploads",
			Handler:  h.InitiateModelUploadSession,
			Method:   http.MethodPost,
			SpanName: "initiate-model-upload-session",
		},
		{
			Path:     "/v1/models/uploads/{id}/complete",
			Handler:  h.CompleteModelUploadSession,
			Method:   http.MethodPost,
			SpanName: "complete-model-upload-session",
		},
		{
			Path:     "/v1/models/onboard/huggingface",
			Handler:  h.OnboardHuggingFaceModel,
			Method:   http.MethodPost,
			SpanName: "onboard-huggingface-model",
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

type initiateModelUploadResponse struct {
	UploadID   string            `json:"upload_id"`
	ResourceID string            `json:"resource_id"`
	URL        string            `json:"url"`
	Fields     map[string]string `json:"fields"`
	ExpiresAt  string            `json:"expires_at"`
}

type completeModelUploadResponse struct {
	UploadID        string `json:"upload_id"`
	ResourceID      string `json:"resource_id"`
	StorageLocation string `json:"storage_location"`
	Status          string `json:"status"`
	Checksum        string `json:"checksum"`
	ActualSizeBytes int64  `json:"actual_size_bytes"`
	ArtifactType    string `json:"artifact_type"`
	ArtifactFormat  string `json:"artifact_format"`
	ModelName       string `json:"model_name"`
	ModelVersion    string `json:"model_version"`
	BaseModel       string `json:"base_model"`
}

func (h *DataUploadHandlers) InitiateUploadSession(ctx context.Context, r *http.Request) (APIResponse, error) {
	log.Trace("DataUploadHandlers InitiateUploadSession")

	datasetID, authResult, dataset, err := h.authenticateDatasetForUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	body, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}
	command, err := h.uploadDTOAdapter.FromInitiateUploadDTO(ctx, body, datasetID, authResult.UserID, dataset, h.resolveInitiateUploadFormat, h.maxFileSizeBytes)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid upload session request")
	}
	command.OrgID = authResult.OrgID
	result, err := h.uploadUseCase.InitiateUploadSession(ctx, *command)
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

func (h *DataUploadHandlers) InitiateModelUploadSession(ctx context.Context, r *http.Request) (APIResponse, error) {
	log.Trace("DataUploadHandlers InitiateModelUploadSession")

	authResult, err := h.authenticateRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	body, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}
	command, err := h.uploadDTOAdapter.FromInitiateModelUploadDTO(ctx, body, authResult.UserID, h.maxFileSizeBytes)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid model upload session request")
	}
	command.OrgID = authResult.OrgID
	result, err := h.uploadUseCase.InitiateModelUploadSession(ctx, *command)
	if err != nil {
		return nil, h.uploadError(ctx, err, "Failed to initiate model upload session")
	}
	return jsonResponse(http.StatusCreated, initiateModelUploadResponse{
		UploadID:   result.UploadID.String(),
		ResourceID: result.ResourceID.String(),
		URL:        result.URL,
		Fields:     result.Fields,
		ExpiresAt:  result.ExpiresAt.Format("2006-01-02T15:04:05Z07:00"),
	})
}

func (h *DataUploadHandlers) CompleteUploadSession(ctx context.Context, r *http.Request) (APIResponse, error) {
	log.Trace("DataUploadHandlers CompleteUploadSession")

	vars := mux.Vars(r)
	uploadID, err := uuid.Parse(vars["id"])
	if err != nil || uploadID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Error("parse upload ID failed")
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid upload ID")
	}
	if h.authenticator == nil {
		log.WithContext(ctx).Error("authenticator is not configured")
		return nil, ErrInternalServer().WithMessage("Authentication is not configured")
	}
	authResult, err := h.authenticator.Authenticate(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	session, err := h.uploadUseCase.CompleteUploadSession(ctx, model.CompleteUploadSessionRequest{
		UploadID: uploadID,
		UserID:   authResult.UserID,
		OrgID:    authResult.OrgID,
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

func (h *DataUploadHandlers) CompleteModelUploadSession(ctx context.Context, r *http.Request) (APIResponse, error) {
	log.Trace("DataUploadHandlers CompleteModelUploadSession")

	uploadID, authResult, err := h.uploadIDAndAuthenticatedUser(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	session, err := h.uploadUseCase.CompleteModelUploadSession(ctx, model.CompleteUploadSessionRequest{
		UploadID: uploadID,
		UserID:   authResult.UserID,
		OrgID:    authResult.OrgID,
	})
	if err != nil {
		return nil, h.uploadError(ctx, err, "Failed to complete model upload session")
	}
	return jsonResponse(http.StatusCreated, completeModelUploadResponse{
		UploadID:        session.UploadID.String(),
		ResourceID:      session.ResourceID.String(),
		StorageLocation: session.StorageLocation,
		Status:          string(session.Status),
		Checksum:        session.Checksum,
		ActualSizeBytes: session.ActualSizeBytes,
		ArtifactType:    session.ArtifactType,
		ArtifactFormat:  session.DeclaredFormat,
		ModelName:       session.ModelName,
		ModelVersion:    session.ModelVersion,
		BaseModel:       session.BaseModel,
	})
}

func (h *DataUploadHandlers) OnboardHuggingFaceModel(ctx context.Context, r *http.Request) (APIResponse, error) {
	log.Trace("DataUploadHandlers OnboardHuggingFaceModel")

	authResult, err := h.authenticateRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	body, err := transport.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}
	command, err := h.uploadDTOAdapter.FromOnboardHuggingFaceModelDTO(ctx, body, authResult.UserID)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid Hugging Face onboarding request")
	}
	command.OrgID = authResult.OrgID
	session, err := h.uploadUseCase.OnboardHuggingFaceModel(ctx, *command)
	if err != nil {
		return nil, h.uploadError(ctx, err, "Failed to onboard Hugging Face model")
	}
	return jsonResponse(http.StatusCreated, completeModelUploadResponse{
		UploadID:        session.UploadID.String(),
		ResourceID:      session.ResourceID.String(),
		StorageLocation: session.StorageLocation,
		Status:          string(session.Status),
		Checksum:        session.Checksum,
		ActualSizeBytes: session.ActualSizeBytes,
		ArtifactType:    session.ArtifactType,
		ArtifactFormat:  session.DeclaredFormat,
		ModelName:       session.ModelName,
		ModelVersion:    session.ModelVersion,
		BaseModel:       session.BaseModel,
	})
}

// UploadDataFile uploads a data file for the given id dataset.
func (h *DataUploadHandlers) UploadDataFile(ctx context.Context, r *http.Request) (APIResponse, error) {
	log.Trace("DataUploadHandlers UploadDataFile")

	datasetID, authResult, dataset, err := h.authenticateDatasetForUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	userID := authResult.UserID

	// Set the maximum file size for the request (this includes the entire request body) to prevent abuse.
	r.Body = http.MaxBytesReader(nil, r.Body, h.MaxBytesReaderSizeBytes)

	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		if err.Error() == (&http.MaxBytesError{}).Error() {
			log.WithContext(ctx).WithError(err).Error("file size is too large")
			return nil, ErrBadRequest().WithMessage("File size is too large")
		}
		log.WithContext(ctx).WithError(err).Error("failed to read form file for data set file upload")
		return nil, ErrBadRequest().Wrap(err).WithMessage("Failed to read form file for data set file upload")
	}
	defer file.Close()

	if fileHeader.Size > h.maxFileSizeBytes {
		log.WithContext(ctx).Error("file size is too large")
		return nil, ErrBadRequest().WithMessage("File size is too large")
	}

	if fileHeader.Size == 0 {
		log.WithContext(ctx).Error("file is empty")
		return nil, ErrBadRequest().WithMessage("File is empty")
	}

	supportedFileTypes := h.GetSupportedFileFormats(fileHeader.Filename)

	fileFormat := h.detector.DetectFileFormat(ctx, file, int(fileHeader.Size), supportedFileTypes)
	if fileFormat == FileTypeUnsupported {
		log.WithContext(ctx).Error("file format not supported")
		return nil, ErrBadRequest().WithMessage("File format not supported")
	}

	contentType := h.detector.GetContentType(fileFormat)
	if contentType == DefaultContentType {
		log.WithContext(ctx).Error("content type not supported")
		return nil, ErrBadRequest().WithMessage("Content type not supported")
	}

	upload := &model.DataFile{
		DatasetID:         datasetID,
		UserID:            userID,
		OrgID:             authResult.OrgID,
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
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to upload data set file")
	}

	return NewReponse(http.StatusCreated), nil
}

func (h *DataUploadHandlers) authenticateDatasetForUpload(ctx context.Context, r *http.Request) (uuid.UUID, AuthResult, *model.Dataset, error) {
	log.Trace("DataUploadHandlers authenticateDatasetForUpload")

	vars := mux.Vars(r)
	datasetID, err := uuid.Parse(vars["id"])
	if err != nil || datasetID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Error("parse dataset ID failed")
		return uuid.Nil, AuthResult{}, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid dataset ID")
	}
	ctx = context.WithValue(ctx, contextKey("DatasetID"), datasetID.String())
	authResult, err := h.authenticateRequest(ctx, r)
	if err != nil {
		return uuid.Nil, AuthResult{}, nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), authResult.UserID.String())
	ctx = ctxutil.WithActorOrg(ctx, authResult.UserID, authResult.OrgID)
	dataset, err := h.datasetsUsecase.DatasetForUpload(ctx, datasetID, authResult.UserID)
	if err != nil {
		if errors.Is(err, domain.ErrResourceNotFound) {
			log.WithContext(ctx).Error("no valid dataset found for upload")
			return uuid.Nil, AuthResult{}, nil, ErrNotFound().WithMessage("No valid dataset found for upload")
		}
		log.WithContext(ctx).WithError(err).Error("failed to validate dataset for upload")
		return uuid.Nil, AuthResult{}, nil, ErrInternalServer().Wrap(err).WithMessage("Failed to validate dataset for upload")
	}
	if err := requireDatasetUploadMetadata(dataset); err != nil {
		log.WithContext(ctx).WithError(err).Error("dataset materialization metadata is incomplete")
		return uuid.Nil, AuthResult{}, nil, ErrInternalServer().Wrap(err).WithMessage("Dataset materialization metadata is incomplete")
	}
	return datasetID, authResult, dataset, nil
}

func (h *DataUploadHandlers) uploadIDAndAuthenticatedUser(ctx context.Context, r *http.Request) (uuid.UUID, AuthResult, error) {
	log.Trace("DataUploadHandlers uploadIDAndAuthenticatedUser")

	vars := mux.Vars(r)
	uploadID, err := uuid.Parse(vars["id"])
	if err != nil || uploadID == uuid.Nil {
		log.WithContext(ctx).WithError(err).Error("parse upload ID failed")
		return uuid.Nil, AuthResult{}, ErrBadRequest().Wrap(err).WithMessage("Invalid upload ID")
	}
	authResult, err := h.authenticateRequest(ctx, r)
	if err != nil {
		return uuid.Nil, AuthResult{}, err
	}
	return uploadID, authResult, nil
}

func (h *DataUploadHandlers) authenticateRequest(ctx context.Context, r *http.Request) (AuthResult, error) {
	log.Trace("DataUploadHandlers authenticateRequest")

	if h.authenticator == nil {
		log.WithContext(ctx).Error("authenticator is not configured")
		return AuthResult{}, ErrInternalServer().WithMessage("Authentication is not configured")
	}
	authResult, err := h.authenticator.Authenticate(ctx, r)
	if err != nil {
		return AuthResult{}, err
	}
	return authResult, nil
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

func (h *DataUploadHandlers) resolveInitiateUploadFormat(ctx context.Context, fileName, declaredFormat, contentType string) (string, string, error) {
	log.Trace("DataUploadHandlers resolveInitiateUploadFormat")
	_ = ctx

	format := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(declaredFormat), "."))
	if format == "" {
		format = strings.ToLower(strings.TrimPrefix(filepath.Ext(fileName), "."))
	}
	if !h.isSupportedFileFormat(format, fileName) {
		return "", "", domain.ErrValidationFailed.Extend("declared file format is not supported")
	}
	resolvedContentType := strings.TrimSpace(contentType)
	if resolvedContentType == "" {
		resolvedContentType = h.detector.GetContentType(format)
	}
	if resolvedContentType == DefaultContentType {
		return "", "", domain.ErrValidationFailed.Extend("content type not supported")
	}
	return format, resolvedContentType, nil
}

func (h *DataUploadHandlers) uploadError(ctx context.Context, err error, message string) error {
	if errors.Is(err, domain.ErrResourceNotFound) {
		return ErrNotFound().WithMessage("Upload session not found")
	}
	if errors.Is(err, domain.ErrValidationFailed) {
		return ErrBadRequest().Wrap(err).WithMessage(err.Error())
	}
	log.WithContext(ctx).WithError(err).Error(message)
	return ErrInternalServer().Wrap(err).WithMessage(message)
}

func jsonResponse(statusCode int, body any) (APIResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode response")
	}
	return NewJSONResponse(statusCode, raw), nil
}
