package adapter

import (
	"context"
	"strconv"
	"strings"

	domainErrors "ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"
	"lib/shared_lib/uuidutil"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	defaultHuggingFaceRevision = "main"
	defaultModelContentType    = "application/octet-stream"
)

type InitiateUploadDTO struct {
	FileName          string `json:"file_name"           validate:"required"`
	DeclaredFormat    string `json:"declared_format"`
	ContentType       string `json:"content_type"`
	DeclaredSizeBytes int64  `json:"declared_size_bytes" validate:"required,gt=0"`
	ClientNonce       string `json:"client_nonce"        validate:"required"`
}

type InitiateModelUploadDTO struct {
	ResourceID        string `json:"resource_id"         validate:"omitempty,uuid"`
	DatasetID         string `json:"dataset_id"          validate:"required,uuid"`
	FileName          string `json:"file_name"           validate:"required"`
	ArtifactType      string `json:"artifact_type"       validate:"required,model_artifact_type"`
	ArtifactFormat    string `json:"artifact_format"     validate:"required,model_artifact_format"`
	ContentType       string `json:"content_type"`
	DeclaredSizeBytes int64  `json:"declared_size_bytes" validate:"required,gt=0"`
	ClientNonce       string `json:"client_nonce"        validate:"required"`
	ModelName         string `json:"model_name"          validate:"required"`
	ModelVersion      string `json:"model_version"       validate:"required,model_version"`
	BaseModel         string `json:"base_model"          validate:"required"`
}

type OnboardHuggingFaceModelDTO struct {
	ResourceID     string `json:"resource_id"   validate:"omitempty,uuid"`
	DatasetID      string `json:"dataset_id"    validate:"required,uuid"`
	RepoID         string `json:"repo_id"       validate:"required"`
	Revision       string `json:"revision"`
	HFFile         string `json:"hf_file"`
	ClientNonce    string `json:"client_nonce"  validate:"required"`
	ModelName      string `json:"model_name"    validate:"required"`
	ModelVersion   string `json:"model_version" validate:"required,model_version"`
	BaseModel      string `json:"base_model"    validate:"required"`
	ArtifactType   string `json:"artifact_type"   validate:"omitempty,model_artifact_type"`
	ArtifactFormat string `json:"artifact_format" validate:"omitempty,model_artifact_format"`
}

type UploadDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type UploadFormatResolver func(ctx context.Context, fileName, declaredFormat, contentType string) (string, string, error)

func NewUploadDTOAdapter(encoder *serializers.Encoder) *UploadDTOAdapter {
	log.Trace("NewUploadDTOAdapter")

	v := validator.New()
	_ = v.RegisterValidation("model_artifact_type", func(fl validator.FieldLevel) bool {
		return isSupportedModelArtifactType(fl.Field().String())
	})
	_ = v.RegisterValidation("model_artifact_format", func(fl validator.FieldLevel) bool {
		return isSupportedModelArtifactFormat(fl.Field().String())
	})
	_ = v.RegisterValidation("model_version", func(fl validator.FieldLevel) bool {
		_, err := normalizeModelVersion(fl.Field().String())
		return err == nil
	})
	return &UploadDTOAdapter{
		validator: v,
		encoder:   encoder,
	}
}

func (a *UploadDTOAdapter) FromInitiateUploadDTO(
	ctx context.Context,
	body []byte,
	datasetID uuid.UUID,
	userID uuid.UUID,
	dataset *model.Dataset,
	resolveFormat UploadFormatResolver,
	maxFileSizeBytes int64,
) (*model.InitiateUploadSessionRequest, error) {
	log.Trace("UploadDTOAdapter FromInitiateUploadDTO")

	var dto InitiateUploadDTO
	if err := a.decodeAndValidate(ctx, body, &dto, "upload session DTO"); err != nil {
		return nil, err
	}
	declaredFormat, contentType, err := resolveFormat(ctx, dto.FileName, dto.DeclaredFormat, dto.ContentType)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(declaredFormat) == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("declared format is required")
	}
	if strings.TrimSpace(contentType) == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("content type is required")
	}
	if dto.DeclaredSizeBytes > maxFileSizeBytes {
		return nil, domainErrors.ErrValidationFailed.Extend("declared size is too large")
	}
	return &model.InitiateUploadSessionRequest{
		DatasetID:           datasetID,
		UserID:              userID,
		ClientNonce:         dto.ClientNonce,
		FileName:            dto.FileName,
		DeclaredFormat:      declaredFormat,
		DeclaredContentType: contentType,
		DeclaredSizeBytes:   dto.DeclaredSizeBytes,
		TableNamespace:      dataset.TableNamespace,
		TableName:           dataset.TableName,
		TableFormat:         dataset.TableFormat,
		CatalogProvider:     dataset.CatalogProvider,
		ProcessingProfile:   dataset.ProcessingProfile,
	}, nil
}

func (a *UploadDTOAdapter) FromInitiateModelUploadDTO(
	ctx context.Context,
	body []byte,
	userID uuid.UUID,
	maxFileSizeBytes int64,
) (*model.InitiateModelUploadSessionRequest, error) {
	log.Trace("UploadDTOAdapter FromInitiateModelUploadDTO")

	var dto InitiateModelUploadDTO
	if err := a.decodeAndValidate(ctx, body, &dto, "model upload session DTO"); err != nil {
		return nil, err
	}
	contentType := strings.TrimSpace(dto.ContentType)
	if contentType == "" {
		contentType = defaultModelContentType
	}
	if strings.TrimSpace(contentType) == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("content type is required")
	}
	if dto.DeclaredSizeBytes > maxFileSizeBytes {
		return nil, domainErrors.ErrValidationFailed.Extend("declared size is too large")
	}
	resourceID, err := uuidutil.ParseOptional("resource_id", dto.ResourceID)
	if err != nil {
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	datasetID, err := uuidutil.ParseOptional("dataset_id", dto.DatasetID)
	if err != nil {
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	modelVersion, err := normalizeModelVersion(dto.ModelVersion)
	if err != nil {
		return nil, err
	}
	return &model.InitiateModelUploadSessionRequest{
		ResourceID:          resourceID,
		DatasetID:           datasetID,
		UserID:              userID,
		ClientNonce:         dto.ClientNonce,
		FileName:            dto.FileName,
		ArtifactType:        normalizeModelArtifactToken(dto.ArtifactType),
		ArtifactFormat:      normalizeModelArtifactToken(dto.ArtifactFormat),
		DeclaredContentType: contentType,
		DeclaredSizeBytes:   dto.DeclaredSizeBytes,
		ModelName:           dto.ModelName,
		ModelVersion:        modelVersion,
		BaseModel:           dto.BaseModel,
	}, nil
}

func (a *UploadDTOAdapter) FromOnboardHuggingFaceModelDTO(ctx context.Context, body []byte, userID uuid.UUID) (*model.OnboardHuggingFaceModelRequest, error) {
	log.Trace("UploadDTOAdapter FromOnboardHuggingFaceModelDTO")

	var dto OnboardHuggingFaceModelDTO
	if err := a.decodeAndValidate(ctx, body, &dto, "Hugging Face onboarding DTO"); err != nil {
		return nil, err
	}
	resourceID, err := uuidutil.ParseOptional("resource_id", dto.ResourceID)
	if err != nil {
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	datasetID, err := uuidutil.ParseOptional("dataset_id", dto.DatasetID)
	if err != nil {
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	modelVersion, err := normalizeModelVersion(dto.ModelVersion)
	if err != nil {
		return nil, err
	}
	revision := strings.TrimSpace(dto.Revision)
	if revision == "" {
		revision = defaultHuggingFaceRevision
	}
	return &model.OnboardHuggingFaceModelRequest{
		ResourceID:      resourceID,
		DatasetID:       datasetID,
		UserID:          userID,
		ClientNonce:     dto.ClientNonce,
		RepoID:          dto.RepoID,
		Revision:        revision,
		HuggingFaceFile: strings.TrimSpace(dto.HFFile),
		ArtifactType:    modelArtifactTokenOrDefault(dto.ArtifactType, string(model.ModelArtifactTypeBase)),
		ArtifactFormat:  modelArtifactTokenOrDefault(dto.ArtifactFormat, string(model.ModelArtifactFormatHFModel)),
		ModelName:       dto.ModelName,
		ModelVersion:    modelVersion,
		BaseModel:       dto.BaseModel,
	}, nil
}

func (a *UploadDTOAdapter) decodeAndValidate(ctx context.Context, body []byte, dto any, name string) error {
	log.Trace("UploadDTOAdapter decodeAndValidate")

	if err := a.encoder.Deserialize(body, dto); err != nil {
		log.WithContext(ctx).WithError(err).Error(name + " decoding failed")
		return domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error(name + " validation failed")
		return domainErrors.ErrValidationFailed.Extend(err.Error())
	}
	return nil
}

func normalizeModelVersion(value string) (string, error) {
	log.Trace("normalizeModelVersion")

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", domainErrors.ErrValidationFailed.Extend("model version is required")
	}
	normalized := strings.TrimPrefix(strings.ToLower(trimmed), "v")
	version, err := strconv.Atoi(normalized)
	if err != nil || version <= 0 {
		return "", domainErrors.ErrValidationFailed.Extend("model version must be a positive integer")
	}
	return strconv.Itoa(version), nil
}

func normalizeModelArtifactToken(value string) string {
	log.Trace("normalizeModelArtifactToken")

	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"))
}

func modelArtifactTokenOrDefault(value string, fallback string) string {
	log.Trace("modelArtifactTokenOrDefault")

	normalized := normalizeModelArtifactToken(value)
	if normalized == "" {
		return fallback
	}
	return normalized
}

func isSupportedModelArtifactType(value string) bool {
	switch normalizeModelArtifactToken(value) {
	case string(model.ModelArtifactTypeBase), string(model.ModelArtifactTypeLoraAdapter), string(model.ModelArtifactTypeMerged):
		return true
	default:
		return false
	}
}

func isSupportedModelArtifactFormat(value string) bool {
	switch normalizeModelArtifactToken(value) {
	case string(model.ModelArtifactFormatHFModel),
		string(model.ModelArtifactFormatHFPEFTAdapter),
		string(model.ModelArtifactFormatSafetensors),
		string(model.ModelArtifactFormatGGUF),
		string(model.ModelArtifactFormatGGUFModel),
		string(model.ModelArtifactFormatGGUFLoraAdapter),
		string(model.ModelArtifactFormatZIP):
		return true
	default:
		return false
	}
}
