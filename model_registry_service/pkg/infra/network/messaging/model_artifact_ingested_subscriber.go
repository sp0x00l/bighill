package messaging

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	usecase "model_registry_service/pkg/app"
	"model_registry_service/pkg/domain/model"

	ingestionpb "lib/data_contracts_lib/ingestion"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type modelArtifactIngestedEventListener struct {
	usecase usecase.ModelRegistryUsecase
}

func NewModelArtifactIngestedEventListener(usecase usecase.ModelRegistryUsecase) *modelArtifactIngestedEventListener {
	log.Trace("NewModelArtifactIngestedEventListener")

	return &modelArtifactIngestedEventListener{
		usecase: usecase,
	}
}

func (l *modelArtifactIngestedEventListener) MsgType() msgConn.MsgType {
	log.Trace("modelArtifactIngestedEventListener MsgType")

	return msgConn.MsgTypeModelArtifactIngested
}

func (l *modelArtifactIngestedEventListener) NewMessage() *ingestionpb.ModelArtifactIngestedEvent {
	log.Trace("modelArtifactIngestedEventListener NewMessage")

	return &ingestionpb.ModelArtifactIngestedEvent{}
}

func (l *modelArtifactIngestedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *ingestionpb.ModelArtifactIngestedEvent) error {
	log.Trace("modelArtifactIngestedEventListener Handle")

	ingestedModel, idempotencyKey, err := artifactIngestedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordModelArtifactIngested(ctx, ingestedModel, idempotencyKey)
	return err
}

func artifactIngestedEventToModel(resourceKey uuid.UUID, payload *ingestionpb.ModelArtifactIngestedEvent) (*model.Model, uuid.UUID, error) {
	log.Trace("artifactIngestedEventToModel")

	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("model artifact ingested payload is required")
	}
	modelID, err := msgConn.ParseUUID("artifact_id", payload.GetArtifactId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if resourceKey != uuid.Nil && resourceKey != modelID {
		return nil, uuid.Nil, fmt.Errorf("resource key does not match artifact id")
	}
	uploadID, err := msgConn.ParseUUID("upload_id", payload.GetUploadId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	datasetID, err := msgConn.ParseOptionalUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelName, err := requiredTrainingEventString("model name", payload.GetModelName())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelVersion, err := artifactModelVersion(payload.GetModelVersion())
	if err != nil {
		return nil, uuid.Nil, err
	}
	artifactLocation, err := requiredTrainingEventString("artifact location", payload.GetStorageLocation())
	if err != nil {
		return nil, uuid.Nil, err
	}
	artifactFormat, err := requiredTrainingEventString("artifact format", payload.GetArtifactFormat())
	if err != nil {
		return nil, uuid.Nil, err
	}
	baseModel, err := requiredTrainingEventString("base model", payload.GetBaseModel())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if payload.GetArtifactSizeBytes() <= 0 {
		return nil, uuid.Nil, fmt.Errorf("artifact size is required")
	}
	if strings.TrimSpace(payload.GetArtifactChecksum()) == "" {
		return nil, uuid.Nil, fmt.Errorf("artifact checksum is required")
	}
	artifactType, err := requiredTrainingEventString("artifact type", payload.GetArtifactType())
	if err != nil {
		return nil, uuid.Nil, err
	}
	artifactType = strings.ToUpper(strings.ReplaceAll(artifactType, "-", "_"))
	modelKind := model.ModelKindBase
	adapterURI := ""
	switch artifactType {
	case "BASE_MODEL", "GGUF":
	case "LORA_ADAPTER":
		modelKind = model.ModelKindFineTuned
		adapterURI = artifactLocation
	case "MERGED_MODEL":
		modelKind = model.ModelKindFineTuned
	default:
		return nil, uuid.Nil, fmt.Errorf("artifact type is invalid")
	}
	userID := uuid.Nil
	if modelKind == model.ModelKindBase {
		userID, err = msgConn.ParseOptionalUUID("user_id", payload.GetUserId())
	} else {
		userID, err = msgConn.ParseUUID("user_id", payload.GetUserId())
	}
	if err != nil {
		return nil, uuid.Nil, err
	}
	source := sourceFromArtifactEvent(payload.GetSource())
	if !model.IsKnownModelSource(source) {
		return nil, uuid.Nil, fmt.Errorf("model source is invalid")
	}
	ingestedModel := &model.Model{
		ModelID:           modelID,
		UserID:            userID,
		DatasetID:         datasetID,
		ModelKind:         modelKind,
		Source:            source,
		SourceURI:         strings.TrimSpace(payload.GetSourceUri()),
		SourceMetadata:    withDefaultSourceMetadata(payload.GetSourceMetadata()),
		Name:              modelName,
		ModelVersion:      modelVersion,
		BaseModel:         baseModel,
		ArtifactLocation:  artifactLocation,
		ArtifactFormat:    artifactFormat,
		ArtifactChecksum:  strings.TrimSpace(payload.GetArtifactChecksum()),
		ArtifactSizeBytes: payload.GetArtifactSizeBytes(),
		AdapterURI:        adapterURI,
		ServingLoadStatus: model.ModelLoadStatusNotLoaded,
		MetricsMetadata:   "{}",
	}
	return ingestedModel, uploadID, nil
}

func artifactModelVersion(value string) (int, error) {
	log.Trace("artifactModelVersion")

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("model version is required")
	}
	version, err := strconv.Atoi(trimmed)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("model version is invalid")
	}
	return version, nil
}

func sourceFromArtifactEvent(value string) model.ModelSource {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "hugging_face", "huggingface", "hf":
		return model.ModelSourceHuggingFace
	case "upload":
		return model.ModelSourceUpload
	default:
		return model.ModelSource(strings.ToUpper(strings.TrimSpace(value)))
	}
}

func withDefaultSourceMetadata(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return strings.TrimSpace(value)
}
