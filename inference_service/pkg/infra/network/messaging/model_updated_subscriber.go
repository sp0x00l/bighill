package messaging

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain/model"

	datasetpb "lib/data_contracts_lib/data_registry"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type InferenceTopics struct {
	ModelRegistry     string
	DataRegistry      string
	PreferenceDataset string
}

func (t InferenceTopics) List() []string {
	log.Trace("InferenceTopics List")

	topics := make([]string, 0, 2)
	if strings.TrimSpace(t.ModelRegistry) != "" {
		topics = append(topics, t.ModelRegistry)
	}
	if strings.TrimSpace(t.DataRegistry) != "" {
		topics = append(topics, t.DataRegistry)
	}
	return topics
}

type ModelUpdatedSubscriber interface {
	Start(ctx context.Context) error
}

type modelUpdatedSubscriber struct {
	subscriber msgConn.Subscriber
	usecase    usecase.InferenceUsecase
	topics     InferenceTopics
}

func NewModelUpdatedSubscriber(subscriber msgConn.Subscriber, usecase usecase.InferenceUsecase, topics InferenceTopics) ModelUpdatedSubscriber {
	log.Trace("NewModelUpdatedSubscriber")

	return &modelUpdatedSubscriber{
		subscriber: subscriber,
		usecase:    usecase,
		topics:     topics,
	}
}

func (s *modelUpdatedSubscriber) Start(ctx context.Context) error {
	log.Trace("modelUpdatedSubscriber Start")

	msgConn.AddListener(s.subscriber, NewModelUpdatedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewDatasetUpdatedEventListener(s.usecase))
	return s.subscriber.Subscribe(ctx, s.topics.List())
}

type modelUpdatedEventListener struct {
	usecase usecase.InferenceUsecase
}

func NewModelUpdatedEventListener(usecase usecase.InferenceUsecase) *modelUpdatedEventListener {
	log.Trace("NewModelUpdatedEventListener")

	return &modelUpdatedEventListener{
		usecase: usecase,
	}
}

func (l *modelUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("modelUpdatedEventListener MsgType")

	return msgConn.MsgTypeModelUpdated
}

func (l *modelUpdatedEventListener) NewMessage() *modelregistrypb.ModelUpdatedEvent {
	log.Trace("modelUpdatedEventListener NewMessage")

	return &modelregistrypb.ModelUpdatedEvent{}
}

func (l *modelUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *modelregistrypb.ModelUpdatedEvent) error {
	log.Trace("modelUpdatedEventListener Handle")

	if l.usecase == nil {
		return msgConn.NonRetryable(fmt.Errorf("inference usecase is required"))
	}
	inferenceModel, idempotencyKey, err := modelUpdatedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordModelUpdated(ctx, inferenceModel, idempotencyKey)
	return err
}

type datasetUpdatedEventListener struct {
	usecase usecase.InferenceUsecase
}

func NewDatasetUpdatedEventListener(usecase usecase.InferenceUsecase) *datasetUpdatedEventListener {
	log.Trace("NewDatasetUpdatedEventListener")

	return &datasetUpdatedEventListener{
		usecase: usecase,
	}
}

func (l *datasetUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetUpdatedEventListener MsgType")

	return msgConn.MsgTypeDatasetUpdated
}

func (l *datasetUpdatedEventListener) NewMessage() *datasetpb.DatasetUpdatedEvent {
	log.Trace("datasetUpdatedEventListener NewMessage")

	return &datasetpb.DatasetUpdatedEvent{}
}

func (l *datasetUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent) error {
	log.Trace("datasetUpdatedEventListener Handle")

	if l.usecase == nil {
		return msgConn.NonRetryable(fmt.Errorf("inference usecase is required"))
	}
	dataset, idempotencyKey, err := datasetUpdatedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordDatasetUpdated(ctx, dataset, idempotencyKey)
	return err
}

func modelUpdatedEventToModel(resourceKey uuid.UUID, payload *modelregistrypb.ModelUpdatedEvent) (*model.InferenceModel, uuid.UUID, error) {
	log.Trace("modelUpdatedEventToModel")

	if resourceKey == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("model updated payload is required")
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if modelID != resourceKey {
		return nil, uuid.Nil, fmt.Errorf("model id %s does not match resource key %s", modelID, resourceKey)
	}
	trainingRunID, err := msgConn.ParseUUID("training_run_id", payload.GetTrainingRunId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	status, err := model.ToModelStatus(strings.TrimSpace(payload.GetStatus()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	servingLoadStatus, err := model.ToModelLoadStatus(strings.TrimSpace(payload.GetServingLoadStatus()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	inferenceModel := &model.InferenceModel{
		ModelID:           modelID,
		TrainingRunID:     trainingRunID,
		DatasetID:         datasetID,
		Name:              strings.TrimSpace(payload.GetName()),
		ModelVersion:      int(payload.GetModelVersion()),
		BaseModel:         strings.TrimSpace(payload.GetBaseModel()),
		ArtifactLocation:  strings.TrimSpace(payload.GetArtifactLocation()),
		ArtifactFormat:    strings.TrimSpace(payload.GetArtifactFormat()),
		ArtifactChecksum:  strings.TrimSpace(payload.GetArtifactChecksum()),
		ArtifactSizeBytes: payload.GetArtifactSizeBytes(),
		AdapterURI:        strings.TrimSpace(payload.GetAdapterUri()),
		ServingTarget:     strings.TrimSpace(payload.GetServingTarget()),
		ServingModel:      strings.TrimSpace(payload.GetServingModel()),
		ServingLoadStatus: servingLoadStatus,
		MetricsMetadata:   strings.TrimSpace(payload.GetMetricsMetadata()),
		Status:            status,
		FailureReason:     strings.TrimSpace(payload.GetFailureReason()),
	}
	if err := validateModelUpdatedEvent(inferenceModel); err != nil {
		return nil, uuid.Nil, err
	}
	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		modelID.String(),
		trainingRunID.String(),
		status.String(),
		inferenceModel.ArtifactChecksum,
	}, ":")))
	return inferenceModel, idempotencyKey, nil
}

func validateModelUpdatedEvent(inferenceModel *model.InferenceModel) error {
	log.Trace("validateModelUpdatedEvent")

	if strings.TrimSpace(inferenceModel.BaseModel) == "" {
		return fmt.Errorf("base model is required")
	}
	if strings.TrimSpace(inferenceModel.Name) == "" {
		return fmt.Errorf("model name is required")
	}
	if inferenceModel.ModelVersion <= 0 {
		return fmt.Errorf("model version is required")
	}
	if strings.TrimSpace(inferenceModel.MetricsMetadata) == "" {
		return fmt.Errorf("metrics metadata is required")
	}
	if inferenceModel.Status == model.ModelStatusReady && strings.TrimSpace(inferenceModel.ArtifactLocation) == "" {
		return fmt.Errorf("artifact location is required for ready models")
	}
	if inferenceModel.Status == model.ModelStatusReady && inferenceModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		return fmt.Errorf("ready models must be loaded by the serving layer")
	}
	if inferenceModel.Status == model.ModelStatusFailed && strings.TrimSpace(inferenceModel.FailureReason) == "" {
		return fmt.Errorf("failure reason is required for failed models")
	}
	return nil
}

func datasetUpdatedEventToModel(resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent) (*model.InferenceDataset, uuid.UUID, error) {
	log.Trace("datasetUpdatedEventToModel")

	if resourceKey == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("dataset updated payload is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	processingState, err := model.ToDatasetProcessingState(strings.TrimSpace(payload.GetProcessingState()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	rawSnapshotID, err := parseOptionalEventUUID("raw_snapshot_id", payload.GetRawSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	featureSnapshotID, err := parseOptionalEventUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	embeddingSnapshotID, err := parseOptionalEventUUID("embedding_snapshot_id", payload.GetEmbeddingSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}

	dataset := &model.InferenceDataset{
		DatasetID:                datasetID,
		UserID:                   userID,
		DatasetVersion:           int(payload.GetDatasetVersion()),
		ProcessingState:          processingState,
		StorageLocation:          strings.TrimSpace(payload.GetStorageLocation()),
		TableNamespace:           strings.TrimSpace(payload.GetTableNamespace()),
		TableName:                strings.TrimSpace(payload.GetTableName()),
		TableFormat:              strings.TrimSpace(payload.GetTableFormat()),
		CatalogProvider:          strings.TrimSpace(payload.GetCatalogProvider()),
		ProcessingProfile:        strings.TrimSpace(payload.GetProcessingProfile()),
		SchemaVersion:            int(payload.GetSchemaVersion()),
		SchemaMetadata:           strings.TrimSpace(payload.GetSchemaMetadata()),
		RawSnapshotID:            rawSnapshotID,
		FeatureSnapshotID:        featureSnapshotID,
		EmbeddingSnapshotID:      embeddingSnapshotID,
		VectorStore:              strings.TrimSpace(payload.GetVectorStore()),
		CollectionName:           strings.TrimSpace(payload.GetCollectionName()),
		EmbeddingDimensions:      int(payload.GetEmbeddingDimensions()),
		EmbeddingCount:           payload.GetEmbeddingCount(),
		EmbeddingStrategyVersion: strings.TrimSpace(payload.GetEmbeddingStrategyVersion()),
		EmbeddingChunkerName:     strings.TrimSpace(payload.GetEmbeddingChunkerName()),
		EmbeddingChunkerVersion:  strings.TrimSpace(payload.GetEmbeddingChunkerVersion()),
		EmbeddingChunkSize:       int(payload.GetEmbeddingChunkSize()),
		EmbeddingChunkOverlap:    int(payload.GetEmbeddingChunkOverlap()),
		EmbeddingProvider:        strings.TrimSpace(payload.GetEmbeddingProvider()),
		EmbeddingModel:           strings.TrimSpace(payload.GetEmbeddingModel()),
	}
	if err := validateDatasetUpdatedEvent(dataset); err != nil {
		return nil, uuid.Nil, err
	}

	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		datasetID.String(),
		strconv.Itoa(dataset.DatasetVersion),
		processingState.String(),
		dataset.RawSnapshotID.String(),
		dataset.FeatureSnapshotID.String(),
		dataset.EmbeddingSnapshotID.String(),
	}, ":")))
	return dataset, idempotencyKey, nil
}

func parseOptionalEventUUID(field, value string) (uuid.UUID, error) {
	log.Trace("parseOptionalEventUUID")

	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.Nil, nil
	}
	parsed, err := msgConn.ParseUUID(field, value)
	if err != nil {
		return uuid.Nil, err
	}
	return parsed, nil
}

func validateDatasetUpdatedEvent(dataset *model.InferenceDataset) error {
	log.Trace("validateDatasetUpdatedEvent")

	if dataset.DatasetVersion <= 0 {
		return fmt.Errorf("dataset version is required")
	}
	if dataset.SchemaVersion <= 0 {
		return fmt.Errorf("schema version is required")
	}
	if strings.TrimSpace(dataset.SchemaMetadata) == "" {
		return fmt.Errorf("schema metadata is required")
	}
	return nil
}
