package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	usecase "model_registry_service/pkg/app"
	"model_registry_service/pkg/domain/model"

	trainingpb "lib/data_contracts_lib/training"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type TrainingEventSubscriber interface {
	Start(ctx context.Context) error
}

type trainingEventSubscriber struct {
	subscriber msgConn.Subscriber
	usecase    usecase.ModelRegistryUsecase
	topics     ModelRegistryTopics
}

func NewTrainingEventSubscriber(subscriber msgConn.Subscriber, usecase usecase.ModelRegistryUsecase, topics ModelRegistryTopics) TrainingEventSubscriber {
	log.Trace("NewTrainingEventSubscriber")

	return &trainingEventSubscriber{
		subscriber: subscriber,
		usecase:    usecase,
		topics:     topics,
	}
}

func (s *trainingEventSubscriber) Start(ctx context.Context) error {
	log.Trace("trainingEventSubscriber Start")

	msgConn.AddListener(s.subscriber, NewModelTrainingCompletedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewModelTrainingFailedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewPromotionReportReadyEventListener(s.usecase))
	return s.subscriber.Subscribe(ctx, []string{s.topics.Training})
}

type modelTrainingCompletedEventListener struct {
	usecase usecase.ModelRegistryUsecase
}

func NewModelTrainingCompletedEventListener(usecase usecase.ModelRegistryUsecase) *modelTrainingCompletedEventListener {
	log.Trace("NewModelTrainingCompletedEventListener")

	return &modelTrainingCompletedEventListener{
		usecase: usecase,
	}
}

func (l *modelTrainingCompletedEventListener) MsgType() msgConn.MsgType {
	log.Trace("modelTrainingCompletedEventListener MsgType")

	return msgConn.MsgTypeModelTrainingCompleted
}

func (l *modelTrainingCompletedEventListener) NewMessage() *trainingpb.ModelTrainingCompletedEvent {
	log.Trace("modelTrainingCompletedEventListener NewMessage")

	return &trainingpb.ModelTrainingCompletedEvent{}
}

func (l *modelTrainingCompletedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *trainingpb.ModelTrainingCompletedEvent) error {
	log.Trace("modelTrainingCompletedEventListener Handle")

	trainedModel, idempotencyKey, err := completedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordModelTrainingCompleted(ctx, trainedModel, idempotencyKey)
	return err
}

type promotionReportReadyEventListener struct {
	usecase usecase.ModelRegistryUsecase
}

func NewPromotionReportReadyEventListener(usecase usecase.ModelRegistryUsecase) *promotionReportReadyEventListener {
	log.Trace("NewPromotionReportReadyEventListener")

	return &promotionReportReadyEventListener{
		usecase: usecase,
	}
}

func (l *promotionReportReadyEventListener) MsgType() msgConn.MsgType {
	log.Trace("promotionReportReadyEventListener MsgType")

	return msgConn.MsgTypePromotionReportReady
}

func (l *promotionReportReadyEventListener) NewMessage() *trainingpb.PromotionReportReadyEvent {
	log.Trace("promotionReportReadyEventListener NewMessage")

	return &trainingpb.PromotionReportReadyEvent{}
}

func (l *promotionReportReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *trainingpb.PromotionReportReadyEvent) error {
	log.Trace("promotionReportReadyEventListener Handle")

	report, idempotencyKey, err := promotionReportReadyToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordPromotionReportReady(ctx, report, idempotencyKey)
	return err
}

type modelTrainingFailedEventListener struct {
	usecase usecase.ModelRegistryUsecase
}

func NewModelTrainingFailedEventListener(usecase usecase.ModelRegistryUsecase) *modelTrainingFailedEventListener {
	log.Trace("NewModelTrainingFailedEventListener")

	return &modelTrainingFailedEventListener{
		usecase: usecase,
	}
}

func (l *modelTrainingFailedEventListener) MsgType() msgConn.MsgType {
	log.Trace("modelTrainingFailedEventListener MsgType")

	return msgConn.MsgTypeModelTrainingFailed
}

func (l *modelTrainingFailedEventListener) NewMessage() *trainingpb.ModelTrainingFailedEvent {
	log.Trace("modelTrainingFailedEventListener NewMessage")

	return &trainingpb.ModelTrainingFailedEvent{}
}

func (l *modelTrainingFailedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *trainingpb.ModelTrainingFailedEvent) error {
	log.Trace("modelTrainingFailedEventListener Handle")

	failedModel, idempotencyKey, err := failedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordModelTrainingFailed(ctx, failedModel, idempotencyKey)
	return err
}

func completedEventToModel(resourceKey uuid.UUID, payload *trainingpb.ModelTrainingCompletedEvent) (*model.Model, uuid.UUID, error) {
	log.Trace("completedEventToModel")

	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("model training completed payload is required")
	}
	trainingRunID, datasetID, err := parseTrainingEventIDs(resourceKey, payload.GetTrainingRunId(), payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelVersion, err := modelVersionFromEvent(payload.GetModelVersion())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelName, err := requiredTrainingEventString("model name", payload.GetModelName())
	if err != nil {
		return nil, uuid.Nil, err
	}
	metricsMetadata, err := requiredTrainingEventString("metrics metadata", payload.GetMetricsMetadata())
	if err != nil {
		return nil, uuid.Nil, err
	}
	servingProtocol, err := model.ToServingProtocol(strings.TrimSpace(payload.GetServingProtocol()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	trainedModel := &model.Model{
		ModelID:           modelID,
		UserID:            userID,
		OrgID:             orgID,
		TrainingRunID:     trainingRunID,
		DatasetID:         datasetID,
		Name:              modelName,
		ModelVersion:      modelVersion,
		BaseModel:         strings.TrimSpace(payload.GetBaseModel()),
		ArtifactLocation:  strings.TrimSpace(payload.GetArtifactLocation()),
		ArtifactFormat:    strings.TrimSpace(payload.GetArtifactFormat()),
		ArtifactChecksum:  strings.TrimSpace(payload.GetArtifactChecksum()),
		ArtifactSizeBytes: payload.GetArtifactSizeBytes(),
		AdapterURI:        strings.TrimSpace(payload.GetAdapterUri()),
		ServingTarget:     strings.TrimSpace(payload.GetServingTarget()),
		ServingModel:      strings.TrimSpace(payload.GetServingModel()),
		ServingProtocol:   servingProtocol,
		MetricsMetadata:   metricsMetadata,
	}
	trainedModel.ServingLoadStatus, err = model.ToModelLoadStatus(strings.TrimSpace(payload.GetServingLoadStatus()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	if err := validateCompletedModelEvent(trainedModel); err != nil {
		return nil, uuid.Nil, err
	}
	return trainedModel, trainingRunID, nil
}

func failedEventToModel(resourceKey uuid.UUID, payload *trainingpb.ModelTrainingFailedEvent) (*model.Model, uuid.UUID, error) {
	log.Trace("failedEventToModel")

	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("model training failed payload is required")
	}
	trainingRunID, datasetID, err := parseTrainingEventIDs(resourceKey, payload.GetTrainingRunId(), payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelVersion, err := modelVersionFromEvent(payload.GetModelVersion())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelName, err := requiredTrainingEventString("model name", payload.GetModelName())
	if err != nil {
		return nil, uuid.Nil, err
	}
	failedModel := &model.Model{
		ModelID:         modelID,
		UserID:          userID,
		OrgID:           orgID,
		TrainingRunID:   trainingRunID,
		DatasetID:       datasetID,
		Name:            modelName,
		ModelVersion:    modelVersion,
		BaseModel:       strings.TrimSpace(payload.GetBaseModel()),
		MetricsMetadata: "{}",
		FailureReason:   strings.TrimSpace(payload.GetFailureReason()),
	}
	if err := validateFailedModelEvent(failedModel); err != nil {
		return nil, uuid.Nil, err
	}
	return failedModel, trainingRunID, nil
}

func promotionReportReadyToModel(resourceKey uuid.UUID, payload *trainingpb.PromotionReportReadyEvent) (model.PromotionReportResult, uuid.UUID, error) {
	log.Trace("promotionReportReadyToModel")

	if payload == nil {
		return model.PromotionReportResult{}, uuid.Nil, fmt.Errorf("promotion report ready payload is required")
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return model.PromotionReportResult{}, uuid.Nil, err
	}
	if modelID != resourceKey {
		return model.PromotionReportResult{}, uuid.Nil, fmt.Errorf("model id %s does not match resource key %s", modelID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return model.PromotionReportResult{}, uuid.Nil, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.PromotionReportResult{}, uuid.Nil, err
	}
	trainingRunID, err := msgConn.ParseUUID("training_run_id", payload.GetTrainingRunId())
	if err != nil {
		return model.PromotionReportResult{}, uuid.Nil, err
	}
	reportURI, err := requiredTrainingEventString("promotion report uri", payload.GetPromotionReportUri())
	if err != nil {
		return model.PromotionReportResult{}, uuid.Nil, err
	}
	deltas, err := promotionDeltasFromEvent(payload.GetPromotionDeltas())
	if err != nil {
		return model.PromotionReportResult{}, uuid.Nil, err
	}
	return model.PromotionReportResult{
		UserID:              userID,
		OrgID:               orgID,
		ModelID:             modelID,
		TrainingRunID:       trainingRunID,
		PromotionReportURI:  reportURI,
		DeepchecksPassed:    payload.GetDeepchecksPassed(),
		DeepchecksReportURI: strings.TrimSpace(payload.GetDeepchecksReportUri()),
		EvidentlyPassed:     payload.GetEvidentlyPassed(),
		EvidentlyReportURI:  strings.TrimSpace(payload.GetEvidentlyReportUri()),
		Deltas:              deltas,
		FailureReason:       strings.TrimSpace(payload.GetFailureReason()),
	}, modelID, nil
}

func promotionDeltasFromEvent(raw string) (map[string]float64, error) {
	log.Trace("promotionDeltasFromEvent")

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]float64{}, nil
	}
	deltas := map[string]float64{}
	if err := json.Unmarshal([]byte(raw), &deltas); err != nil {
		return nil, fmt.Errorf("promotion deltas are invalid: %w", err)
	}
	return deltas, nil
}

func validateCompletedModelEvent(trainedModel *model.Model) error {
	log.Trace("validateCompletedModelEvent")

	if strings.TrimSpace(trainedModel.BaseModel) == "" {
		return fmt.Errorf("base model is required")
	}
	if strings.TrimSpace(trainedModel.Name) == "" {
		return fmt.Errorf("model name is required")
	}
	if trainedModel.ModelVersion <= 0 {
		return fmt.Errorf("model version is required")
	}
	if strings.TrimSpace(trainedModel.ArtifactLocation) == "" {
		return fmt.Errorf("artifact location is required")
	}
	if strings.TrimSpace(trainedModel.ArtifactFormat) == "" {
		return fmt.Errorf("artifact format is required")
	}
	if strings.TrimSpace(trainedModel.AdapterURI) == "" {
		return fmt.Errorf("adapter uri is required")
	}
	if strings.TrimSpace(trainedModel.MetricsMetadata) == "" {
		return fmt.Errorf("metrics metadata is required")
	}
	return nil
}

func validateFailedModelEvent(failedModel *model.Model) error {
	log.Trace("validateFailedModelEvent")

	if strings.TrimSpace(failedModel.BaseModel) == "" {
		return fmt.Errorf("base model is required")
	}
	if strings.TrimSpace(failedModel.Name) == "" {
		return fmt.Errorf("model name is required")
	}
	if failedModel.ModelVersion <= 0 {
		return fmt.Errorf("model version is required")
	}
	if strings.TrimSpace(failedModel.FailureReason) == "" {
		return fmt.Errorf("failure reason is required")
	}
	return nil
}

func parseTrainingEventIDs(resourceKey uuid.UUID, trainingRunIDRaw string, datasetIDRaw string) (uuid.UUID, uuid.UUID, error) {
	log.Trace("parseTrainingEventIDs")

	if resourceKey == uuid.Nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	trainingRunID, err := msgConn.ParseUUID("training_run_id", trainingRunIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", datasetIDRaw)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return uuid.Nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	return trainingRunID, datasetID, nil
}

func modelVersionFromEvent(modelVersionRaw string) (int, error) {
	log.Trace("modelVersionFromEvent")

	candidate := strings.TrimSpace(modelVersionRaw)
	candidate = strings.TrimPrefix(candidate, "dataset-v")
	candidate = strings.TrimPrefix(candidate, "v")
	if candidate == "" {
		return 0, fmt.Errorf("model version is required")
	}
	value, err := strconv.Atoi(candidate)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("model version is invalid")
	}
	return value, nil
}

func requiredTrainingEventString(fieldName string, value string) (string, error) {
	log.Trace("requiredTrainingEventString")

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	return value, nil
}
