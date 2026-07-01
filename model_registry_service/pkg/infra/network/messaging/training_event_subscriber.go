package messaging

import (
	"context"
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

	if l.usecase == nil {
		return msgConn.NonRetryable(fmt.Errorf("model registry usecase is required"))
	}
	trainedModel, idempotencyKey, err := completedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordModelTrainingCompleted(ctx, trainedModel, idempotencyKey)
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

	if l.usecase == nil {
		return msgConn.NonRetryable(fmt.Errorf("model registry usecase is required"))
	}
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
	modelVersion := modelVersionFromEvent(payload.GetModelVersion(), payload.GetDatasetVersion())
	trainedModel := &model.Model{
		TrainingRunID:     trainingRunID,
		DatasetID:         datasetID,
		Name:              withDefault(payload.GetModelName(), "model_"+strings.ReplaceAll(datasetID.String(), "-", "_")),
		ModelVersion:      modelVersion,
		BaseModel:         strings.TrimSpace(payload.GetBaseModel()),
		ArtifactLocation:  strings.TrimSpace(payload.GetArtifactLocation()),
		ArtifactFormat:    strings.TrimSpace(payload.GetArtifactFormat()),
		ArtifactChecksum:  strings.TrimSpace(payload.GetArtifactChecksum()),
		ArtifactSizeBytes: payload.GetArtifactSizeBytes(),
		MetricsMetadata:   withDefault(payload.GetMetricsMetadata(), "{}"),
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
	failedModel := &model.Model{
		TrainingRunID: trainingRunID,
		DatasetID:     datasetID,
		Name:          withDefault(payload.GetModelName(), "model_"+strings.ReplaceAll(datasetID.String(), "-", "_")),
		ModelVersion:  modelVersionFromEvent(payload.GetModelVersion(), payload.GetDatasetVersion()),
		BaseModel:     strings.TrimSpace(payload.GetBaseModel()),
		FailureReason: strings.TrimSpace(payload.GetFailureReason()),
	}
	return failedModel, trainingRunID, nil
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

func modelVersionFromEvent(modelVersionRaw string, datasetVersionRaw string) int {
	log.Trace("modelVersionFromEvent")

	for _, candidate := range []string{modelVersionRaw, datasetVersionRaw} {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.TrimPrefix(candidate, "dataset-v")
		candidate = strings.TrimPrefix(candidate, "v")
		if candidate == "" {
			continue
		}
		value, err := strconv.Atoi(candidate)
		if err == nil && value > 0 {
			return value
		}
	}
	return 1
}

func withDefault(value, defaultValue string) string {
	log.Trace("withDefault")

	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue
	}
	return value
}
