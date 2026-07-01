package messaging

import (
	"context"
	"fmt"
	"strings"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain/model"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type InferenceTopics struct {
	ModelRegistry string
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
	return s.subscriber.Subscribe(ctx, []string{s.topics.ModelRegistry})
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
	inferenceModel := &model.InferenceModel{
		ModelID:           modelID,
		TrainingRunID:     trainingRunID,
		DatasetID:         datasetID,
		Name:              withDefault(payload.GetName(), "model_"+strings.ReplaceAll(modelID.String(), "-", "_")),
		ModelVersion:      int(payload.GetModelVersion()),
		BaseModel:         strings.TrimSpace(payload.GetBaseModel()),
		ArtifactLocation:  strings.TrimSpace(payload.GetArtifactLocation()),
		ArtifactFormat:    strings.TrimSpace(payload.GetArtifactFormat()),
		ArtifactChecksum:  strings.TrimSpace(payload.GetArtifactChecksum()),
		ArtifactSizeBytes: payload.GetArtifactSizeBytes(),
		MetricsMetadata:   withDefault(payload.GetMetricsMetadata(), "{}"),
		Status:            status,
		FailureReason:     strings.TrimSpace(payload.GetFailureReason()),
	}
	if inferenceModel.ModelVersion <= 0 {
		inferenceModel.ModelVersion = 1
	}
	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		modelID.String(),
		trainingRunID.String(),
		status.String(),
		inferenceModel.ArtifactChecksum,
	}, ":")))
	return inferenceModel, idempotencyKey, nil
}

func withDefault(value, defaultValue string) string {
	log.Trace("withDefault")

	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue
	}
	return value
}
