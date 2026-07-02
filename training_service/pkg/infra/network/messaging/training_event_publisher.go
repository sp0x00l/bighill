package messaging

import (
	"context"
	"fmt"

	"training_service/pkg/domain/model"

	trainingpb "lib/data_contracts_lib/training"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type TrainingEventPublisher interface {
	PublishModelTrainingCompleted(ctx context.Context, result *model.TrainingRunResult) error
	PublishModelTrainingFailed(ctx context.Context, result *model.TrainingRunResult) error
}

type trainingEventPublisher struct {
	publisher msgConn.Publisher
	topics    TrainingTopics
}

func NewTrainingEventPublisher(publisher msgConn.Publisher, topics TrainingTopics) TrainingEventPublisher {
	log.Trace("NewTrainingEventPublisher")

	return &trainingEventPublisher{
		publisher: publisher,
		topics:    topics,
	}
}

func (p *trainingEventPublisher) PublishModelTrainingCompleted(ctx context.Context, result *model.TrainingRunResult) error {
	log.Trace("trainingEventPublisher PublishModelTrainingCompleted")

	if result == nil {
		return msgConn.NonRetryable(fmt.Errorf("training result is required"))
	}
	datasetID, err := uuid.Parse(result.DatasetID)
	if err != nil || datasetID == uuid.Nil {
		return msgConn.NonRetryable(fmt.Errorf("dataset id is invalid: %w", err))
	}
	modelID, err := uuid.Parse(result.ModelID)
	if err != nil || modelID == uuid.Nil {
		return msgConn.NonRetryable(fmt.Errorf("model id is invalid: %w", err))
	}
	return p.publish(ctx, datasetID, msgConn.MsgTypeModelTrainingCompleted, &trainingpb.ModelTrainingCompletedEvent{
		TrainingRunId:     result.TrainingRunID,
		DatasetId:         result.DatasetID,
		DatasetVersion:    result.DatasetVersion,
		FeatureSnapshotId: result.FeatureSnapshotID,
		ModelId:           modelID.String(),
		ModelName:         result.ModelName,
		ModelVersion:      result.ModelVersion,
		BaseModel:         result.BaseModel,
		ArtifactLocation:  result.ModelURI,
		ArtifactFormat:    result.ArtifactFormat,
		ArtifactChecksum:  result.ArtifactChecksum,
		ArtifactSizeBytes: result.ArtifactSizeBytes,
		MetricsMetadata:   result.MetricsMetadata,
		ReportLocation:    result.ReportURI,
	})
}

func (p *trainingEventPublisher) PublishModelTrainingFailed(ctx context.Context, result *model.TrainingRunResult) error {
	log.Trace("trainingEventPublisher PublishModelTrainingFailed")

	if result == nil {
		return msgConn.NonRetryable(fmt.Errorf("training result is required"))
	}
	datasetID, err := uuid.Parse(result.DatasetID)
	if err != nil || datasetID == uuid.Nil {
		return msgConn.NonRetryable(fmt.Errorf("dataset id is invalid: %w", err))
	}
	modelID, err := uuid.Parse(result.ModelID)
	if err != nil || modelID == uuid.Nil {
		return msgConn.NonRetryable(fmt.Errorf("model id is invalid: %w", err))
	}
	return p.publish(ctx, datasetID, msgConn.MsgTypeModelTrainingFailed, &trainingpb.ModelTrainingFailedEvent{
		TrainingRunId:     result.TrainingRunID,
		DatasetId:         result.DatasetID,
		DatasetVersion:    result.DatasetVersion,
		FeatureSnapshotId: result.FeatureSnapshotID,
		ModelId:           modelID.String(),
		ModelName:         result.ModelName,
		ModelVersion:      result.ModelVersion,
		BaseModel:         result.BaseModel,
		FailureReason:     result.FailureReason,
	})
}

func (p *trainingEventPublisher) publish(ctx context.Context, resourceKey uuid.UUID, msgType msgConn.MsgType, payload proto.Message) error {
	log.Trace("trainingEventPublisher publish")

	if p == nil || p.publisher == nil {
		return fmt.Errorf("training event publisher is required")
	}
	if p.topics.Training == "" {
		return msgConn.NonRetryable(fmt.Errorf("training topic is required"))
	}
	return p.publisher.Publish(ctx, p.topics.Training, msgConn.Message{
		ResourceKey: resourceKey,
		MsgType:     msgType,
	}, payload)
}
