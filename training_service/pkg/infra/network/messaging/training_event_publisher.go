package messaging

import (
	"context"
	"fmt"

	"training_service/pkg/app"
	"training_service/pkg/domain/model"

	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type trainingEventPublisher struct {
	publisher msgConn.Publisher
	builder   *TrainingEventBuilder
}

func NewTrainingEventPublisher(publisher msgConn.Publisher, topics TrainingTopics) app.TrainingEventPublisher {
	log.Trace("NewTrainingEventPublisher")

	return &trainingEventPublisher{
		publisher: publisher,
		builder:   NewTrainingEventBuilder(topics.Training),
	}
}

func (p *trainingEventPublisher) PublishModelTrainingCompleted(ctx context.Context, result *model.TrainingRunResult) error {
	log.Trace("trainingEventPublisher PublishModelTrainingCompleted")

	message, err := p.builder.ModelTrainingCompletedMessage(result)
	return p.publish(ctx, message, err)
}

func (p *trainingEventPublisher) PublishModelTrainingFailed(ctx context.Context, result *model.TrainingRunResult) error {
	log.Trace("trainingEventPublisher PublishModelTrainingFailed")

	message, err := p.builder.ModelTrainingFailedMessage(result)
	return p.publish(ctx, message, err)
}

func (p *trainingEventPublisher) PublishPromotionReportReady(ctx context.Context, report *model.PromotionReport) error {
	log.Trace("trainingEventPublisher PublishPromotionReportReady")

	message, err := p.builder.PromotionReportReadyMessage(report)
	return p.publish(ctx, message, err)
}

func (p *trainingEventPublisher) publish(ctx context.Context, message msgConn.OutboundMessage, buildErr error) error {
	log.Trace("trainingEventPublisher publish")

	if buildErr != nil {
		return msgConn.NonRetryable(buildErr)
	}
	if p == nil || p.publisher == nil {
		return fmt.Errorf("training event publisher is required")
	}
	if message.Topic == "" {
		return msgConn.NonRetryable(fmt.Errorf("training topic is required"))
	}
	return p.publisher.Publish(ctx, message.Topic, message.Message, nil)
}
