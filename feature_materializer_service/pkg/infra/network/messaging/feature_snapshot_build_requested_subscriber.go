package messaging

import (
	"context"
	"errors"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type FeatureSnapshotBuildRequestedListener interface {
	BuildFeatureSnapshot(ctx context.Context, rawSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
}

type featureSnapshotBuildRequestedEventListener struct {
	listener  FeatureSnapshotBuildRequestedListener
	publisher MaterializationEventPublisher
}

func NewFeatureSnapshotBuildRequestedEventListener(listener FeatureSnapshotBuildRequestedListener, publisher MaterializationEventPublisher) *featureSnapshotBuildRequestedEventListener {
	log.Trace("NewFeatureSnapshotBuildRequestedEventListener")

	return &featureSnapshotBuildRequestedEventListener{
		listener:  listener,
		publisher: publisher,
	}
}

func (l *featureSnapshotBuildRequestedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeFeatureSnapshotBuildRequested
}

func (l *featureSnapshotBuildRequestedEventListener) NewMessage() *featurepb.FeatureSnapshotBuildRequestedEvent {
	return &featurepb.FeatureSnapshotBuildRequestedEvent{}
}

func (l *featureSnapshotBuildRequestedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.FeatureSnapshotBuildRequestedEvent) error {
	log.Trace("featureSnapshotBuildRequestedEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("feature snapshot build listener is nil"))
	}
	rawSnapshotID, idempotencyKey, err := featureBuildRequestToIDs(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}

	featureSnapshot, err := l.listener.BuildFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)
	if err != nil {
		existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err)
		if !ok {
			return err
		}
		featureSnapshot = existing
	}
	if featureSnapshot == nil {
		return msgConn.NonRetryable(fmt.Errorf("feature snapshot builder returned nil"))
	}
	if err := l.publishNext(ctx, featureSnapshot); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("publish feature snapshot materialization events: %w", err)
	}
	return nil
}

func (l *featureSnapshotBuildRequestedEventListener) publishNext(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error {
	log.Trace("featureSnapshotBuildRequestedEventListener publishNext")

	if l.publisher == nil {
		return nil
	}
	if err := l.publisher.PublishFeatureSnapshotReady(ctx, featureSnapshot); err != nil {
		return err
	}
	return l.publisher.PublishEmbeddingMaterializationRequested(ctx, featureSnapshot, embeddingSnapshotIdempotencyKey(featureSnapshot.FeatureSnapshotID))
}

func featureBuildRequestToIDs(resourceKey uuid.UUID, payload *featurepb.FeatureSnapshotBuildRequestedEvent) (uuid.UUID, uuid.UUID, error) {
	if resourceKey == uuid.Nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("feature snapshot build requested payload is required")
	}
	rawSnapshotID, err := msgConn.ParseUUID("raw_snapshot_id", payload.GetRawSnapshotId())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	idempotencyKey, err := msgConn.ParseUUID("idempotency_key", payload.GetIdempotencyKey())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return rawSnapshotID, idempotencyKey, nil
}
