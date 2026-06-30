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

type EmbeddingMaterializationRequestedListener interface {
	MaterializeEmbeddings(ctx context.Context, featureSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error)
}

type embeddingMaterializationRequestedEventListener struct {
	listener  EmbeddingMaterializationRequestedListener
	publisher MaterializationEventPublisher
}

func NewEmbeddingMaterializationRequestedEventListener(listener EmbeddingMaterializationRequestedListener, publisher MaterializationEventPublisher) *embeddingMaterializationRequestedEventListener {
	log.Trace("NewEmbeddingMaterializationRequestedEventListener")

	return &embeddingMaterializationRequestedEventListener{
		listener:  listener,
		publisher: publisher,
	}
}

func (l *embeddingMaterializationRequestedEventListener) MsgType() msgConn.MsgType {
	return msgConn.MsgTypeEmbeddingMaterializationRequested
}

func (l *embeddingMaterializationRequestedEventListener) NewMessage() *featurepb.EmbeddingMaterializationRequestedEvent {
	return &featurepb.EmbeddingMaterializationRequestedEvent{}
}

func (l *embeddingMaterializationRequestedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *featurepb.EmbeddingMaterializationRequestedEvent) error {
	log.Trace("embeddingMaterializationRequestedEventListener Handle")

	if l.listener == nil {
		return msgConn.NonRetryable(fmt.Errorf("embedding materialization listener is nil"))
	}
	featureSnapshotID, idempotencyKey, err := embeddingRequestToIDs(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}

	embeddingSnapshot, err := l.listener.MaterializeEmbeddings(ctx, featureSnapshotID, idempotencyKey)
	if err != nil {
		existing, ok := domain.IsEmbeddingsAlreadyMaterialized(err)
		if !ok {
			return err
		}
		embeddingSnapshot = existing
	}
	if embeddingSnapshot == nil {
		return msgConn.NonRetryable(fmt.Errorf("embedding materializer returned nil"))
	}
	if l.publisher != nil {
		if err := l.publisher.PublishEmbeddingSnapshotReady(ctx, embeddingSnapshot); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("publish embedding snapshot ready: %w", err)
		}
	}
	return nil
}

func embeddingRequestToIDs(resourceKey uuid.UUID, payload *featurepb.EmbeddingMaterializationRequestedEvent) (uuid.UUID, uuid.UUID, error) {
	if resourceKey == uuid.Nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("embedding materialization requested payload is required")
	}
	featureSnapshotID, err := msgConn.ParseUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	idempotencyKey, err := msgConn.ParseUUID("idempotency_key", payload.GetIdempotencyKey())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return featureSnapshotID, idempotencyKey, nil
}
