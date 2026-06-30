package app

import (
	"context"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
)

type RawSnapshotRepository interface {
	SavePendingRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
	MarkRawReady(ctx context.Context, rawSnapshotID uuid.UUID, storageLocation string) error
	MarkRawFailed(ctx context.Context, rawSnapshotID uuid.UUID, reason string) error
	ReadRawByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
}

type RawSnapshotWriter interface {
	WriteRawSnapshot(context.Context, *model.DatasetFile, *model.RawSnapshot) (*model.RawSnapshot, error)
}

type FeatureSnapshotRepository interface {
	SavePendingFeatureSnapshot(ctx context.Context, rawSnapshotID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
	MarkFeatureReady(ctx context.Context, featureSnapshotID uuid.UUID, storageLocation string) error
	MarkFeatureFailed(ctx context.Context, featureSnapshotID uuid.UUID, reason string) error
	ReadFeatureByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
}

type FeatureSnapshotBuilder interface {
	BuildFeatureSnapshot(context.Context, *model.RawSnapshot, *model.FeatureSnapshot) (*model.FeatureSnapshot, error)
}

type RawSnapshotReader interface {
	ReadRawSnapshot(ctx context.Context, rawSnapshotID uuid.UUID) (*model.RawSnapshot, error)
}

type EmbeddingSnapshotRepository interface {
	SavePendingEmbeddingSnapshot(ctx context.Context, featureSnapshotID, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error)
	MarkEmbeddingReady(ctx context.Context, embeddingSnapshotID uuid.UUID, vectorStore, collectionName string) error
	MarkEmbeddingFailed(ctx context.Context, embeddingSnapshotID uuid.UUID, reason string) error
	ReadEmbeddingByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error)
}

type EmbeddingWriter interface {
	MaterializeEmbeddings(context.Context, *model.FeatureSnapshot, *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, error)
}

type FeatureSnapshotReader interface {
	ReadFeatureSnapshot(ctx context.Context, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error)
}
