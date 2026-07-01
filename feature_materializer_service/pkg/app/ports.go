package app

import (
	"context"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
)

type RawSnapshotRepository interface {
	SavePendingRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
	MarkRawReady(ctx context.Context, rawSnapshot *model.RawSnapshot) error
	MarkRawFailed(ctx context.Context, rawSnapshotID uuid.UUID, reason string) error
	ReadRawByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
}

type RawSnapshotWriter interface {
	WriteRawSnapshot(context.Context, *model.DatasetFile, *model.RawSnapshot) (*model.RawSnapshot, error)
}

type FeatureSnapshotRepository interface {
	SavePendingFeatureSnapshot(ctx context.Context, rawSnapshotID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
	MarkFeatureReady(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error
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
	SavePendingEmbeddingSnapshot(ctx context.Context, featureSnapshotID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error)
	MarkEmbeddingReady(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot) error
	MarkEmbeddingFailed(ctx context.Context, embeddingSnapshotID uuid.UUID, reason string) error
	ReadEmbeddingByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error)
}

type EmbeddingSearchRepository interface {
	ReadActiveEmbeddingSnapshot(ctx context.Context, datasetID uuid.UUID) (*model.EmbeddingSnapshot, error)
	SearchEmbeddingRecords(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot, queryVector []float32, topK int) ([]model.EmbeddingRecord, error)
}

type EmbeddingWriter interface {
	MaterializeEmbeddings(context.Context, *model.FeatureSnapshot, *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, error)
}

type FeatureSnapshotReader interface {
	ReadFeatureSnapshot(ctx context.Context, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error)
}
