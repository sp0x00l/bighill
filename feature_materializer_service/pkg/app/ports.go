package app

import (
	"context"
	"feature_materializer_service/pkg/domain/model"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type RawSnapshotRepository interface {
	SavePendingRawSnapshot(ctx context.Context, tx pgx.Tx, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
	MarkRawReady(ctx context.Context, tx pgx.Tx, rawSnapshot *model.RawSnapshot) error
	MarkRawFailed(ctx context.Context, tx pgx.Tx, rawSnapshotID uuid.UUID, reason string) error
	ReadRawByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
}

type RawSnapshotWriter interface {
	WriteRawSnapshot(context.Context, *model.DatasetFile, *model.RawSnapshot) (*model.RawSnapshot, error)
}

type FeatureSnapshotRepository interface {
	SavePendingFeatureSnapshot(ctx context.Context, tx pgx.Tx, rawSnapshotID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
	MarkFeatureReady(ctx context.Context, tx pgx.Tx, featureSnapshot *model.FeatureSnapshot) error
	MarkFeatureFailed(ctx context.Context, tx pgx.Tx, featureSnapshotID uuid.UUID, reason string) error
	ReadFeatureByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
}

type FeatureSnapshotBuilder interface {
	BuildFeatureSnapshot(context.Context, *model.RawSnapshot, *model.FeatureSnapshot) (*model.FeatureSnapshot, error)
}

type RawSnapshotReader interface {
	ReadRawSnapshot(ctx context.Context, rawSnapshotID uuid.UUID) (*model.RawSnapshot, error)
}

type EmbeddingSnapshotRepository interface {
	SavePendingEmbeddingSnapshot(ctx context.Context, tx pgx.Tx, featureSnapshotID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error)
	SaveEmbeddingRecords(ctx context.Context, tx pgx.Tx, records []model.EmbeddingRecord) error
	MarkEmbeddingReady(ctx context.Context, tx pgx.Tx, embeddingSnapshot *model.EmbeddingSnapshot) error
	MarkEmbeddingFailed(ctx context.Context, tx pgx.Tx, embeddingSnapshotID uuid.UUID, reason string) error
	ReadEmbeddingByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error)
}

type GraphSnapshotRepository interface {
	SavePendingGraphSnapshot(ctx context.Context, tx pgx.Tx, embeddingSnapshotID, idempotencyKey uuid.UUID, strategy model.GraphExtractionStrategy) (*model.GraphSnapshot, error)
	ReadEmbeddingChunks(ctx context.Context, embeddingSnapshotID uuid.UUID) ([]model.GraphChunk, error)
	SaveGraphMaterialization(ctx context.Context, tx pgx.Tx, materialization *model.GraphMaterialization) error
	MarkGraphReady(ctx context.Context, tx pgx.Tx, graphSnapshot *model.GraphSnapshot) error
	MarkGraphFailed(ctx context.Context, tx pgx.Tx, graphSnapshotID uuid.UUID, reason string) error
	ReadGraphByIdempotencyKey(ctx context.Context, idempotencyKey uuid.UUID) (*model.GraphSnapshot, error)
}

type SnapshotUnitOfWorkAdapter interface {
	Do(ctx context.Context, fn shareduow.TxFunc) error
}

type SnapshotEventBuilder interface {
	RawSnapshotReadyMessage(rawSnapshot *model.RawSnapshot) (shareduow.OutboundMessage, error)
	FeatureSnapshotReadyMessage(featureSnapshot *model.FeatureSnapshot) (shareduow.OutboundMessage, error)
	EmbeddingSnapshotReadyMessage(embeddingSnapshot *model.EmbeddingSnapshot) (shareduow.OutboundMessage, error)
	GraphSnapshotReadyMessage(graphSnapshot *model.GraphSnapshot) (shareduow.OutboundMessage, error)
}

type EmbeddingSearchRepository interface {
	ReadActiveEmbeddingSnapshot(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.EmbeddingSnapshot, error)
	SearchEmbeddingRecords(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot, queryVector []float32, topK int) ([]model.EmbeddingRecord, error)
}

type GraphSearchRepository interface {
	ReadActiveGraphSnapshot(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID) (*model.GraphSnapshot, error)
	SearchGraph(ctx context.Context, graphSnapshot *model.GraphSnapshot, queryText string, topK int, maxHops int) (*model.GraphSearchResult, error)
}

type EmbeddingWriter interface {
	MaterializeEmbeddings(context.Context, *model.FeatureSnapshot, *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, []model.EmbeddingRecord, error)
}

type GraphExtractor interface {
	ExtractGraph(context.Context, []model.GraphChunk, model.GraphExtractionStrategy) (*model.GraphExtraction, error)
}

type FeatureSnapshotReader interface {
	ReadFeatureSnapshot(ctx context.Context, featureSnapshotID uuid.UUID) (*model.FeatureSnapshot, error)
}

type UserEventPublisher interface {
	Publish(ctx context.Context, event userevents.Event) error
}
