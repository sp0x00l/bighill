package app

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type EmbeddingMaterializationUsecase interface {
	MaterializeEmbeddings(ctx context.Context, featureSnapshotID uuid.UUID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error)
}

type embeddingMaterializationUsecase struct {
	repo          EmbeddingSnapshotRepository
	unitOfWork    SnapshotUnitOfWorkAdapter
	eventBuilder  SnapshotEventBuilder
	featureReader FeatureSnapshotReader
	writer        EmbeddingWriter
}

func NewEmbeddingMaterializationUsecase(repo EmbeddingSnapshotRepository, unitOfWork SnapshotUnitOfWorkAdapter, eventBuilder SnapshotEventBuilder, featureReader FeatureSnapshotReader, writer EmbeddingWriter) EmbeddingMaterializationUsecase {
	log.Trace("NewEmbeddingMaterializationUsecase")

	return &embeddingMaterializationUsecase{
		repo:          repo,
		unitOfWork:    unitOfWork,
		eventBuilder:  eventBuilder,
		featureReader: featureReader,
		writer:        writer,
	}
}

func (u *embeddingMaterializationUsecase) MaterializeEmbeddings(ctx context.Context, featureSnapshotID uuid.UUID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (out *model.EmbeddingSnapshot, err error) {
	log.Trace("EmbeddingMaterializationUsecase MaterializeEmbeddings")

	strategy = model.NormalizeEmbeddingStrategy(strategy)
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "embedding.materialize",
		attribute.String("feature_snapshot_id", featureSnapshotID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
		attribute.String("strategy_version", strategy.StrategyVersion),
		attribute.String("embedding_provider", strategy.EmbeddingProvider),
		attribute.String("embedding_model", strategy.EmbeddingModel),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	embeddingSnapshot, err := u.savePendingEmbeddingSnapshot(ctx, featureSnapshotID, idempotencyKey, strategy)
	if err != nil {
		if existing, ok := domain.IsEmbeddingsAlreadyMaterialized(err); ok {
			return existing, err
		}
		return nil, err
	}

	if u.featureReader == nil || u.writer == nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"embedding_snapshot_id": embeddingSnapshot.EmbeddingSnapshotID,
			"feature_snapshot_id":   featureSnapshotID,
		}).Info("embeddings accepted for future materialization")
		return embeddingSnapshot, nil
	}

	featureSnapshot, err := u.featureReader.ReadFeatureSnapshot(ctx, featureSnapshotID)
	if err != nil {
		return nil, err
	}
	materialized, records, err := u.writer.MaterializeEmbeddings(ctx, featureSnapshot, embeddingSnapshot)
	if err != nil {
		_ = u.markEmbeddingFailed(ctx, embeddingSnapshot.EmbeddingSnapshotID, err.Error())
		return nil, fmt.Errorf("%w: %w", domain.ErrEmbeddingMaterialize, err)
	}
	if materialized == nil {
		return nil, fmt.Errorf("%w: embedding writer returned nil", domain.ErrEmbeddingMaterialize)
	}
	materialized.EmbeddingSnapshotID = embeddingSnapshot.EmbeddingSnapshotID
	if err := u.markEmbeddingReady(ctx, materialized, records); err != nil {
		return nil, err
	}
	return materialized, nil
}

func (u *embeddingMaterializationUsecase) savePendingEmbeddingSnapshot(ctx context.Context, featureSnapshotID, idempotencyKey uuid.UUID, strategy model.EmbeddingStrategy) (*model.EmbeddingSnapshot, error) {
	log.Trace("EmbeddingMaterializationUsecase savePendingEmbeddingSnapshot")

	var embeddingSnapshot *model.EmbeddingSnapshot
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		out, err := u.repo.SavePendingEmbeddingSnapshot(ctx, tx, featureSnapshotID, idempotencyKey, strategy)
		if err != nil {
			return err
		}
		embeddingSnapshot = out
		return nil
	})
	return embeddingSnapshot, err
}

func (u *embeddingMaterializationUsecase) markEmbeddingReady(ctx context.Context, embeddingSnapshot *model.EmbeddingSnapshot, records []model.EmbeddingRecord) error {
	log.Trace("EmbeddingMaterializationUsecase markEmbeddingReady")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.repo.SaveEmbeddingRecords(ctx, tx, records); err != nil {
			return err
		}
		if err := u.repo.MarkEmbeddingReady(ctx, tx, embeddingSnapshot); err != nil {
			return err
		}
		if err := enqueue(u.eventBuilder.EmbeddingSnapshotReadyMessage(embeddingSnapshot)); err != nil {
			return fmt.Errorf("enqueue embedding snapshot ready: %w", err)
		}
		return nil
	})
}

func (u *embeddingMaterializationUsecase) markEmbeddingFailed(ctx context.Context, embeddingSnapshotID uuid.UUID, reason string) error {
	log.Trace("EmbeddingMaterializationUsecase markEmbeddingFailed")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.repo.MarkEmbeddingFailed(ctx, tx, embeddingSnapshotID, reason)
	})
}
