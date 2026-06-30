package app

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type EmbeddingMaterializationUsecase interface {
	MaterializeEmbeddings(ctx context.Context, featureSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.EmbeddingSnapshot, error)
}

type embeddingMaterializationUsecase struct {
	repo          EmbeddingSnapshotRepository
	featureReader FeatureSnapshotReader
	writer        EmbeddingWriter
}

func NewEmbeddingMaterializationUsecase(repo EmbeddingSnapshotRepository, featureReader FeatureSnapshotReader, writer EmbeddingWriter) EmbeddingMaterializationUsecase {
	log.Trace("NewEmbeddingMaterializationUsecase")

	return &embeddingMaterializationUsecase{
		repo:          repo,
		featureReader: featureReader,
		writer:        writer,
	}
}

func (u *embeddingMaterializationUsecase) MaterializeEmbeddings(ctx context.Context, featureSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (out *model.EmbeddingSnapshot, err error) {
	log.Trace("EmbeddingMaterializationUsecase MaterializeEmbeddings")
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "embedding.materialize",
		attribute.String("feature_snapshot_id", featureSnapshotID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	embeddingSnapshot, err := u.repo.SavePendingEmbeddingSnapshot(ctx, featureSnapshotID, idempotencyKey)
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
	materialized, err := u.writer.MaterializeEmbeddings(ctx, featureSnapshot, embeddingSnapshot)
	if err != nil {
		_ = u.repo.MarkEmbeddingFailed(ctx, embeddingSnapshot.EmbeddingSnapshotID, err.Error())
		return nil, fmt.Errorf("%w: %w", domain.ErrEmbeddingMaterialize, err)
	}
	if materialized == nil {
		return nil, fmt.Errorf("%w: embedding writer returned nil", domain.ErrEmbeddingMaterialize)
	}
	if err := u.repo.MarkEmbeddingReady(ctx, embeddingSnapshot.EmbeddingSnapshotID, materialized.VectorStore, materialized.CollectionName); err != nil {
		return nil, err
	}
	materialized.EmbeddingSnapshotID = embeddingSnapshot.EmbeddingSnapshotID
	return materialized, nil
}
