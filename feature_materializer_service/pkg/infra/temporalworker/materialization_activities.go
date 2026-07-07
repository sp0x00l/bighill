package temporalworker

import (
	"context"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	log "github.com/sirupsen/logrus"
)

type MaterializationActivities struct {
	rawSnapshotUsecase     usecase.RawSnapshotUsecase
	featureSnapshotUsecase usecase.FeatureSnapshotUsecase
	embeddingUsecase       usecase.EmbeddingMaterializationUsecase
}

func NewMaterializationActivities(
	rawSnapshotUsecase usecase.RawSnapshotUsecase,
	featureSnapshotUsecase usecase.FeatureSnapshotUsecase,
	embeddingUsecase usecase.EmbeddingMaterializationUsecase,
) *MaterializationActivities {
	log.Trace("NewMaterializationActivities")

	return &MaterializationActivities{
		rawSnapshotUsecase:     rawSnapshotUsecase,
		featureSnapshotUsecase: featureSnapshotUsecase,
		embeddingUsecase:       embeddingUsecase,
	}
}

func (a *MaterializationActivities) MaterializeRawSnapshot(ctx context.Context, input usecase.MaterializeRawSnapshotActivityInput) (*model.RawSnapshot, error) {
	log.Trace("MaterializationActivities MaterializeRawSnapshot")

	ctx = ctxutil.WithActorOrg(ctx, input.DatasetFile.UserID, input.DatasetFile.OrgID)
	rawSnapshot, err := a.rawSnapshotUsecase.MaterializeRawSnapshot(ctx, &input.DatasetFile, input.IdempotencyKey)
	if err != nil {
		if existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return rawSnapshot, nil
}

func (a *MaterializationActivities) BuildFeatureSnapshot(ctx context.Context, input usecase.BuildFeatureSnapshotActivityInput) (*model.FeatureSnapshot, error) {
	log.Trace("MaterializationActivities BuildFeatureSnapshot")

	ctx = ctxutil.WithActorOrg(ctx, input.UserID, input.OrgID)
	featureSnapshot, err := a.featureSnapshotUsecase.BuildFeatureSnapshot(ctx, input.RawSnapshotID, input.IdempotencyKey)
	if err != nil {
		if existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return featureSnapshot, nil
}

func (a *MaterializationActivities) MaterializeEmbeddings(ctx context.Context, input usecase.MaterializeEmbeddingsActivityInput) (*model.EmbeddingSnapshot, error) {
	log.Trace("MaterializationActivities MaterializeEmbeddings")

	ctx = ctxutil.WithActorOrg(ctx, input.UserID, input.OrgID)
	embeddingSnapshot, err := a.embeddingUsecase.MaterializeEmbeddings(ctx, input.FeatureSnapshotID, input.IdempotencyKey, input.Strategy)
	if err != nil {
		if existing, ok := domain.IsEmbeddingsAlreadyMaterialized(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return embeddingSnapshot, nil
}
