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

type FeatureSnapshotUsecase interface {
	BuildFeatureSnapshot(ctx context.Context, rawSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
}

type featureSnapshotUsecase struct {
	repo      FeatureSnapshotRepository
	rawReader RawSnapshotReader
	builder   FeatureSnapshotBuilder
}

func NewFeatureSnapshotUsecase(repo FeatureSnapshotRepository, rawReader RawSnapshotReader, builder FeatureSnapshotBuilder) FeatureSnapshotUsecase {
	log.Trace("NewFeatureSnapshotUsecase")

	return &featureSnapshotUsecase{
		repo:      repo,
		rawReader: rawReader,
		builder:   builder,
	}
}

func (u *featureSnapshotUsecase) BuildFeatureSnapshot(ctx context.Context, rawSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (out *model.FeatureSnapshot, err error) {
	log.Trace("FeatureSnapshotUsecase BuildFeatureSnapshot")
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "feature_snapshot.build",
		attribute.String("raw_snapshot_id", rawSnapshotID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	featureSnapshot, err := u.repo.SavePendingFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)
	if err != nil {
		if existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err); ok {
			return existing, err
		}
		return nil, err
	}

	if u.rawReader == nil || u.builder == nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"feature_snapshot_id": featureSnapshot.FeatureSnapshotID,
			"raw_snapshot_id":     rawSnapshotID,
		}).Info("feature snapshot accepted for future build")
		return featureSnapshot, nil
	}

	rawSnapshot, err := u.rawReader.ReadRawSnapshot(ctx, rawSnapshotID)
	if err != nil {
		return nil, err
	}
	built, err := u.builder.BuildFeatureSnapshot(ctx, rawSnapshot, featureSnapshot)
	if err != nil {
		_ = u.repo.MarkFeatureFailed(ctx, featureSnapshot.FeatureSnapshotID, err.Error())
		return nil, fmt.Errorf("%w: %w", domain.ErrFeatureSnapshotBuild, err)
	}
	if built == nil {
		return nil, fmt.Errorf("%w: feature snapshot builder returned nil", domain.ErrFeatureSnapshotBuild)
	}
	built.FeatureSnapshotID = featureSnapshot.FeatureSnapshotID
	if err := u.repo.MarkFeatureReady(ctx, built); err != nil {
		return nil, err
	}
	return built, nil
}
