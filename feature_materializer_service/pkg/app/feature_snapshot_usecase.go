package app

import (
	"context"
	"errors"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type FeatureSnapshotUsecase interface {
	BuildFeatureSnapshot(ctx context.Context, rawSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error)
}

type featureSnapshotUsecase struct {
	repo         FeatureSnapshotRepository
	unitOfWork   SnapshotUnitOfWorkAdapter
	eventBuilder SnapshotEventBuilder
	rawReader    RawSnapshotReader
	builder      FeatureSnapshotBuilder
}

func NewFeatureSnapshotUsecase(repo FeatureSnapshotRepository, unitOfWork SnapshotUnitOfWorkAdapter, eventBuilder SnapshotEventBuilder, rawReader RawSnapshotReader, builder FeatureSnapshotBuilder) FeatureSnapshotUsecase {
	log.Trace("NewFeatureSnapshotUsecase")

	return &featureSnapshotUsecase{
		repo:         repo,
		unitOfWork:   unitOfWork,
		eventBuilder: eventBuilder,
		rawReader:    rawReader,
		builder:      builder,
	}
}

func (u *featureSnapshotUsecase) BuildFeatureSnapshot(ctx context.Context, rawSnapshotID uuid.UUID, idempotencyKey uuid.UUID) (out *model.FeatureSnapshot, err error) {
	log.Trace("FeatureSnapshotUsecase BuildFeatureSnapshot")
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "feature_snapshot.build",
		attribute.String("raw_snapshot_id", rawSnapshotID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	featureSnapshot, err := u.savePendingFeatureSnapshot(ctx, rawSnapshotID, idempotencyKey)
	if err != nil {
		if existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err); ok {
			return existing, err
		}
		return nil, err
	}

	rawSnapshot, err := u.rawReader.ReadRawSnapshot(ctx, rawSnapshotID)
	if err != nil {
		return nil, err
	}
	built, err := u.builder.BuildFeatureSnapshot(ctx, rawSnapshot, featureSnapshot)
	if err != nil {
		outErr := fmt.Errorf("%w: %w", domain.ErrFeatureSnapshotBuild, err)
		if markErr := u.markFeatureFailed(ctx, featureSnapshot.FeatureSnapshotID, err.Error()); markErr != nil {
			return nil, errors.Join(outErr, fmt.Errorf("mark feature snapshot failed: %w", markErr))
		}
		return nil, outErr
	}
	if built == nil {
		return nil, fmt.Errorf("%w: feature snapshot builder returned nil", domain.ErrFeatureSnapshotBuild)
	}
	built.FeatureSnapshotID = featureSnapshot.FeatureSnapshotID
	if err := u.markFeatureReady(ctx, built); err != nil {
		return nil, err
	}
	return built, nil
}

func (u *featureSnapshotUsecase) savePendingFeatureSnapshot(ctx context.Context, rawSnapshotID, idempotencyKey uuid.UUID) (*model.FeatureSnapshot, error) {
	log.Trace("FeatureSnapshotUsecase savePendingFeatureSnapshot")

	var featureSnapshot *model.FeatureSnapshot
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		out, err := u.repo.SavePendingFeatureSnapshot(ctx, tx, rawSnapshotID, idempotencyKey)
		if err != nil {
			return err
		}
		featureSnapshot = out
		return nil
	})
	return featureSnapshot, err
}

func (u *featureSnapshotUsecase) markFeatureReady(ctx context.Context, featureSnapshot *model.FeatureSnapshot) error {
	log.Trace("FeatureSnapshotUsecase markFeatureReady")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.repo.MarkFeatureReady(ctx, tx, featureSnapshot); err != nil {
			return err
		}
		msg, err := u.eventBuilder.FeatureSnapshotReadyMessage(featureSnapshot)
		if err != nil {
			return err
		}
		if err := enqueue(msg); err != nil {
			return fmt.Errorf("enqueue feature snapshot ready: %w", err)
		}
		return nil
	})
}

func (u *featureSnapshotUsecase) markFeatureFailed(ctx context.Context, featureSnapshotID uuid.UUID, reason string) error {
	log.Trace("FeatureSnapshotUsecase markFeatureFailed")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.repo.MarkFeatureFailed(ctx, tx, featureSnapshotID, reason)
	})
}
