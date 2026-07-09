package app

import (
	"context"
	"errors"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type RawSnapshotUsecase interface {
	MaterializeRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
}

type rawSnapshotUsecase struct {
	repo         RawSnapshotRepository
	unitOfWork   SnapshotUnitOfWorkAdapter
	eventBuilder SnapshotEventBuilder
	writer       RawSnapshotWriter
}

func NewRawSnapshotUsecase(repo RawSnapshotRepository, unitOfWork SnapshotUnitOfWorkAdapter, eventBuilder SnapshotEventBuilder, writer RawSnapshotWriter) RawSnapshotUsecase {
	log.Trace("NewRawSnapshotUsecase")

	return &rawSnapshotUsecase{
		repo:         repo,
		unitOfWork:   unitOfWork,
		eventBuilder: eventBuilder,
		writer:       writer,
	}
}

func (u *rawSnapshotUsecase) MaterializeRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (out *model.RawSnapshot, err error) {
	log.Trace("RawSnapshotUsecase MaterializeRawSnapshot")

	if datasetFile == nil {
		return nil, domain.ErrValidationFailed.Extend("dataset file is required")
	}
	if datasetFile.UserID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("user_id is required")
	}
	if datasetFile.OrgID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("org_id is required")
	}
	ctx = ctxutil.WithActorOrg(ctx, datasetFile.UserID, datasetFile.OrgID)
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "raw_snapshot.materialize",
		attribute.String("dataset_id", datasetFile.DatasetID.String()),
		attribute.String("user_id", datasetFile.UserID.String()),
		attribute.String("org_id", datasetFile.OrgID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	rawSnapshot, err := u.savePendingRawSnapshot(ctx, datasetFile, idempotencyKey)
	if err != nil {
		if existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err); ok {
			return existing, err
		}
		return nil, err
	}

	written, err := u.writer.WriteRawSnapshot(ctx, datasetFile, rawSnapshot)
	if err != nil {
		outErr := fmt.Errorf("%w: %w", domain.ErrRawSnapshotMaterialize, err)
		if markErr := u.markRawFailed(ctx, rawSnapshot.RawSnapshotID, err.Error()); markErr != nil {
			return nil, errors.Join(outErr, fmt.Errorf("mark raw snapshot failed: %w", markErr))
		}
		return nil, outErr
	}
	if written == nil {
		return nil, fmt.Errorf("%w: raw snapshot writer returned nil", domain.ErrRawSnapshotMaterialize)
	}
	written.RawSnapshotID = rawSnapshot.RawSnapshotID
	if err := u.markRawReady(ctx, written); err != nil {
		return nil, err
	}
	return written, nil
}

func (u *rawSnapshotUsecase) savePendingRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error) {
	log.Trace("RawSnapshotUsecase savePendingRawSnapshot")

	var rawSnapshot *model.RawSnapshot
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		out, err := u.repo.SavePendingRawSnapshot(ctx, tx, datasetFile, idempotencyKey)
		if err != nil {
			return err
		}
		rawSnapshot = out
		return nil
	})
	return rawSnapshot, err
}

func (u *rawSnapshotUsecase) markRawReady(ctx context.Context, rawSnapshot *model.RawSnapshot) error {
	log.Trace("RawSnapshotUsecase markRawReady")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.repo.MarkRawReady(ctx, tx, rawSnapshot); err != nil {
			return err
		}
		msg, err := u.eventBuilder.RawSnapshotReadyMessage(rawSnapshot)
		if err != nil {
			return err
		}
		if err := enqueue(msg); err != nil {
			return fmt.Errorf("enqueue raw snapshot ready: %w", err)
		}
		return nil
	})
}

func (u *rawSnapshotUsecase) markRawFailed(ctx context.Context, rawSnapshotID uuid.UUID, reason string) error {
	log.Trace("RawSnapshotUsecase markRawFailed")

	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.repo.MarkRawFailed(ctx, tx, rawSnapshotID, reason)
	})
}
