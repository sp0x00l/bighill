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

type RawSnapshotUsecase interface {
	MaterializeRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (*model.RawSnapshot, error)
}

type rawSnapshotUsecase struct {
	repo   RawSnapshotRepository
	writer RawSnapshotWriter
}

func NewRawSnapshotUsecase(repo RawSnapshotRepository, writer RawSnapshotWriter) RawSnapshotUsecase {
	log.Trace("NewRawSnapshotUsecase")

	return &rawSnapshotUsecase{
		repo:   repo,
		writer: writer,
	}
}

func (u *rawSnapshotUsecase) MaterializeRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, idempotencyKey uuid.UUID) (out *model.RawSnapshot, err error) {
	log.Trace("RawSnapshotUsecase MaterializeRawSnapshot")
	ctx, span := startFeatureMaterializerSpan(ctx, "feature_materializer_service/app", "raw_snapshot.materialize",
		attribute.String("dataset_id", datasetFile.DatasetID.String()),
		attribute.String("user_id", datasetFile.UserID.String()),
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer endFeatureMaterializerSpanOnReturn(ctx, span, &err)

	rawSnapshot, err := u.repo.SavePendingRawSnapshot(ctx, datasetFile, idempotencyKey)
	if err != nil {
		if existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err); ok {
			return existing, err
		}
		return nil, err
	}

	if u.writer == nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"raw_snapshot_id": rawSnapshot.RawSnapshotID,
			"dataset_id":      rawSnapshot.DatasetID,
		}).Info("raw snapshot accepted for future materialization")
		return rawSnapshot, nil
	}

	written, err := u.writer.WriteRawSnapshot(ctx, datasetFile, rawSnapshot)
	if err != nil {
		_ = u.repo.MarkRawFailed(ctx, rawSnapshot.RawSnapshotID, err.Error())
		return nil, fmt.Errorf("%w: %w", domain.ErrRawSnapshotMaterialize, err)
	}
	if written == nil {
		return nil, fmt.Errorf("%w: raw snapshot writer returned nil", domain.ErrRawSnapshotMaterialize)
	}
	written.RawSnapshotID = rawSnapshot.RawSnapshotID
	if err := u.repo.MarkRawReady(ctx, written); err != nil {
		return nil, err
	}
	return written, nil
}
