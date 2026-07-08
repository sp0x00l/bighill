package app

import (
	"context"
	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type DatasetUsecase struct {
	datasetsRepository DatasetsRepositoryAdapter
}

func NewDatasetUseCase(datasetsRepository DatasetsRepositoryAdapter) *DatasetUsecase {
	log.Trace("NewDatasetUseCase")

	return &DatasetUsecase{
		datasetsRepository: datasetsRepository,
	}
}

func (u *DatasetUsecase) AddDataset(ctx context.Context, dataset *model.Dataset) (err error) {
	log.Trace("DatasetUsecase AddDataset")

	var attrs []attribute.KeyValue
	if dataset != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", dataset.DatasetID.String()),
			attribute.String("user_id", dataset.UserID.String()),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "dataset.add_dataset", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if dataset != nil {
		if dataset.OrgID == uuid.Nil {
			return domain.ErrValidationFailed.Extend("org id is required")
		}
		ctx = ctxutil.WithActorOrg(ctx, dataset.UserID, dataset.OrgID)
	}
	return u.datasetsRepository.Upsert(ctx, dataset)
}

func (u *DatasetUsecase) UpdateDataset(ctx context.Context, dataset *model.Dataset) (err error) {
	log.Trace("DatasetUsecase UpdateDataset")

	var attrs []attribute.KeyValue
	if dataset != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", dataset.DatasetID.String()),
			attribute.String("user_id", dataset.UserID.String()),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "dataset.update_dataset", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if dataset != nil {
		if dataset.OrgID == uuid.Nil {
			return domain.ErrValidationFailed.Extend("org id is required")
		}
		ctx = ctxutil.WithActorOrg(ctx, dataset.UserID, dataset.OrgID)
	}
	return u.datasetsRepository.Upsert(ctx, dataset)
}

func (u *DatasetUsecase) DatasetForUpload(ctx context.Context, datasetID, userID uuid.UUID) (dataset *model.Dataset, err error) {
	log.Trace("DatasetUsecase DatasetForUpload")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "dataset.dataset_for_upload",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if _, ok := ctxutil.OrgID(ctx); !ok {
		return nil, domain.ErrValidationFailed.Extend("org id is required")
	}
	return u.datasetsRepository.ReadForUpload(ctx, datasetID, userID)
}

func (u *DatasetUsecase) BlacklistDataset(ctx context.Context, datasetID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUseCase BlacklistDataset")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "dataset.blacklist_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if _, ok := ctxutil.OrgID(ctx); !ok {
		return domain.ErrValidationFailed.Extend("org id is required")
	}
	return u.datasetsRepository.BlacklistDataset(ctx, datasetID, userID)
}

func (u *DatasetUsecase) DeleteDataset(ctx context.Context, datasetID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUseCase DeleteDataset")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "dataset.delete_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if _, ok := ctxutil.OrgID(ctx); !ok {
		return domain.ErrValidationFailed.Extend("org id is required")
	}
	return u.datasetsRepository.DeleteDataset(ctx, datasetID, userID)
}
