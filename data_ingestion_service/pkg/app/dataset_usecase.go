package app

import (
	"context"
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

func (u *DatasetUsecase) AddDataset(ctx context.Context, datasetID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUsecase AddDataset")
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "dataset.add_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.Save(ctx, datasetID, userID)
}

func (u *DatasetUsecase) IsValidForUpload(ctx context.Context, datasetID, userID uuid.UUID) (valid bool, err error) {
	log.Trace("DatasetUsecase IsValidForUpload")
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "dataset.is_valid_for_upload",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.IsValid(ctx, datasetID, userID)
}

func (u *DatasetUsecase) BlacklistDataset(ctx context.Context, datasetID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUseCase BlacklistDataset")
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "dataset.blacklist_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.BlacklistDataset(ctx, datasetID, userID)
}

func (u *DatasetUsecase) DeleteDataset(ctx context.Context, datasetID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUseCase DeleteDataset")
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "dataset.delete_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.DeleteDataset(ctx, datasetID, userID)
}
