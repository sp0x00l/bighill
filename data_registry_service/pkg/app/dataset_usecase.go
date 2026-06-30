package usecase

import (
	"context"

	"data_registry_service/pkg/domain/model"
	corePagination "lib/shared_lib/transport"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type DatasetUsecase interface {
	CreateDataset(ctx context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) error
	ReadPublishedDatasets(ctx context.Context, pagination corePagination.Pagination, filters []model.Filter) ([]*model.Dataset, int, error)
	ReadPublishedDatasetByID(ctx context.Context, ID uuid.UUID) (*model.Dataset, error)
	ReadPublishedDatasetsByUserID(ctx context.Context, userID uuid.UUID, pagination corePagination.Pagination, filters []model.Filter) ([]*model.Dataset, int, error)
	ReadDatasetsForUser(ctx context.Context, userID uuid.UUID, pagination corePagination.Pagination, filters []model.Filter) ([]*model.Dataset, int, error)
	ReadDatasetForUser(ctx context.Context, ID uuid.UUID, userID uuid.UUID) (*model.Dataset, error)
	DeleteDataset(ctx context.Context, ID uuid.UUID, userID uuid.UUID) error
	PublishDataset(ctx context.Context, ID uuid.UUID, userID uuid.UUID) error
	ReplaceDataset(ctx context.Context, dataset *model.Dataset) (*model.Dataset, error)
}

type datasetUseCase struct {
	datasetsRepository DatasetRepositoryAdapter
}

func NewDatasetUseCase(datasetsRepository DatasetRepositoryAdapter) *datasetUseCase {
	log.Trace("NewDatasetUseCase")

	return &datasetUseCase{
		datasetsRepository: datasetsRepository,
	}
}

func (u *datasetUseCase) CreateDataset(ctx context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) (err error) {
	log.Trace("DatasetUsecase CreateDataset")
	attrs := []attribute.KeyValue{attribute.String("idempotency_key", idempotencyKey.String())}
	if dataset != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", dataset.ID.String()),
			attribute.String("user_id", dataset.UserID.String()),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.create_dataset", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	model.NormalizeDatasetMetadata(dataset)

	if err := u.datasetsRepository.Create(ctx, dataset, idempotencyKey); err != nil {
		return err
	}

	return nil
}

func (u *datasetUseCase) ReadPublishedDatasets(ctx context.Context, pagination corePagination.Pagination, filters []model.Filter) (datasets []*model.Dataset, total int, err error) {
	log.Trace("DatasetUseCase ReadPublishedDatasets")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_published_datasets",
		attribute.Int("filter_count", len(filters)),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.ReadPublished(ctx, pagination, filters)
}

func (u *datasetUseCase) ReadPublishedDatasetByID(ctx context.Context, datasetID uuid.UUID) (dataset *model.Dataset, err error) {
	log.Trace("DatasetUsecase ReadPublishedDatasetByID")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_published_dataset_by_id",
		attribute.String("dataset_id", datasetID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.ReadPublishedByID(ctx, datasetID)
}

func (u *datasetUseCase) ReadPublishedDatasetsByUserID(ctx context.Context, userID uuid.UUID, pagination corePagination.Pagination, filters []model.Filter) (datasets []*model.Dataset, total int, err error) {
	log.Trace("DatasetUseCase ReadPublishedDatasetsByUserID")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_published_datasets_by_user_id",
		attribute.String("user_id", userID.String()),
		attribute.Int("filter_count", len(filters)),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.ReadPublishedByUserID(ctx, userID, pagination, filters)
}

func (u *datasetUseCase) ReadDatasetsForUser(ctx context.Context, userID uuid.UUID, pagination corePagination.Pagination, filters []model.Filter) (datasets []*model.Dataset, total int, err error) {
	log.Trace("DatasetUseCase ReadDatasetsForUser")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_datasets_for_user",
		attribute.String("user_id", userID.String()),
		attribute.Int("filter_count", len(filters)),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.Read(ctx, userID, pagination, filters)
}

func (u *datasetUseCase) ReadDatasetForUser(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (dataset *model.Dataset, err error) {
	log.Trace("DatasetUsecase ReadDatasetForUser")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_dataset_for_user",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.ReadByID(ctx, datasetID, userID)
}

func (u *datasetUseCase) DeleteDataset(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUsecase DeleteDataset")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.delete_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if err := u.datasetsRepository.Delete(ctx, datasetID, userID); err != nil {
		return err
	}

	return nil
}

func (u *datasetUseCase) PublishDataset(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUsecase PublishDataset")
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.publish_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.datasetsRepository.UpdatePublishedState(ctx, datasetID, userID)
}

func (u *datasetUseCase) ReplaceDataset(ctx context.Context, dataset *model.Dataset) (updated *model.Dataset, err error) {
	log.Trace("DatasetUsecase ReplaceDataset")
	var attrs []attribute.KeyValue
	if dataset != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", dataset.ID.String()),
			attribute.String("user_id", dataset.UserID.String()),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.replace_dataset", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	model.NormalizeDatasetMetadata(dataset)

	return u.datasetsRepository.Replace(ctx, dataset)
}
