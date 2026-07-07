package usecase

import (
	"context"
	"strings"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	corePagination "lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type DatasetUsecase interface {
	CreateDataset(ctx context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) error
	ReadDatasetsForUser(ctx context.Context, userID uuid.UUID, pagination corePagination.Pagination, filters []model.Filter) ([]*model.Dataset, int, error)
	ReadDatasetForUser(ctx context.Context, ID uuid.UUID, userID uuid.UUID) (*model.Dataset, error)
	DeleteDataset(ctx context.Context, ID uuid.UUID, userID uuid.UUID) error
	PublishDataset(ctx context.Context, ID uuid.UUID, userID uuid.UUID) error
	ReplaceDataset(ctx context.Context, dataset *model.Dataset) (*model.Dataset, error)
	AdvanceDatasetProcessingState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (*model.Dataset, error)
	RecordDatasetMaterialization(ctx context.Context, dataset *model.Dataset, state model.ProcessingState) (*model.Dataset, error)
	ReadDatasetTable(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, snapshotID string) (*model.Dataset, error)
}

type datasetUseCase struct {
	datasetsRepository DatasetRepositoryAdapter
	unitOfWork         DatasetUnitOfWorkAdapter
	eventBuilder       DatasetEventBuilderAdapter
	tableCatalog       DatasetTableCatalogAdapter
}

type DatasetUsecaseOption func(*datasetUseCase)

func WithDatasetTableCatalog(tableCatalog DatasetTableCatalogAdapter) DatasetUsecaseOption {
	return func(u *datasetUseCase) {
		u.tableCatalog = tableCatalog
	}
}

func NewDatasetUseCase(datasetsRepository DatasetRepositoryAdapter, unitOfWork DatasetUnitOfWorkAdapter, eventBuilder DatasetEventBuilderAdapter, opts ...DatasetUsecaseOption) *datasetUseCase {
	log.Trace("NewDatasetUseCase")

	usecase := &datasetUseCase{
		datasetsRepository: datasetsRepository,
		unitOfWork:         unitOfWork,
		eventBuilder:       eventBuilder,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(usecase)
		}
	}
	return usecase
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

	if dataset != nil {
		orgID, orgErr := requireOrgID(ctx)
		if orgErr != nil {
			return orgErr
		}
		dataset.OrgID = orgID
		ctx = ctxutil.WithActorOrg(ctx, dataset.UserID, dataset.OrgID)
	}
	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.datasetsRepository.Create(ctx, tx, dataset, idempotencyKey); err != nil {
			return err
		}
		return enqueue(u.eventBuilder.DatasetCreatedMessage(dataset))
	})
}

func (u *datasetUseCase) ReadDatasetsForUser(ctx context.Context, userID uuid.UUID, pagination corePagination.Pagination, filters []model.Filter) (datasets []*model.Dataset, total int, err error) {
	log.Trace("DatasetUseCase ReadDatasetsForUser")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_datasets_for_user",
		attribute.String("user_id", userID.String()),
		attribute.Int("filter_count", len(filters)),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	return u.datasetsRepository.Read(ctx, userID, pagination, filters)
}

func (u *datasetUseCase) ReadDatasetForUser(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (dataset *model.Dataset, err error) {
	log.Trace("DatasetUsecase ReadDatasetForUser")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_dataset_for_user",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return nil, err
	}
	return u.datasetsRepository.ReadByID(ctx, datasetID, userID)
}

func (u *datasetUseCase) ReadDatasetTable(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, snapshotID string) (dataset *model.Dataset, err error) {
	log.Trace("DatasetUsecase ReadDatasetTable")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.read_dataset_table",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
		attribute.String("snapshot_id", snapshotID),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return nil, err
	}
	dataset, err = u.datasetsRepository.ReadByID(ctx, datasetID, userID)
	if err != nil {
		return nil, err
	}
	if !isQueryableDatasetTable(dataset) {
		return nil, domainErrors.ErrValidationFailed.Extend("dataset table is not materialized")
	}
	if err := validateDatasetTableSnapshot(dataset, snapshotID); err != nil {
		return nil, err
	}
	return dataset, nil
}

func (u *datasetUseCase) DeleteDataset(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUsecase DeleteDataset")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.delete_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return err
	}
	orgID, _ := ctxutil.OrgID(ctx)
	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if err := u.datasetsRepository.Delete(ctx, tx, datasetID, userID); err != nil {
			return err
		}
		return enqueue(u.eventBuilder.DatasetDeletedMessage(datasetID, userID, orgID))
	})
}

func (u *datasetUseCase) PublishDataset(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID) (err error) {
	log.Trace("DatasetUsecase PublishDataset")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.publish_dataset",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return err
	}
	return u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.datasetsRepository.UpdatePublishedState(ctx, tx, datasetID, userID)
	})
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

	if dataset != nil {
		orgID, orgErr := requireOrgID(ctx)
		if orgErr != nil {
			return nil, orgErr
		}
		dataset.OrgID = orgID
		ctx = ctxutil.WithActorOrg(ctx, dataset.UserID, dataset.OrgID)
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var replaceErr error
		updated, replaceErr = u.datasetsRepository.Replace(ctx, tx, dataset)
		if replaceErr != nil {
			return replaceErr
		}
		return enqueue(u.eventBuilder.DatasetUpdatedMessage(updated))
	})
	return updated, err
}

func (u *datasetUseCase) AdvanceDatasetProcessingState(ctx context.Context, datasetID uuid.UUID, userID uuid.UUID, state model.ProcessingState) (updated *model.Dataset, err error) {
	log.Trace("DatasetUsecase AdvanceDatasetProcessingState")

	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.advance_processing_state",
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("user_id", userID.String()),
		attribute.String("processing_state", state.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx, err = contextForActorOrg(ctx, userID)
	if err != nil {
		return nil, err
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var changed bool
		var updateErr error
		updated, changed, updateErr = u.datasetsRepository.UpdateProcessingState(ctx, tx, datasetID, userID, state)
		if updateErr != nil {
			return updateErr
		}
		if !changed {
			return nil
		}
		return enqueue(u.eventBuilder.DatasetUpdatedMessage(updated))
	})
	return updated, err
}

func (u *datasetUseCase) RecordDatasetMaterialization(ctx context.Context, materialized *model.Dataset, state model.ProcessingState) (updated *model.Dataset, err error) {
	log.Trace("DatasetUsecase RecordDatasetMaterialization")

	var attrs []attribute.KeyValue
	if materialized != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", materialized.ID.String()),
			attribute.String("user_id", materialized.UserID.String()),
			attribute.String("processing_state", state.String()),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "data_registry_service/app", "dataset.record_materialization", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if materialized != nil {
		orgID, orgErr := requireOrgID(ctx)
		if orgErr != nil {
			return nil, orgErr
		}
		materialized.OrgID = orgID
		ctx = ctxutil.WithActorOrg(ctx, materialized.UserID, materialized.OrgID)
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var changed bool
		var updateErr error
		updated, changed, updateErr = u.datasetsRepository.RecordMaterialization(ctx, tx, materialized, state)
		if updateErr != nil {
			return updateErr
		}
		if !changed {
			return nil
		}
		return enqueue(u.eventBuilder.DatasetUpdatedMessage(updated))
	})
	return updated, err
}

func requireOrgID(ctx context.Context) (uuid.UUID, error) {
	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return uuid.Nil, domainErrors.ErrValidationFailed.Extend("org id is required")
	}
	return orgID, nil
}

func contextForActorOrg(ctx context.Context, userID uuid.UUID) (context.Context, error) {
	orgID, ok := ctxutil.OrgID(ctx)
	if !ok {
		return ctx, domainErrors.ErrValidationFailed.Extend("org id is required")
	}
	return ctxutil.WithActorOrg(ctx, userID, orgID), nil
}

func isQueryableDatasetTable(dataset *model.Dataset) bool {
	log.Trace("isQueryableDatasetTable")

	if dataset == nil {
		return false
	}
	if dataset.ProcessingState != model.DatasetProcessingFeatureMaterialized &&
		dataset.ProcessingState != model.DatasetProcessingEmbeddingsMaterialized {
		return false
	}
	return strings.TrimSpace(dataset.Location) != "" &&
		strings.TrimSpace(dataset.TableNamespace) != "" &&
		strings.TrimSpace(dataset.TableName) != ""
}

func validateDatasetTableSnapshot(dataset *model.Dataset, snapshotID string) error {
	log.Trace("validateDatasetTableSnapshot")

	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return nil
	}
	parsed, err := uuid.Parse(snapshotID)
	if err != nil || parsed == uuid.Nil {
		return domainErrors.ErrValidationFailed.Extend("snapshot_id is invalid")
	}
	if dataset.FeatureSnapshotID == parsed || dataset.EmbeddingSnapshotID == parsed {
		return nil
	}
	return domainErrors.ErrResourceNotFound.Extend("dataset snapshot was not found")
}
