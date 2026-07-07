package usecase

import (
	"context"

	"data_registry_service/pkg/domain/model"
	transport "lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DatasetRepositoryAdapter interface {
	Close()
	Create(context.Context, pgx.Tx, *model.Dataset, uuid.UUID) error
	Read(context.Context, uuid.UUID, transport.Pagination, []model.Filter) ([]*model.Dataset, int, error)
	ReadByID(context.Context, uuid.UUID, uuid.UUID) (*model.Dataset, error)
	Delete(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error
	UpdatePublishedState(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error
	UpdateProcessingState(context.Context, pgx.Tx, uuid.UUID, uuid.UUID, model.ProcessingState) (*model.Dataset, bool, error)
	RecordMaterialization(context.Context, pgx.Tx, *model.Dataset, model.ProcessingState) (*model.Dataset, bool, error)
	Replace(context.Context, pgx.Tx, *model.Dataset) (*model.Dataset, error)
}

type DatasetUnitOfWorkAdapter interface {
	Do(context.Context, shareduow.TxFunc) error
}

type DatasetEventBuilderAdapter interface {
	DatasetCreatedMessage(*model.Dataset) shareduow.OutboundMessage
	DatasetUpdatedMessage(*model.Dataset) shareduow.OutboundMessage
	DatasetDeletedMessage(uuid.UUID, uuid.UUID, uuid.UUID) shareduow.OutboundMessage
}

type SourceRepositoryAdapter interface {
	Close()
	ReserveID(context.Context, pgx.Tx) (uuid.UUID, error)
	Create(context.Context, pgx.Tx, *model.SourceConnector, uuid.UUID) error
	ReadByUserID(context.Context, uuid.UUID) ([]model.SourceConnector, error)
	ReadByID(context.Context, uuid.UUID, uuid.UUID) (*model.SourceConnector, error)
	ReadCatalogID(context.Context, uuid.UUID, uuid.UUID) (uuid.UUID, error)
	Delete(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error
	Replace(context.Context, pgx.Tx, *model.SourceConnector) error
}

type SourceUnitOfWorkAdapter interface {
	Do(context.Context, shareduow.TxFunc) error
}

type CatalogClientAdapter interface {
	CreateResource(ctx context.Context, name string, sourceConnCfg model.ConnectorConfig) (uuid.UUID, error)
	ReplaceResource(ctx context.Context, name string, catalogID uuid.UUID, sourceConnCfg model.ConnectorConfig) error
	DeleteResource(ctx context.Context, catalogID uuid.UUID) error
}

type DatasetTableCatalogAdapter interface {
	ValidateDatasetTable(ctx context.Context, dataset *model.Dataset) error
}
