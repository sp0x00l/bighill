package usecase

import (
	"context"

	"data_registry_service/pkg/domain/model"
	transport "lib/shared_lib/transport"

	"github.com/google/uuid"
)

type DatasetRepositoryAdapter interface {
	Close()
	Create(context.Context, *model.Dataset, uuid.UUID) error
	ReadPublished(context.Context, transport.Pagination, []model.Filter) ([]*model.Dataset, int, error)
	ReadPublishedByID(context.Context, uuid.UUID) (*model.Dataset, error)
	ReadPublishedByUserID(context.Context, uuid.UUID, transport.Pagination, []model.Filter) ([]*model.Dataset, int, error)
	Read(context.Context, uuid.UUID, transport.Pagination, []model.Filter) ([]*model.Dataset, int, error)
	ReadByID(context.Context, uuid.UUID, uuid.UUID) (*model.Dataset, error)
	Delete(context.Context, uuid.UUID, uuid.UUID) error
	UpdatePublishedState(context.Context, uuid.UUID, uuid.UUID) error
	UpdateProcessingState(context.Context, uuid.UUID, uuid.UUID, model.ProcessingState) (*model.Dataset, error)
	UpdateMaterializationMetadata(context.Context, *model.Dataset) (*model.Dataset, error)
	Replace(context.Context, *model.Dataset) (*model.Dataset, error)
}

type SourceRepositoryAdapter interface {
	Close()
	Create(context.Context, *model.SourceConnector, uuid.UUID) error
	ReadByUserID(context.Context, uuid.UUID) ([]model.SourceConnector, error)
	ReadByID(context.Context, uuid.UUID, uuid.UUID) (*model.SourceConnector, error)
	ReadCatalogID(context.Context, uuid.UUID, uuid.UUID) (uuid.UUID, error)
	Delete(context.Context, uuid.UUID, uuid.UUID) error
	Replace(context.Context, *model.SourceConnector) error
}

type CatalogClientAdapter interface {
	CreateResource(ctx context.Context, name string, sourceConnCfg model.ConnectorConfig) (uuid.UUID, error)
	ReplaceResource(ctx context.Context, name string, catalogID uuid.UUID, sourceConnCfg model.ConnectorConfig) error
	DeleteResource(ctx context.Context, catalogID uuid.UUID) error
}
