package app

import (
	"context"

	"data_ingestion_service/pkg/domain/model"
	messaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type BlobRepositoryAdapter interface {
	Save(context.Context, *model.DataFile) (string, error)
}

type DatasetsRepositoryAdapter interface {
	Upsert(context.Context, *model.Dataset) error
	BlacklistDataset(context.Context, uuid.UUID, uuid.UUID) error
	DeleteDataset(context.Context, uuid.UUID, uuid.UUID) error
	ReadForUpload(context.Context, uuid.UUID, uuid.UUID) (*model.Dataset, error)
}

type EventPublisher interface {
	Publish(context.Context, string, messaging.Message, proto.Message) error
}
