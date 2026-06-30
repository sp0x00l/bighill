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
	Save(context.Context, uuid.UUID, uuid.UUID) error
	BlacklistDataset(context.Context, uuid.UUID, uuid.UUID) error
	DeleteDataset(context.Context, uuid.UUID, uuid.UUID) error
	IsValid(context.Context, uuid.UUID, uuid.UUID) (bool, error)
}

type EventPublisher interface {
	Publish(context.Context, string, messaging.Message, proto.Message) error
}
