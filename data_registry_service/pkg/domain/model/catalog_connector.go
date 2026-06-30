package model

import "github.com/google/uuid"

type ConnectorConfig interface {
	GetStorageType() StorageType
}

type SourceConnector struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	CatalogID uuid.UUID
	Config    ConnectorConfig
}
