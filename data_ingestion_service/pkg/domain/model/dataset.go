package model

import "github.com/google/uuid"

type Dataset struct {
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	StorageLocation   string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile string
	SchemaVersion     int
	SchemaMetadata    string
}
