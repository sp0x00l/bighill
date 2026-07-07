package model

import "github.com/google/uuid"

type Dataset struct {
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	OrgID             uuid.UUID
	StorageLocation   string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile string
	SchemaVersion     int
	SchemaMetadata    string
}
