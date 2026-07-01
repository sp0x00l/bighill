package model

import "github.com/google/uuid"

type DatasetFile struct {
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	StorageLocation   string
	ContentType       string
	FileExtension     string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile ProcessingProfile
}
