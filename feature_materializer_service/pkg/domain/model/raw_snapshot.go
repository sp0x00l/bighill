package model

import "github.com/google/uuid"

type RawSnapshot struct {
	RawSnapshotID     uuid.UUID
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	OrgID             uuid.UUID
	StorageLocation   string
	ContentType       string
	FileExtension     string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile ProcessingProfile
	SchemaVersion     int
	SchemaMetadata    string
	Status            SnapshotStatus
	FailureReason     string
}
