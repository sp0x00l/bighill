package model

import "github.com/google/uuid"

type FeatureSnapshot struct {
	FeatureSnapshotID uuid.UUID
	RawSnapshotID     uuid.UUID
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	StorageLocation   string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	SchemaVersion     int
	SchemaMetadata    string
	Status            SnapshotStatus
	FailureReason     string
}
