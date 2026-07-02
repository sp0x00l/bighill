package model

import (
	"mime/multipart"

	"github.com/google/uuid"
)

type DataFile struct {
	DatasetID         uuid.UUID
	UserID            uuid.UUID
	File              multipart.File
	ContentType       string
	Extension         string
	TableNamespace    string
	TableName         string
	TableFormat       string
	CatalogProvider   string
	ProcessingProfile string
}
