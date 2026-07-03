package model

import (
	"time"

	"github.com/google/uuid"
)

type UploadSessionStatus string

const (
	UploadSessionPending  UploadSessionStatus = "PENDING"
	UploadSessionPromoted UploadSessionStatus = "PROMOTED"
	UploadSessionRejected UploadSessionStatus = "REJECTED"
	UploadSessionExpired  UploadSessionStatus = "EXPIRED"
)

type UploadSession struct {
	UploadID            uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	ClientNonce         string
	FileName            string
	StagingKey          string
	FinalKey            string
	StorageLocation     string
	DeclaredFormat      string
	DeclaredContentType string
	DeclaredSizeBytes   int64
	ActualSizeBytes     int64
	Checksum            string
	Status              UploadSessionStatus
	TableNamespace      string
	TableName           string
	TableFormat         string
	CatalogProvider     string
	ProcessingProfile   string
	CreatedAt           time.Time
	ExpiresAt           time.Time
}

type PresignedUploadPost struct {
	URL       string
	Fields    map[string]string
	ExpiresAt time.Time
}

type ObjectInfo struct {
	Size        int64
	ContentType string
	Checksum    string
}

type InitiateUploadSessionRequest struct {
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	ClientNonce         string
	FileName            string
	DeclaredFormat      string
	DeclaredContentType string
	DeclaredSizeBytes   int64
	TableNamespace      string
	TableName           string
	TableFormat         string
	CatalogProvider     string
	ProcessingProfile   string
}

type InitiatedUploadSession struct {
	UploadID  uuid.UUID
	URL       string
	Fields    map[string]string
	ExpiresAt time.Time
}

type CompleteUploadSessionRequest struct {
	UploadID uuid.UUID
	UserID   uuid.UUID
}
