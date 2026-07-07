package model

import (
	"time"

	"github.com/google/uuid"
)

type UploadSessionStatus string
type UploadResourceType string

const (
	UploadSessionPending  UploadSessionStatus = "PENDING"
	UploadSessionPromoted UploadSessionStatus = "PROMOTED"
	UploadSessionRejected UploadSessionStatus = "REJECTED"
	UploadSessionExpired  UploadSessionStatus = "EXPIRED"
)

const (
	UploadResourceDataFile      UploadResourceType = "DATA_FILE"
	UploadResourceModelArtifact UploadResourceType = "MODEL_ARTIFACT"
)

type UploadSession struct {
	UploadID            uuid.UUID
	ResourceType        UploadResourceType
	ResourceID          uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	OrgID               uuid.UUID
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
	ArtifactType        string
	ModelName           string
	ModelVersion        string
	BaseModel           string
	Source              string
	SourceURI           string
	ManifestLocation    string
	HFRepoID            string
	HFRevision          string
	HFCommitSHA         string
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
	OrgID               uuid.UUID
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

type InitiateModelUploadSessionRequest struct {
	ResourceID          uuid.UUID
	DatasetID           uuid.UUID
	UserID              uuid.UUID
	OrgID               uuid.UUID
	ClientNonce         string
	FileName            string
	ArtifactType        string
	ArtifactFormat      string
	DeclaredContentType string
	DeclaredSizeBytes   int64
	ModelName           string
	ModelVersion        string
	BaseModel           string
}

type OnboardHuggingFaceModelRequest struct {
	ResourceID       uuid.UUID
	DatasetID        uuid.UUID
	UserID           uuid.UUID
	OrgID            uuid.UUID
	ClientNonce      string
	RepoID           string
	Revision         string
	ModelName        string
	ModelVersion     string
	BaseModel        string
	ArtifactType     string
	ArtifactFormat   string
	HuggingFaceToken string
}

type OnboardedModelArtifact struct {
	ResourceID        uuid.UUID
	StorageLocation   string
	ManifestLocation  string
	ArtifactType      string
	ArtifactFormat    string
	ArtifactSizeBytes int64
	ArtifactChecksum  string
	ModelName         string
	ModelVersion      string
	BaseModel         string
	SourceURI         string
	HFRepoID          string
	HFRevision        string
	HFCommitSHA       string
}

type InitiatedUploadSession struct {
	UploadID   uuid.UUID
	ResourceID uuid.UUID
	URL        string
	Fields     map[string]string
	ExpiresAt  time.Time
}

type CompleteUploadSessionRequest struct {
	UploadID uuid.UUID
	UserID   uuid.UUID
	OrgID    uuid.UUID
}
