package app

import (
	"context"
	"io"
	"time"

	"ingestion_service/pkg/domain/model"
	sharedDomain "lib/shared_lib/domain"
	messaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type BlobRepositoryAdapter interface {
	Save(context.Context, *model.DataFile) (string, error)
	SignUploadPost(context.Context, *model.UploadSession, int64, time.Duration) (*model.PresignedUploadPost, error)
	HeadStagingObject(context.Context, *model.UploadSession) (*model.ObjectInfo, error)
	ReadStagingRange(context.Context, *model.UploadSession, int64, int64) ([]byte, error)
	PromoteStagedObject(context.Context, *model.UploadSession, string) (string, error)
	DeleteStagedObject(context.Context, *model.UploadSession) error
}

type UploadSessionRepositoryAdapter interface {
	CreateUploadSession(context.Context, *model.UploadSession) (*model.UploadSession, error)
	ReadUploadSessionForComplete(context.Context, uuid.UUID, uuid.UUID) (*model.UploadSession, error)
	PromoteUploadSession(context.Context, *model.UploadSession) (*model.UploadSession, error)
	RejectUploadSession(context.Context, uuid.UUID, uuid.UUID) error
	ExpireUploadSession(context.Context, uuid.UUID, uuid.UUID) error
	RecordUploadedFile(context.Context, *model.DataFile, string, uuid.UUID) error
	RecordModelArtifact(context.Context, *model.UploadSession) (*model.UploadSession, error)
}

type DatasetsRepositoryAdapter interface {
	Upsert(context.Context, *model.Dataset) error
	BlacklistDataset(context.Context, uuid.UUID, uuid.UUID) error
	DeleteDataset(context.Context, uuid.UUID, uuid.UUID) error
	ReadForUpload(context.Context, uuid.UUID, uuid.UUID) (*model.Dataset, error)
}

type TenantsRepositoryAdapter interface {
	Upsert(context.Context, *sharedDomain.Tenant) error
	Delete(context.Context, uuid.UUID) error
	Read(context.Context, uuid.UUID) (*sharedDomain.Tenant, error)
}

type SecretDecryptor interface {
	Decrypt(context.Context, string) (string, error)
}

type EventPublisher interface {
	Publish(context.Context, string, messaging.Message, proto.Message) error
}

type FileDetector interface {
	DetectFileFormat(ctx context.Context, file io.ReadSeeker, fileSize int, validFormats []string) string
	GetContentType(fileType string) string
}

type ModelArtifactDownloader interface {
	DownloadHuggingFaceModel(context.Context, model.OnboardHuggingFaceModelRequest) (*model.OnboardedModelArtifact, error)
}
