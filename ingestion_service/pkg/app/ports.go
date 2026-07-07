package app

import (
	"context"
	"io"
	"time"

	"ingestion_service/pkg/domain/model"
	sharedDomain "lib/shared_lib/domain"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	ReserveID(context.Context, pgx.Tx) (uuid.UUID, error)
	CreateUploadSession(context.Context, pgx.Tx, *model.UploadSession) (*model.UploadSession, error)
	ReadUploadSessionForComplete(context.Context, uuid.UUID, uuid.UUID) (*model.UploadSession, error)
	PromoteUploadSession(context.Context, pgx.Tx, *model.UploadSession) (*model.UploadSession, bool, error)
	RejectUploadSession(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error
	ExpireUploadSession(context.Context, pgx.Tx, uuid.UUID, uuid.UUID) error
	RecordUploadedFile(context.Context, pgx.Tx, *model.DataFile, string, uuid.UUID) (*model.UploadSession, error)
	RecordModelArtifact(context.Context, pgx.Tx, *model.UploadSession) (*model.UploadSession, error)
}

type UploadSessionUnitOfWorkAdapter interface {
	Do(ctx context.Context, fn shareduow.TxFunc) error
}

type UploadEventBuilder interface {
	DatasetFileUploadedMessage(session *model.UploadSession) shareduow.OutboundMessage
	ModelArtifactIngestedMessage(session *model.UploadSession) shareduow.OutboundMessage
	UploadSessionPromotedMessage(session *model.UploadSession) shareduow.OutboundMessage
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

type FileDetector interface {
	DetectFileFormat(ctx context.Context, file io.ReadSeeker, fileSize int, validFormats []string) string
	GetContentType(fileType string) string
}

type ModelArtifactDownloader interface {
	DownloadHuggingFaceModel(context.Context, model.OnboardHuggingFaceModelRequest) (*model.OnboardedModelArtifact, error)
}
