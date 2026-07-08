package app

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	shareduow "lib/shared_lib/uow"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type dataUploadUseCase struct {
	bucket                 BlobRepositoryAdapter
	uploadSessions         UploadSessionRepositoryAdapter
	uploadSessionUOW       UploadSessionUnitOfWorkAdapter
	uploadEventBuilder     UploadEventBuilder
	datasets               DatasetsRepositoryAdapter
	tenants                TenantsRepositoryAdapter
	huggingFaceTokenCodec  SecretDecryptor
	modelDownloader        ModelArtifactDownloader
	detector               FileDetector
	maxUploadSizeBytes     int64
	uploadSessionTTL       time.Duration
	validationReadMaxBytes int64
}

type DataUploadOption func(*dataUploadUseCase)

func WithUploadSessionRepository(repo UploadSessionRepositoryAdapter) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.uploadSessions = repo
	}
}

func WithUploadSessionUnitOfWork(unitOfWork UploadSessionUnitOfWorkAdapter, eventBuilder UploadEventBuilder) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.uploadSessionUOW = unitOfWork
		u.uploadEventBuilder = eventBuilder
	}
}

func WithUploadDatasetRepository(repo DatasetsRepositoryAdapter) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.datasets = repo
	}
}

func WithUploadTenantsRepository(repo TenantsRepositoryAdapter) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.tenants = repo
	}
}

func WithHuggingFaceTokenDecryptor(decryptor SecretDecryptor) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.huggingFaceTokenCodec = decryptor
	}
}

func WithUploadFileDetector(detector FileDetector) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.detector = detector
	}
}

func WithModelArtifactDownloader(downloader ModelArtifactDownloader) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.modelDownloader = downloader
	}
}

func WithUploadPolicy(maxUploadSizeBytes int64, sessionTTL time.Duration, validationReadMaxBytes int64) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.maxUploadSizeBytes = maxUploadSizeBytes
		u.uploadSessionTTL = sessionTTL
		u.validationReadMaxBytes = validationReadMaxBytes
	}
}

func NewDataUploadUseCase(bucket BlobRepositoryAdapter, opts ...DataUploadOption) *dataUploadUseCase {
	log.Trace("NewDataUploadUseCase")

	u := &dataUploadUseCase{bucket: bucket}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
}

// UploadFile keeps the small multipart compatibility path. The repository
// records the already-promoted object and enqueues the boundary fact through the
// transactional outbox when configured.
func (u *dataUploadUseCase) UploadFile(ctx context.Context, upload *model.DataFile) (err error) {
	log.Trace("DataUploadUseCase UploadFile")

	var attrs []attribute.KeyValue
	if upload != nil {
		attrs = append(attrs,
			attribute.String("dataset_id", upload.DatasetID.String()),
			attribute.String("user_id", upload.UserID.String()),
			attribute.String("content_type", upload.ContentType),
			attribute.String("extension", upload.Extension),
		)
	}
	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "data_upload.upload_file", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if upload != nil {
		if upload.OrgID == uuid.Nil {
			return domain.ErrValidationFailed.Extend("org id is required")
		}
		ctx = ctxutil.WithActorOrg(ctx, upload.UserID, upload.OrgID)
	}
	storageLocation, err := u.bucket.Save(ctx, upload)
	if err != nil {
		return err
	}
	return u.recordUploadedFile(ctx, upload, storageLocation, uuid.Nil)
}

func (u *dataUploadUseCase) InitiateUploadSession(ctx context.Context, request model.InitiateUploadSessionRequest) (result *model.InitiatedUploadSession, err error) {
	log.Trace("DataUploadUseCase InitiateUploadSession")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "data_upload.initiate_upload_session",
		attribute.String("dataset_id", request.DatasetID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("declared_format", request.DeclaredFormat),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if request.OrgID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("org id is required")
	}
	ctx = ctxutil.WithActorOrg(ctx, request.UserID, request.OrgID)
	now := time.Now().UTC()
	uploadID, err := u.reserveID(ctx)
	if err != nil {
		return nil, err
	}
	fileName := safeFileName(request.FileName, request.DeclaredFormat)
	session := &model.UploadSession{
		UploadID:            uploadID,
		ResourceType:        model.UploadResourceDataFile,
		ResourceID:          request.DatasetID,
		DatasetID:           request.DatasetID,
		UserID:              request.UserID,
		OrgID:               request.OrgID,
		ClientNonce:         strings.TrimSpace(request.ClientNonce),
		FileName:            fileName,
		StagingKey:          fmt.Sprintf("staging/%s/%s/%s", request.DatasetID, uploadID, fileName),
		FinalKey:            fmt.Sprintf("raw/%s/%s/%s", request.DatasetID, uploadID, fileName),
		DeclaredFormat:      normalizeFormat(request.DeclaredFormat),
		DeclaredContentType: strings.TrimSpace(request.DeclaredContentType),
		DeclaredSizeBytes:   request.DeclaredSizeBytes,
		Status:              model.UploadSessionPending,
		TableNamespace:      strings.TrimSpace(request.TableNamespace),
		TableName:           strings.TrimSpace(request.TableName),
		TableFormat:         strings.TrimSpace(request.TableFormat),
		CatalogProvider:     strings.TrimSpace(request.CatalogProvider),
		ProcessingProfile:   strings.TrimSpace(request.ProcessingProfile),
		Source:              "UPLOAD",
		CreatedAt:           now,
		ExpiresAt:           now.Add(u.uploadSessionTTL),
	}
	created, err := u.createUploadSession(ctx, session)
	if err != nil {
		return nil, err
	}
	post, err := u.bucket.SignUploadPost(ctx, created, created.DeclaredSizeBytes, time.Until(created.ExpiresAt))
	if err != nil {
		return nil, err
	}
	return &model.InitiatedUploadSession{
		UploadID:   created.UploadID,
		ResourceID: created.ResourceID,
		URL:        post.URL,
		Fields:     post.Fields,
		ExpiresAt:  created.ExpiresAt,
	}, nil
}

func (u *dataUploadUseCase) InitiateModelUploadSession(ctx context.Context, request model.InitiateModelUploadSessionRequest) (result *model.InitiatedUploadSession, err error) {
	log.Trace("DataUploadUseCase InitiateModelUploadSession")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "model_upload.initiate_upload_session",
		attribute.String("resource_id", request.ResourceID.String()),
		attribute.String("dataset_id", request.DatasetID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("artifact_type", request.ArtifactType),
		attribute.String("artifact_format", request.ArtifactFormat),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if request.OrgID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("org id is required")
	}
	ctx = ctxutil.WithActorOrg(ctx, request.UserID, request.OrgID)
	now := time.Now().UTC()
	resourceID := request.ResourceID
	if resourceID == uuid.Nil {
		resourceID, err = u.reserveID(ctx)
		if err != nil {
			return nil, err
		}
	}
	artifactFormat := normalizeFormat(request.ArtifactFormat)
	uploadID, err := u.reserveID(ctx)
	if err != nil {
		return nil, err
	}
	fileName := safeFileName(request.FileName, artifactFormat)
	session := &model.UploadSession{
		UploadID:            uploadID,
		ResourceType:        model.UploadResourceModelArtifact,
		ResourceID:          resourceID,
		DatasetID:           request.DatasetID,
		UserID:              request.UserID,
		OrgID:               request.OrgID,
		ClientNonce:         strings.TrimSpace(request.ClientNonce),
		FileName:            fileName,
		StagingKey:          fmt.Sprintf("staging/%s/%s/%s/%s", strings.ToLower(string(model.UploadResourceModelArtifact)), resourceID, uploadID, fileName),
		FinalKey:            fmt.Sprintf("models/artifacts/%s/%s/%s", resourceID, uploadID, fileName),
		DeclaredFormat:      artifactFormat,
		DeclaredContentType: strings.TrimSpace(request.DeclaredContentType),
		DeclaredSizeBytes:   request.DeclaredSizeBytes,
		Status:              model.UploadSessionPending,
		ArtifactType:        normalizeModelToken(request.ArtifactType),
		ModelName:           strings.TrimSpace(request.ModelName),
		ModelVersion:        strings.TrimSpace(request.ModelVersion),
		BaseModel:           strings.TrimSpace(request.BaseModel),
		Source:              "UPLOAD",
		CreatedAt:           now,
		ExpiresAt:           now.Add(u.uploadSessionTTL),
	}
	created, err := u.createUploadSession(ctx, session)
	if err != nil {
		return nil, err
	}
	post, err := u.bucket.SignUploadPost(ctx, created, created.DeclaredSizeBytes, time.Until(created.ExpiresAt))
	if err != nil {
		return nil, err
	}
	return &model.InitiatedUploadSession{
		UploadID:   created.UploadID,
		ResourceID: created.ResourceID,
		URL:        post.URL,
		Fields:     post.Fields,
		ExpiresAt:  created.ExpiresAt,
	}, nil
}

func (u *dataUploadUseCase) OnboardHuggingFaceModel(ctx context.Context, request model.OnboardHuggingFaceModelRequest) (session *model.UploadSession, err error) {
	log.Trace("DataUploadUseCase OnboardHuggingFaceModel")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "model_upload.onboard_huggingface",
		attribute.String("resource_id", request.ResourceID.String()),
		attribute.String("repo_id", request.RepoID),
		attribute.String("revision", request.Revision),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if request.OrgID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("org id is required")
	}
	ctx = ctxutil.WithActorOrg(ctx, request.UserID, request.OrgID)
	now := time.Now().UTC()
	artifactFormat := normalizeModelToken(request.ArtifactFormat)
	if artifactFormat == "" {
		artifactFormat = "HF_MODEL"
	}
	artifactType := normalizeModelToken(request.ArtifactType)
	if artifactType == "" {
		artifactType = "BASE_MODEL"
	}
	fileName := huggingFaceArtifactFileName(request.HuggingFaceFile, artifactFormat)
	reservedSession := &model.UploadSession{
		ResourceType:        model.UploadResourceModelArtifact,
		ResourceID:          request.ResourceID,
		DatasetID:           request.DatasetID,
		UserID:              request.UserID,
		OrgID:               request.OrgID,
		ClientNonce:         strings.TrimSpace(request.ClientNonce),
		FileName:            fileName,
		DeclaredFormat:      artifactFormat,
		DeclaredContentType: "application/octet-stream",
		Status:              model.UploadSessionPending,
		ArtifactType:        artifactType,
		ModelName:           strings.TrimSpace(request.ModelName),
		ModelVersion:        strings.TrimSpace(request.ModelVersion),
		BaseModel:           strings.TrimSpace(request.BaseModel),
		Source:              "HUGGING_FACE",
		HFRepoID:            strings.TrimSpace(request.RepoID),
		HFRevision:          strings.TrimSpace(request.Revision),
		CreatedAt:           now,
		ExpiresAt:           now.Add(u.uploadSessionTTL),
	}
	reservedSession, err = u.createUploadSession(ctx, reservedSession)
	if err != nil {
		return nil, err
	}
	if reservedSession.Status == model.UploadSessionPromoted {
		return reservedSession, nil
	}
	request.ResourceID = reservedSession.ResourceID
	tenant, err := u.tenants.Read(ctx, request.UserID)
	if err != nil {
		return nil, err
	}
	request.HuggingFaceToken, err = u.huggingFaceTokenCodec.Decrypt(ctx, tenant.HuggingFaceTokenCiphertext)
	if err != nil {
		return nil, fmt.Errorf("%w: decrypt hugging face token: %w", domain.ErrValidationFailed, err)
	}
	if strings.TrimSpace(request.HuggingFaceToken) == "" {
		return nil, domain.ErrValidationFailed.Extend("hugging face token is not set for the user")
	}
	downloaded, err := u.modelDownloader.DownloadHuggingFaceModel(ctx, request)
	if err != nil {
		return nil, err
	}
	now = time.Now().UTC()
	if downloaded.ResourceID != request.ResourceID {
		return nil, fmt.Errorf("downloaded model resource id does not match request")
	}
	session = &model.UploadSession{
		UploadID:            reservedSession.UploadID,
		ResourceType:        model.UploadResourceModelArtifact,
		ResourceID:          request.ResourceID,
		DatasetID:           request.DatasetID,
		UserID:              request.UserID,
		OrgID:               request.OrgID,
		ClientNonce:         strings.TrimSpace(request.ClientNonce),
		FileName:            huggingFaceArtifactFileName(request.HuggingFaceFile, downloaded.ArtifactFormat),
		StorageLocation:     strings.TrimSpace(downloaded.StorageLocation),
		DeclaredFormat:      normalizeModelToken(downloaded.ArtifactFormat),
		DeclaredContentType: "application/octet-stream",
		ActualSizeBytes:     downloaded.ArtifactSizeBytes,
		Checksum:            strings.TrimSpace(downloaded.ArtifactChecksum),
		Status:              model.UploadSessionPromoted,
		ArtifactType:        normalizeModelToken(downloaded.ArtifactType),
		ModelName:           strings.TrimSpace(downloaded.ModelName),
		ModelVersion:        strings.TrimSpace(downloaded.ModelVersion),
		BaseModel:           strings.TrimSpace(downloaded.BaseModel),
		Source:              "HUGGING_FACE",
		SourceURI:           strings.TrimSpace(downloaded.SourceURI),
		ManifestLocation:    strings.TrimSpace(downloaded.ManifestLocation),
		HFRepoID:            strings.TrimSpace(downloaded.HFRepoID),
		HFRevision:          strings.TrimSpace(downloaded.HFRevision),
		HFCommitSHA:         strings.TrimSpace(downloaded.HFCommitSHA),
		CreatedAt:           now,
		ExpiresAt:           now,
	}
	return u.recordModelArtifact(ctx, session)
}

func (u *dataUploadUseCase) CompleteUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest) (session *model.UploadSession, err error) {
	log.Trace("DataUploadUseCase CompleteUploadSession")
	return u.completeUploadSession(ctx, request, "")
}

func (u *dataUploadUseCase) CompleteModelUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest) (session *model.UploadSession, err error) {
	log.Trace("DataUploadUseCase CompleteModelUploadSession")
	return u.completeUploadSession(ctx, request, model.UploadResourceModelArtifact)
}

func (u *dataUploadUseCase) completeUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest, expectedResourceType model.UploadResourceType) (session *model.UploadSession, err error) {
	log.Trace("DataUploadUseCase completeUploadSession")

	ctx, span := usecasetrace.StartSpan(ctx, "ingestion_service/app", "data_upload.complete_upload_session",
		attribute.String("upload_id", request.UploadID.String()),
		attribute.String("user_id", request.UserID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if request.OrgID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("org id is required")
	}
	ctx = ctxutil.WithActorOrg(ctx, request.UserID, request.OrgID)
	session, err = u.uploadSessions.ReadUploadSessionForComplete(ctx, request.UploadID, request.UserID)
	if err != nil {
		return nil, err
	}
	if expectedResourceType != "" && session.ResourceType != expectedResourceType {
		return nil, domain.ErrValidationFailed.Extend("upload session has the wrong resource type")
	}
	if session.ResourceType == model.UploadResourceDataFile && u.datasets != nil {
		if _, err := u.datasets.ReadForUpload(ctx, session.DatasetID, session.UserID); err != nil {
			return nil, err
		}
	}
	if session.Status == model.UploadSessionPromoted {
		return session, nil
	}
	if session.Status != model.UploadSessionPending {
		return nil, domain.ErrValidationFailed.Extend("upload session is not pending")
	}
	if time.Now().UTC().After(session.ExpiresAt) {
		u.logExpireUploadSessionFailure(ctx, session)
		return nil, domain.ErrValidationFailed.Extend("upload session expired")
	}
	info, err := u.bucket.HeadStagingObject(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("head staged upload: %w", err)
	}
	if err := validateStagedObject(session, info, u.maxUploadSizeBytes); err != nil {
		u.logRejectUploadSessionFailure(ctx, session)
		u.logDeleteStagedObjectFailure(ctx, session)
		return nil, err
	}
	if session.ResourceType == model.UploadResourceDataFile {
		validationReader, err := u.stagedValidationReader(ctx, session, info.Size)
		if err != nil {
			return nil, err
		}
		detectedFormat := u.detector.DetectFileFormat(ctx, validationReader, safeIntFileSize(info.Size), []string{session.DeclaredFormat})
		if detectedFormat != session.DeclaredFormat {
			u.logRejectUploadSessionFailure(ctx, session)
			u.logDeleteStagedObjectFailure(ctx, session)
			return nil, domain.ErrValidationFailed.Extend("uploaded file format does not match the declared format")
		}
	} else if session.ResourceType == model.UploadResourceModelArtifact {
		validationReader, err := u.stagedValidationReader(ctx, session, info.Size)
		if err != nil {
			return nil, err
		}
		if err := validateModelArtifactContents(validationReader, session, info.Size); err != nil {
			u.logRejectUploadSessionFailure(ctx, session)
			u.logDeleteStagedObjectFailure(ctx, session)
			return nil, err
		}
	} else {
		return nil, domain.ErrValidationFailed.Extend("upload resource type is not supported")
	}
	checksum, err := u.stagedObjectChecksum(ctx, session, info.Size)
	if err != nil {
		return nil, err
	}
	storageLocation, err := u.bucket.PromoteStagedObject(ctx, session, session.DeclaredContentType)
	if err != nil {
		return nil, fmt.Errorf("promote staged upload: %w", err)
	}
	session.StorageLocation = storageLocation
	session.ActualSizeBytes = info.Size
	session.Checksum = checksum
	promoted, err := u.promoteUploadSession(ctx, session)
	if err != nil {
		return nil, err
	}
	_ = u.bucket.DeleteStagedObject(ctx, session)
	return promoted, nil
}

func (u *dataUploadUseCase) createUploadSession(ctx context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("DataUploadUseCase createUploadSession")

	var created *model.UploadSession
	err := u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		out, err := u.uploadSessions.CreateUploadSession(ctx, tx, session)
		if err != nil {
			return err
		}
		created = out
		return nil
	})
	return created, err
}

func (u *dataUploadUseCase) reserveID(ctx context.Context) (uuid.UUID, error) {
	log.Trace("DataUploadUseCase reserveID")

	var id uuid.UUID
	err := u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		reserved, err := u.uploadSessions.ReserveID(ctx, tx)
		if err != nil {
			return err
		}
		id = reserved
		return nil
	})
	return id, err
}

func (u *dataUploadUseCase) recordUploadedFile(ctx context.Context, upload *model.DataFile, storageLocation string, uploadID uuid.UUID) error {
	log.Trace("DataUploadUseCase recordUploadedFile")

	return u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		session, err := u.uploadSessions.RecordUploadedFile(ctx, tx, upload, storageLocation, uploadID)
		if err != nil {
			return err
		}
		if err := enqueue(u.uploadEventBuilder.DatasetFileUploadedMessage(session)); err != nil {
			return fmt.Errorf("enqueue dataset file uploaded: %w", err)
		}
		return nil
	})
}

func (u *dataUploadUseCase) promoteUploadSession(ctx context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("DataUploadUseCase promoteUploadSession")

	var promoted *model.UploadSession
	err := u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		out, changed, err := u.uploadSessions.PromoteUploadSession(ctx, tx, session)
		if err != nil {
			return err
		}
		if changed {
			if err := enqueue(u.uploadEventBuilder.UploadSessionPromotedMessage(out)); err != nil {
				return fmt.Errorf("enqueue upload promoted: %w", err)
			}
		}
		promoted = out
		return nil
	})
	return promoted, err
}

func (u *dataUploadUseCase) rejectUploadSession(ctx context.Context, uploadID, userID uuid.UUID) error {
	log.Trace("DataUploadUseCase rejectUploadSession")

	return u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.uploadSessions.RejectUploadSession(ctx, tx, uploadID, userID)
	})
}

func (u *dataUploadUseCase) expireUploadSession(ctx context.Context, uploadID, userID uuid.UUID) error {
	log.Trace("DataUploadUseCase expireUploadSession")

	return u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		return u.uploadSessions.ExpireUploadSession(ctx, tx, uploadID, userID)
	})
}

func (u *dataUploadUseCase) logRejectUploadSessionFailure(ctx context.Context, session *model.UploadSession) {
	log.Trace("DataUploadUseCase logRejectUploadSessionFailure")

	if err := u.rejectUploadSession(ctx, session.UploadID, session.UserID); err != nil {
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"upload_id": session.UploadID.String(),
			"user_id":   session.UserID.String(),
		}).Warn("failed to reject upload session during cleanup")
	}
}

func (u *dataUploadUseCase) logExpireUploadSessionFailure(ctx context.Context, session *model.UploadSession) {
	log.Trace("DataUploadUseCase logExpireUploadSessionFailure")

	if err := u.expireUploadSession(ctx, session.UploadID, session.UserID); err != nil {
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"upload_id": session.UploadID.String(),
			"user_id":   session.UserID.String(),
		}).Warn("failed to expire upload session during cleanup")
	}
}

func (u *dataUploadUseCase) logDeleteStagedObjectFailure(ctx context.Context, session *model.UploadSession) {
	log.Trace("DataUploadUseCase logDeleteStagedObjectFailure")

	if err := u.bucket.DeleteStagedObject(ctx, session); err != nil {
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"upload_id": session.UploadID.String(),
			"user_id":   session.UserID.String(),
		}).Warn("failed to delete staged object during cleanup")
	}
}

func (u *dataUploadUseCase) recordModelArtifact(ctx context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("DataUploadUseCase recordModelArtifact")

	var recorded *model.UploadSession
	err := u.uploadSessionUOW.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		out, err := u.uploadSessions.RecordModelArtifact(ctx, tx, session)
		if err != nil {
			return err
		}
		if err := enqueue(u.uploadEventBuilder.ModelArtifactIngestedMessage(out)); err != nil {
			return fmt.Errorf("enqueue model artifact ingested: %w", err)
		}
		recorded = out
		return nil
	})
	return recorded, err
}

func (u *dataUploadUseCase) stagedValidationReader(ctx context.Context, session *model.UploadSession, objectSize int64) (io.ReadSeeker, error) {
	log.Trace("DataUploadUseCase stagedValidationReader")

	if objectSize <= 0 {
		return nil, domain.ErrValidationFailed.Extend("uploaded object is empty")
	}
	readSize := minInt64(u.validationReadMaxBytes, objectSize)
	head, err := u.bucket.ReadStagingRange(ctx, session, 0, readSize)
	if err != nil {
		return nil, fmt.Errorf("read staged upload head: %w", err)
	}
	if int64(len(head)) == objectSize {
		return newRangedObjectReader(objectSize, head, 0, nil), nil
	}
	tailSize := minInt64(u.validationReadMaxBytes, objectSize)
	tailOffset := objectSize - tailSize
	tail, err := u.bucket.ReadStagingRange(ctx, session, tailOffset, tailSize)
	if err != nil {
		return nil, fmt.Errorf("read staged upload tail: %w", err)
	}
	return newRangedObjectReader(objectSize, head, tailOffset, tail), nil
}

func (u *dataUploadUseCase) stagedObjectChecksum(ctx context.Context, session *model.UploadSession, objectSize int64) (string, error) {
	log.Trace("DataUploadUseCase stagedObjectChecksum")

	if objectSize <= 0 {
		return "", domain.ErrValidationFailed.Extend("uploaded object is empty")
	}
	const chunkSize int64 = 8 * 1024 * 1024
	hash := sha256.New()
	for offset := int64(0); offset < objectSize; offset += chunkSize {
		readSize := minInt64(chunkSize, objectSize-offset)
		chunk, err := u.bucket.ReadStagingRange(ctx, session, offset, readSize)
		if err != nil {
			return "", fmt.Errorf("read staged upload for checksum: %w", err)
		}
		if int64(len(chunk)) != readSize {
			return "", fmt.Errorf("staged upload checksum read returned %d bytes, expected %d", len(chunk), readSize)
		}
		if _, err := hash.Write(chunk); err != nil {
			return "", err
		}
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func validateModelArtifactContents(reader io.ReadSeeker, session *model.UploadSession, objectSize int64) error {
	log.Trace("validateModelArtifactContents")

	switch {
	case isDirectGGUFModelArtifact(session):
		return domain.ErrValidationFailed.Extend("GGUF model artifacts must be onboarded through the Hugging Face exact-file path so the full file metadata is validated")
	case normalizeModelToken(session.DeclaredFormat) == "SAFETENSORS" || strings.HasSuffix(strings.ToLower(session.FileName), ".safetensors"):
		return domain.ErrValidationFailed.Extend("safetensors model artifacts must be uploaded as an archive with model metadata")
	case normalizeModelToken(session.DeclaredFormat) == "ZIP" || strings.HasSuffix(strings.ToLower(session.FileName), ".zip") ||
		normalizeModelToken(session.DeclaredFormat) == "HF_MODEL" || normalizeModelToken(session.DeclaredFormat) == "HF_PEFT_ADAPTER":
		return validateModelZipArchive(reader, session, objectSize)
	default:
		return domain.ErrValidationFailed.Extend("uploaded model artifact format is not positively validated")
	}
}

func isDirectGGUFModelArtifact(session *model.UploadSession) bool {
	log.Trace("isDirectGGUFModelArtifact")

	if session == nil {
		return false
	}
	format := normalizeModelToken(session.DeclaredFormat)
	return format == "GGUF" || format == "GGUF_MODEL" || format == "GGUF_LORA_ADAPTER" ||
		strings.HasSuffix(strings.ToLower(session.FileName), ".gguf")
}

func huggingFaceArtifactFileName(huggingFaceFile string, artifactFormat string) string {
	log.Trace("huggingFaceArtifactFileName")

	fileName := safeFileName(filepath.Base(strings.TrimSpace(huggingFaceFile)), artifactFormat)
	if fileName != "" && fileName != "." {
		return fileName
	}
	switch normalizeModelToken(artifactFormat) {
	case "GGUF", "GGUF_MODEL", "GGUF_LORA_ADAPTER":
		return "huggingface-model.gguf"
	default:
		return "huggingface-snapshot"
	}
}

func validateModelZipArchive(reader io.ReadSeeker, session *model.UploadSession, objectSize int64) error {
	log.Trace("validateModelZipArchive")

	names, err := readZipCentralDirectoryNames(reader, objectSize)
	if err != nil {
		return err
	}
	hasConfig := false
	hasAdapterConfig := false
	hasWeights := false
	for _, name := range names {
		lower := strings.ToLower(strings.TrimPrefix(name, "./"))
		switch {
		case lower == "config.json" || strings.HasSuffix(lower, "/config.json"):
			hasConfig = true
		case lower == "adapter_config.json" || strings.HasSuffix(lower, "/adapter_config.json"):
			hasAdapterConfig = true
		case strings.HasSuffix(lower, ".safetensors") || lower == "model.safetensors.index.json" || strings.HasSuffix(lower, "/model.safetensors.index.json"):
			hasWeights = true
		}
	}
	switch normalizeModelToken(session.ArtifactType) {
	case "LORA_ADAPTER":
		if !hasAdapterConfig || !hasWeights {
			return domain.ErrValidationFailed.Extend("uploaded LoRA archive must contain adapter_config.json and safetensors weights")
		}
	default:
		if !hasConfig || !hasWeights {
			return domain.ErrValidationFailed.Extend("uploaded model archive must contain config.json and safetensors weights")
		}
	}
	return nil
}

func readZipCentralDirectoryNames(reader io.ReadSeeker, objectSize int64) ([]string, error) {
	log.Trace("readZipCentralDirectoryNames")

	if objectSize < 22 {
		return nil, domain.ErrValidationFailed.Extend("uploaded zip model is too small")
	}
	tailSize := minInt64(4*1024*1024, objectSize)
	if _, err := reader.Seek(objectSize-tailSize, io.SeekStart); err != nil {
		return nil, err
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	eocd := -1
	for i := len(tail) - 22; i >= 0; i-- {
		if binary.LittleEndian.Uint32(tail[i:i+4]) == 0x06054b50 {
			eocd = i
			break
		}
	}
	if eocd < 0 {
		return nil, domain.ErrValidationFailed.Extend("uploaded model archive is not a valid zip")
	}
	if eocd+22 > len(tail) {
		return nil, domain.ErrValidationFailed.Extend("uploaded model archive central directory is invalid")
	}
	centralSize := int64(binary.LittleEndian.Uint32(tail[eocd+12 : eocd+16]))
	centralOffset := int64(binary.LittleEndian.Uint32(tail[eocd+16 : eocd+20]))
	tailStart := objectSize - tailSize
	if centralSize <= 0 || centralOffset < tailStart || centralOffset+centralSize > objectSize {
		return nil, domain.ErrValidationFailed.Extend("uploaded model archive central directory is too large to validate")
	}
	start := int(centralOffset - tailStart)
	end := int(start + int(centralSize))
	if start < 0 || end > len(tail) || start >= end {
		return nil, domain.ErrValidationFailed.Extend("uploaded model archive central directory is invalid")
	}
	central := tail[start:end]
	names := []string{}
	for offset := 0; offset+46 <= len(central); {
		if binary.LittleEndian.Uint32(central[offset:offset+4]) != 0x02014b50 {
			return nil, domain.ErrValidationFailed.Extend("uploaded model archive central directory entry is invalid")
		}
		nameLen := int(binary.LittleEndian.Uint16(central[offset+28 : offset+30]))
		extraLen := int(binary.LittleEndian.Uint16(central[offset+30 : offset+32]))
		commentLen := int(binary.LittleEndian.Uint16(central[offset+32 : offset+34]))
		nameStart := offset + 46
		nameEnd := nameStart + nameLen
		next := nameEnd + extraLen + commentLen
		if nameLen <= 0 || nameEnd > len(central) || next > len(central) {
			return nil, domain.ErrValidationFailed.Extend("uploaded model archive central directory entry is truncated")
		}
		names = append(names, string(central[nameStart:nameEnd]))
		offset = next
	}
	if len(names) == 0 {
		return nil, domain.ErrValidationFailed.Extend("uploaded model archive is empty")
	}
	return names, nil
}

func normalizeModelToken(value string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"))
}

func validateStagedObject(session *model.UploadSession, info *model.ObjectInfo, maxUploadSizeBytes int64) error {
	log.Trace("validateStagedObject")

	if info == nil {
		return domain.ErrValidationFailed.Extend("uploaded object not found")
	}
	if info.Size <= 0 {
		return domain.ErrValidationFailed.Extend("uploaded object is empty")
	}
	if info.Size > maxUploadSizeBytes {
		return domain.ErrValidationFailed.Extend("uploaded object is too large")
	}
	if session.DeclaredSizeBytes > 0 && info.Size > session.DeclaredSizeBytes {
		return domain.ErrValidationFailed.Extend("uploaded object exceeds declared size")
	}
	if session.ResourceType == model.UploadResourceModelArtifact && isDirectGGUFModelArtifact(session) {
		return domain.ErrValidationFailed.Extend("GGUF model artifacts must be onboarded through the Hugging Face exact-file path so the full file metadata is validated")
	}
	if strings.TrimSpace(info.ContentType) != "" && strings.TrimSpace(session.DeclaredContentType) != "" &&
		!strings.EqualFold(strings.TrimSpace(info.ContentType), strings.TrimSpace(session.DeclaredContentType)) {
		return domain.ErrValidationFailed.Extend("uploaded content type does not match the declared content type")
	}
	return nil
}

func safeFileName(fileName, declaredFormat string) string {
	log.Trace("safeFileName")

	base := filepath.Base(strings.TrimSpace(fileName))
	base = strings.ReplaceAll(base, "\\", "_")
	base = strings.ReplaceAll(base, "/", "_")
	if base == "." || base == "" {
		base = "upload"
	}
	if filepath.Ext(base) == "" && strings.TrimSpace(declaredFormat) != "" {
		base += "." + normalizeFormat(declaredFormat)
	}
	return base
}

func normalizeFormat(format string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(format), "."))
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func safeIntFileSize(size int64) int {
	if size > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(size)
}

type rangedObjectReader struct {
	size      int64
	offset    int64
	head      []byte
	tailStart int64
	tail      []byte
}

func newRangedObjectReader(size int64, head []byte, tailStart int64, tail []byte) *rangedObjectReader {
	log.Trace("newRangedObjectReader")

	return &rangedObjectReader{
		size:      size,
		head:      head,
		tailStart: tailStart,
		tail:      tail,
	}
}

func (r *rangedObjectReader) Read(p []byte) (int, error) {
	log.Trace("rangedObjectReader Read")

	if r.offset >= r.size {
		return 0, io.EOF
	}
	segment, segmentOffset, ok := r.segmentAt(r.offset)
	if !ok {
		return 0, io.EOF
	}
	n := copy(p, segment[segmentOffset:])
	r.offset += int64(n)
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (r *rangedObjectReader) Seek(offset int64, whence int) (int64, error) {
	log.Trace("rangedObjectReader Seek")

	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative seek offset")
	}
	r.offset = next
	return r.offset, nil
}

func (r *rangedObjectReader) segmentAt(offset int64) ([]byte, int, bool) {
	log.Trace("rangedObjectReader segmentAt")

	if offset >= 0 && offset < int64(len(r.head)) {
		return r.head, int(offset), true
	}
	if len(r.tail) > 0 && offset >= r.tailStart && offset < r.tailStart+int64(len(r.tail)) {
		return r.tail, int(offset - r.tailStart), true
	}
	return nil, 0, false
}
