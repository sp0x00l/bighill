package app

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"data_ingestion_service/pkg/domain"
	"data_ingestion_service/pkg/domain/model"
	"lib/shared_lib/idem"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	defaultUploadSessionTTL        = 15 * time.Minute
	defaultUploadValidationMaxSize = 5 * 1000 * 1000
)

type dataUploadUseCase struct {
	bucket                 BlobRepositoryAdapter
	uploadSessions         UploadSessionRepositoryAdapter
	datasets               DatasetsRepositoryAdapter
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

func WithUploadDatasetRepository(repo DatasetsRepositoryAdapter) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.datasets = repo
	}
}

func WithUploadFileDetector(detector FileDetector) DataUploadOption {
	return func(u *dataUploadUseCase) {
		u.detector = detector
	}
}

func WithUploadPolicy(maxUploadSizeBytes int64, sessionTTL time.Duration, validationReadMaxBytes int64) DataUploadOption {
	return func(u *dataUploadUseCase) {
		if maxUploadSizeBytes > 0 {
			u.maxUploadSizeBytes = maxUploadSizeBytes
		}
		if sessionTTL > 0 {
			u.uploadSessionTTL = sessionTTL
		}
		if validationReadMaxBytes > 0 {
			u.validationReadMaxBytes = validationReadMaxBytes
		}
	}
}

func NewDataUploadUseCase(bucket BlobRepositoryAdapter, opts ...DataUploadOption) *dataUploadUseCase {
	log.Trace("NewDataUploadUseCase")

	u := &dataUploadUseCase{
		bucket:                 bucket,
		uploadSessionTTL:       defaultUploadSessionTTL,
		validationReadMaxBytes: defaultUploadValidationMaxSize,
	}
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
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "data_upload.upload_file", attrs...)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if u.uploadSessions == nil {
		return fmt.Errorf("upload session repository is required")
	}
	storageLocation, err := u.bucket.Save(ctx, upload)
	if err != nil {
		return err
	}
	return u.uploadSessions.RecordUploadedFile(ctx, upload, storageLocation, uuid.New())
}

func (u *dataUploadUseCase) InitiateUploadSession(ctx context.Context, request model.InitiateUploadSessionRequest) (result *model.InitiatedUploadSession, err error) {
	log.Trace("DataUploadUseCase InitiateUploadSession")
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "data_upload.initiate_upload_session",
		attribute.String("dataset_id", request.DatasetID.String()),
		attribute.String("user_id", request.UserID.String()),
		attribute.String("declared_format", request.DeclaredFormat),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if u.uploadSessions == nil {
		return nil, fmt.Errorf("upload session repository is required")
	}
	if err := validateInitiateRequest(request, u.maxUploadSizeBytes); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	uploadID := newUploadID(request.DatasetID, request.UserID, request.ClientNonce)
	fileName := safeFileName(request.FileName, request.DeclaredFormat)
	session := &model.UploadSession{
		UploadID:            uploadID,
		DatasetID:           request.DatasetID,
		UserID:              request.UserID,
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
		CreatedAt:           now,
		ExpiresAt:           now.Add(u.uploadSessionTTL),
	}
	created, err := u.uploadSessions.CreateUploadSession(ctx, session)
	if err != nil {
		return nil, err
	}
	post, err := u.bucket.SignUploadPost(ctx, created, created.DeclaredSizeBytes, time.Until(created.ExpiresAt))
	if err != nil {
		return nil, err
	}
	return &model.InitiatedUploadSession{
		UploadID:  created.UploadID,
		URL:       post.URL,
		Fields:    post.Fields,
		ExpiresAt: created.ExpiresAt,
	}, nil
}

func (u *dataUploadUseCase) CompleteUploadSession(ctx context.Context, request model.CompleteUploadSessionRequest) (session *model.UploadSession, err error) {
	log.Trace("DataUploadUseCase CompleteUploadSession")
	ctx, span := usecasetrace.StartSpan(ctx, "data_ingestion_service/app", "data_upload.complete_upload_session",
		attribute.String("upload_id", request.UploadID.String()),
		attribute.String("user_id", request.UserID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if u.uploadSessions == nil {
		return nil, fmt.Errorf("upload session repository is required")
	}
	if u.detector == nil {
		return nil, fmt.Errorf("file detector is required")
	}
	session, err = u.uploadSessions.ReadUploadSessionForComplete(ctx, request.UploadID, request.UserID)
	if err != nil {
		return nil, err
	}
	if u.datasets != nil {
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
		_ = u.uploadSessions.ExpireUploadSession(ctx, session.UploadID, session.UserID)
		return nil, domain.ErrValidationFailed.Extend("upload session expired")
	}
	info, err := u.bucket.HeadStagingObject(ctx, session)
	if err != nil {
		return nil, fmt.Errorf("head staged upload: %w", err)
	}
	if err := validateStagedObject(session, info, u.maxUploadSizeBytes); err != nil {
		_ = u.uploadSessions.RejectUploadSession(ctx, session.UploadID, session.UserID)
		_ = u.bucket.DeleteStagedObject(ctx, session)
		return nil, err
	}
	validationReader, err := u.stagedValidationReader(ctx, session, info.Size)
	if err != nil {
		return nil, err
	}
	detectedFormat := u.detector.DetectFileFormat(ctx, validationReader, safeIntFileSize(info.Size), []string{session.DeclaredFormat})
	if detectedFormat != session.DeclaredFormat {
		_ = u.uploadSessions.RejectUploadSession(ctx, session.UploadID, session.UserID)
		_ = u.bucket.DeleteStagedObject(ctx, session)
		return nil, domain.ErrValidationFailed.Extend("uploaded file format does not match the declared format")
	}
	storageLocation, err := u.bucket.PromoteStagedObject(ctx, session, session.DeclaredContentType)
	if err != nil {
		return nil, fmt.Errorf("promote staged upload: %w", err)
	}
	session.StorageLocation = storageLocation
	session.ActualSizeBytes = info.Size
	session.Checksum = strings.Trim(info.Checksum, `"`)
	promoted, err := u.uploadSessions.PromoteUploadSession(ctx, session)
	if err != nil {
		return nil, err
	}
	_ = u.bucket.DeleteStagedObject(ctx, session)
	return promoted, nil
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

func validateInitiateRequest(request model.InitiateUploadSessionRequest, maxUploadSizeBytes int64) error {
	log.Trace("validateInitiateRequest")

	if request.DatasetID == uuid.Nil || request.UserID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("dataset and user are required")
	}
	if strings.TrimSpace(request.FileName) == "" {
		return domain.ErrValidationFailed.Extend("file name is required")
	}
	if strings.TrimSpace(request.ClientNonce) == "" {
		return domain.ErrValidationFailed.Extend("client nonce is required")
	}
	if normalizeFormat(request.DeclaredFormat) == "" {
		return domain.ErrValidationFailed.Extend("declared format is required")
	}
	if strings.TrimSpace(request.DeclaredContentType) == "" {
		return domain.ErrValidationFailed.Extend("declared content type is required")
	}
	if request.DeclaredSizeBytes <= 0 {
		return domain.ErrValidationFailed.Extend("declared size must be greater than zero")
	}
	if maxUploadSizeBytes <= 0 {
		return domain.ErrValidationFailed.Extend("upload max size policy is not configured")
	}
	if request.DeclaredSizeBytes > maxUploadSizeBytes {
		return domain.ErrValidationFailed.Extend("declared size is too large")
	}
	if strings.TrimSpace(request.TableNamespace) == "" || strings.TrimSpace(request.TableName) == "" ||
		strings.TrimSpace(request.TableFormat) == "" || strings.TrimSpace(request.CatalogProvider) == "" ||
		strings.TrimSpace(request.ProcessingProfile) == "" {
		return domain.ErrValidationFailed.Extend("dataset materialization metadata is incomplete")
	}
	return nil
}

func validateStagedObject(session *model.UploadSession, info *model.ObjectInfo, maxUploadSizeBytes int64) error {
	log.Trace("validateStagedObject")

	if info == nil {
		return domain.ErrValidationFailed.Extend("uploaded object not found")
	}
	if info.Size <= 0 {
		return domain.ErrValidationFailed.Extend("uploaded object is empty")
	}
	if maxUploadSizeBytes <= 0 {
		return domain.ErrValidationFailed.Extend("upload max size policy is not configured")
	}
	if info.Size > maxUploadSizeBytes {
		return domain.ErrValidationFailed.Extend("uploaded object is too large")
	}
	if session.DeclaredSizeBytes > 0 && info.Size > session.DeclaredSizeBytes {
		return domain.ErrValidationFailed.Extend("uploaded object exceeds declared size")
	}
	if strings.TrimSpace(info.ContentType) != "" && strings.TrimSpace(session.DeclaredContentType) != "" &&
		!strings.EqualFold(strings.TrimSpace(info.ContentType), strings.TrimSpace(session.DeclaredContentType)) {
		return domain.ErrValidationFailed.Extend("uploaded content type does not match the declared content type")
	}
	return nil
}

func newUploadID(datasetID, userID uuid.UUID, clientNonce string) uuid.UUID {
	log.Trace("newUploadID")

	return idem.FromParts("data-ingestion-upload-session", datasetID.String(), userID.String(), strings.TrimSpace(clientNonce))
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
