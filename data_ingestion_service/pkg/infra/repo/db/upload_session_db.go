package db

import (
	"context"
	"errors"
	"fmt"

	"data_ingestion_service/pkg/domain"
	"data_ingestion_service/pkg/domain/model"
	datasetpb "lib/data_contracts_lib/data_ingestion"
	coreDb "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type UploadSessionDB struct {
	coreDb.Database
	outbox       msgConn.OrderedOutbox
	topic        string
	outboxSignal func()
}

type UploadSessionDBOption func(*UploadSessionDB)

func WithUploadSessionOutbox(outbox msgConn.OrderedOutbox, topic string) UploadSessionDBOption {
	log.Trace("WithUploadSessionOutbox")

	return func(db *UploadSessionDB) {
		db.outbox = outbox
		db.topic = topic
	}
}

func WithUploadSessionOutboxSignal(signal func()) UploadSessionDBOption {
	log.Trace("WithUploadSessionOutboxSignal")

	return func(db *UploadSessionDB) {
		db.outboxSignal = signal
	}
}

func NewUploadSessionDB(db *coreDb.Database, opts ...UploadSessionDBOption) *UploadSessionDB {
	log.Trace("NewUploadSessionDB")

	repo := &UploadSessionDB{Database: *db}
	for _, opt := range opts {
		if opt != nil {
			opt(repo)
		}
	}
	return repo
}

func (db *UploadSessionDB) CreateUploadSession(ctx context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB CreateUploadSession")

	query := `INSERT INTO ` + db.Name + `.upload_sessions (
		upload_id, dataset_id, user_id, client_nonce, file_name, staging_key, final_key,
		declared_format, declared_content_type, declared_size_bytes, status,
		table_namespace, table_name, table_format, catalog_provider, processing_profile,
		created_at, expires_at
	) VALUES (
		@upload_id, @dataset_id, @user_id, @client_nonce, @file_name, @staging_key, @final_key,
		@declared_format, @declared_content_type, @declared_size_bytes, @status,
		@table_namespace, @table_name, @table_format, @catalog_provider, @processing_profile,
		@created_at, @expires_at
	)
	ON CONFLICT (upload_id) DO UPDATE SET upload_id = EXCLUDED.upload_id
		, expires_at = CASE
			WHEN ` + db.Name + `.upload_sessions.status IN ('PENDING', 'EXPIRED') THEN EXCLUDED.expires_at
			ELSE ` + db.Name + `.upload_sessions.expires_at
		END
		, status = CASE
			WHEN ` + db.Name + `.upload_sessions.status = 'EXPIRED' THEN 'PENDING'
			ELSE ` + db.Name + `.upload_sessions.status
		END
		, updated_at = now()
	RETURNING upload_id::text, dataset_id::text, user_id::text, client_nonce, file_name,
		staging_key, final_key, storage_location, declared_format, declared_content_type,
		declared_size_bytes, actual_size_bytes, checksum, status, table_namespace, table_name,
		table_format, catalog_provider, processing_profile, created_at, expires_at`

	out, err := scanUploadSession(db.Pool.QueryRow(ctx, query, uploadSessionDAO(session)))
	if err != nil {
		return nil, fmt.Errorf("create upload session: %w", err)
	}
	return out, nil
}

func (db *UploadSessionDB) ReadUploadSessionForComplete(ctx context.Context, uploadID, userID uuid.UUID) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB ReadUploadSessionForComplete")

	query := `SELECT upload_id::text, dataset_id::text, user_id::text, client_nonce, file_name,
		staging_key, final_key, storage_location, declared_format, declared_content_type,
		declared_size_bytes, actual_size_bytes, checksum, status, table_namespace, table_name,
		table_format, catalog_provider, processing_profile, created_at, expires_at
		FROM ` + db.Name + `.upload_sessions
		WHERE upload_id = @upload_id AND user_id = @user_id`
	session, err := scanUploadSession(db.Pool.QueryRow(ctx, query, uploadSessionIDsDAO(uploadID, userID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrResourceNotFound
		}
		return nil, fmt.Errorf("read upload session: %w", err)
	}
	return session, nil
}

func (db *UploadSessionDB) PromoteUploadSession(ctx context.Context, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB PromoteUploadSession")

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin upload promotion transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `UPDATE ` + db.Name + `.upload_sessions SET
		storage_location = @storage_location,
		actual_size_bytes = @actual_size_bytes,
		checksum = @checksum,
		status = 'PROMOTED',
		updated_at = now()
		WHERE upload_id = @upload_id AND user_id = @user_id AND status = 'PENDING'
		RETURNING upload_id::text, dataset_id::text, user_id::text, client_nonce, file_name,
			staging_key, final_key, storage_location, declared_format, declared_content_type,
			declared_size_bytes, actual_size_bytes, checksum, status, table_namespace, table_name,
			table_format, catalog_provider, processing_profile, created_at, expires_at`
	promoted, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionDAO(session)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			existing, readErr := db.readUploadSessionTx(ctx, tx, session.UploadID, session.UserID)
			if readErr != nil {
				return nil, readErr
			}
			if existing.Status == model.UploadSessionPromoted {
				if err := tx.Commit(ctx); err != nil {
					return nil, fmt.Errorf("commit already-promoted upload transaction: %w", err)
				}
				return existing, nil
			}
			return nil, domain.ErrValidationFailed.Extend("upload session is not pending")
		}
		return nil, fmt.Errorf("promote upload session: %w", err)
	}
	enqueued := false
	if db.outbox != nil {
		if err := db.outbox.EnqueueTx(ctx, tx, datasetFileUploadedMessage(db.topic, promoted)); err != nil {
			return nil, err
		}
		enqueued = true
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit upload promotion transaction: %w", err)
	}
	if enqueued {
		db.notifyOutbox()
	}
	return promoted, nil
}

func (db *UploadSessionDB) RejectUploadSession(ctx context.Context, uploadID, userID uuid.UUID) error {
	log.Trace("UploadSessionDB RejectUploadSession")

	return db.setUploadSessionStatus(ctx, uploadID, userID, model.UploadSessionRejected)
}

func (db *UploadSessionDB) ExpireUploadSession(ctx context.Context, uploadID, userID uuid.UUID) error {
	log.Trace("UploadSessionDB ExpireUploadSession")

	return db.setUploadSessionStatus(ctx, uploadID, userID, model.UploadSessionExpired)
}

func (db *UploadSessionDB) RecordUploadedFile(ctx context.Context, upload *model.DataFile, storageLocation string, uploadID uuid.UUID) error {
	log.Trace("UploadSessionDB RecordUploadedFile")

	if uploadID == uuid.Nil {
		uploadID = uuid.New()
	}
	session := &model.UploadSession{
		UploadID:            uploadID,
		DatasetID:           upload.DatasetID,
		UserID:              upload.UserID,
		FileName:            "multipart-upload." + upload.Extension,
		StorageLocation:     storageLocation,
		DeclaredFormat:      upload.Extension,
		DeclaredContentType: upload.ContentType,
		Status:              model.UploadSessionPromoted,
		TableNamespace:      upload.TableNamespace,
		TableName:           upload.TableName,
		TableFormat:         upload.TableFormat,
		CatalogProvider:     upload.CatalogProvider,
		ProcessingProfile:   upload.ProcessingProfile,
	}

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin direct upload transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO ` + db.Name + `.upload_sessions (
		upload_id, dataset_id, user_id, file_name, storage_location, declared_format,
		declared_content_type, status, table_namespace, table_name, table_format,
		catalog_provider, processing_profile, created_at, expires_at
	) VALUES (
		@upload_id, @dataset_id, @user_id, @file_name, @storage_location, @declared_format,
		@declared_content_type, 'PROMOTED', @table_namespace, @table_name, @table_format,
		@catalog_provider, @processing_profile, now(), now()
	)
	ON CONFLICT (upload_id) DO UPDATE SET upload_id = EXCLUDED.upload_id`
	if _, err := tx.Exec(ctx, query, uploadSessionDAO(session)); err != nil {
		return fmt.Errorf("record direct upload session: %w", err)
	}
	enqueued := false
	if db.outbox != nil {
		if err := db.outbox.EnqueueTx(ctx, tx, datasetFileUploadedMessage(db.topic, session)); err != nil {
			return err
		}
		enqueued = true
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit direct upload transaction: %w", err)
	}
	if enqueued {
		db.notifyOutbox()
	}
	return nil
}

func (db *UploadSessionDB) readUploadSessionTx(ctx context.Context, tx pgx.Tx, uploadID, userID uuid.UUID) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB readUploadSessionTx")

	query := `SELECT upload_id::text, dataset_id::text, user_id::text, client_nonce, file_name,
		staging_key, final_key, storage_location, declared_format, declared_content_type,
		declared_size_bytes, actual_size_bytes, checksum, status, table_namespace, table_name,
		table_format, catalog_provider, processing_profile, created_at, expires_at
		FROM ` + db.Name + `.upload_sessions
		WHERE upload_id = @upload_id AND user_id = @user_id`
	session, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionIDsDAO(uploadID, userID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrResourceNotFound
		}
		return nil, fmt.Errorf("read upload session in tx: %w", err)
	}
	return session, nil
}

func (db *UploadSessionDB) setUploadSessionStatus(ctx context.Context, uploadID, userID uuid.UUID, status model.UploadSessionStatus) error {
	log.Trace("UploadSessionDB setUploadSessionStatus")

	query := `UPDATE ` + db.Name + `.upload_sessions SET status = @status, updated_at = now()
		WHERE upload_id = @upload_id AND user_id = @user_id AND status = 'PENDING'`
	cmd, err := db.Pool.Exec(ctx, query, pgx.NamedArgs{
		"upload_id": pgtype.UUID{Bytes: uploadID, Valid: true},
		"user_id":   pgtype.UUID{Bytes: userID, Valid: true},
		"status":    string(status),
	})
	if err != nil {
		return fmt.Errorf("set upload session status: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		current, readErr := db.ReadUploadSessionForComplete(ctx, uploadID, userID)
		if readErr != nil {
			return readErr
		}
		if current.Status != model.UploadSessionPending {
			return nil
		}
		return domain.ErrResourceNotFound
	}
	return nil
}

func (db *UploadSessionDB) notifyOutbox() {
	log.Trace("UploadSessionDB notifyOutbox")

	if db.outboxSignal != nil {
		db.outboxSignal()
	}
}

func uploadSessionDAO(session *model.UploadSession) pgx.NamedArgs {
	return pgx.NamedArgs{
		"upload_id":             pgtype.UUID{Bytes: session.UploadID, Valid: session.UploadID != uuid.Nil},
		"dataset_id":            pgtype.UUID{Bytes: session.DatasetID, Valid: session.DatasetID != uuid.Nil},
		"user_id":               pgtype.UUID{Bytes: session.UserID, Valid: session.UserID != uuid.Nil},
		"client_nonce":          session.ClientNonce,
		"file_name":             session.FileName,
		"staging_key":           session.StagingKey,
		"final_key":             session.FinalKey,
		"storage_location":      session.StorageLocation,
		"declared_format":       session.DeclaredFormat,
		"declared_content_type": session.DeclaredContentType,
		"declared_size_bytes":   session.DeclaredSizeBytes,
		"actual_size_bytes":     session.ActualSizeBytes,
		"checksum":              session.Checksum,
		"status":                string(session.Status),
		"table_namespace":       session.TableNamespace,
		"table_name":            session.TableName,
		"table_format":          session.TableFormat,
		"catalog_provider":      session.CatalogProvider,
		"processing_profile":    session.ProcessingProfile,
		"created_at":            session.CreatedAt,
		"expires_at":            session.ExpiresAt,
	}
}

func uploadSessionIDsDAO(uploadID, userID uuid.UUID) pgx.NamedArgs {
	return pgx.NamedArgs{
		"upload_id": pgtype.UUID{Bytes: uploadID, Valid: true},
		"user_id":   pgtype.UUID{Bytes: userID, Valid: true},
	}
}

func scanUploadSession(row pgx.Row) (*model.UploadSession, error) {
	log.Trace("scanUploadSession")

	var uploadID, datasetID, userID, status string
	session := &model.UploadSession{}
	if err := row.Scan(
		&uploadID,
		&datasetID,
		&userID,
		&session.ClientNonce,
		&session.FileName,
		&session.StagingKey,
		&session.FinalKey,
		&session.StorageLocation,
		&session.DeclaredFormat,
		&session.DeclaredContentType,
		&session.DeclaredSizeBytes,
		&session.ActualSizeBytes,
		&session.Checksum,
		&status,
		&session.TableNamespace,
		&session.TableName,
		&session.TableFormat,
		&session.CatalogProvider,
		&session.ProcessingProfile,
		&session.CreatedAt,
		&session.ExpiresAt,
	); err != nil {
		return nil, err
	}
	session.UploadID = uuid.MustParse(uploadID)
	session.DatasetID = uuid.MustParse(datasetID)
	session.UserID = uuid.MustParse(userID)
	session.Status = model.UploadSessionStatus(status)
	return session, nil
}

func datasetFileUploadedMessage(topic string, session *model.UploadSession) msgConn.OutboundMessage {
	log.Trace("datasetFileUploadedMessage")

	payload := mustMarshalUpload(&datasetpb.DatasetFileUploadedEvent{
		DatasetId:         session.DatasetID.String(),
		UserId:            session.UserID.String(),
		StorageLocation:   session.StorageLocation,
		ContentType:       session.DeclaredContentType,
		FileExtension:     session.DeclaredFormat,
		TableNamespace:    session.TableNamespace,
		TableName:         session.TableName,
		TableFormat:       session.TableFormat,
		CatalogProvider:   session.CatalogProvider,
		ProcessingProfile: session.ProcessingProfile,
		SourceType:        "upload",
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: session.DatasetID,
			MsgType:     msgConn.MsgTypeDatasetFileUploaded,
			Payload:     payload,
		},
		DispatchKey: "dataset_file_uploaded:" + session.UploadID.String(),
	}
}

func mustMarshalUpload(payload proto.Message) []byte {
	log.Trace("mustMarshalUpload")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}
