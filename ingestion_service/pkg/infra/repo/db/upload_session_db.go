package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	coreDb "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type UploadSessionDB struct {
	coreDb.Database
}

func NewUploadSessionDB(db *coreDb.Database) *UploadSessionDB {
	log.Trace("NewUploadSessionDB")

	return &UploadSessionDB{Database: *db}
}

func (db *UploadSessionDB) ReserveID(ctx context.Context, tx pgx.Tx) (uuid.UUID, error) {
	log.Trace("UploadSessionDB ReserveID")

	var id string
	if err := tx.QueryRow(ctx, `SELECT uuid_generate_v4()::text`).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("reserve upload id: %w", err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("reserve upload id returned invalid uuid: %w", err)
	}
	return parsed, nil
}

func (db *UploadSessionDB) CreateUploadSession(ctx context.Context, tx pgx.Tx, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB CreateUploadSession")

	query := `INSERT INTO ` + db.Name + `.upload_sessions (
		upload_id, resource_type, resource_id, dataset_id, user_id, org_id, client_nonce, file_name, staging_key, final_key,
		declared_format, declared_content_type, declared_size_bytes, status,
		table_namespace, table_name, table_format, catalog_provider, processing_profile,
		artifact_type, model_name, model_version, base_model,
		source, source_uri, manifest_location, hf_repo_id, hf_revision, hf_commit_sha,
		created_at, expires_at
	) VALUES (
		COALESCE(@upload_id, uuid_generate_v4()), @resource_type::upload_resource_type_enum, COALESCE(@resource_id, uuid_generate_v4()), @dataset_id, @user_id, @org_id, @client_nonce, @file_name, @staging_key, @final_key,
		@declared_format, @declared_content_type, @declared_size_bytes, @status::upload_session_status_enum,
		@table_namespace, @table_name, NULLIF(@table_format, '')::table_format_enum, NULLIF(@catalog_provider, '')::catalog_provider_enum, NULLIF(@processing_profile, '')::processing_profile_enum,
		@artifact_type, @model_name, @model_version, @base_model,
		@source::model_source_enum, @source_uri, @manifest_location, @hf_repo_id, @hf_revision, @hf_commit_sha,
		@created_at, @expires_at
	)
	ON CONFLICT (resource_type, org_id, user_id, client_nonce) WHERE client_nonce <> '' DO UPDATE SET
		expires_at = CASE
			WHEN ` + db.Name + `.upload_sessions.status IN ('PENDING', 'EXPIRED') THEN EXCLUDED.expires_at
			ELSE ` + db.Name + `.upload_sessions.expires_at
		END
		, status = CASE
			WHEN ` + db.Name + `.upload_sessions.status = 'EXPIRED' THEN 'PENDING'::upload_session_status_enum
			ELSE ` + db.Name + `.upload_sessions.status
		END
		, updated_at = now()
	RETURNING ` + uploadSessionColumns()

	out, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionDAO(session)))
	if err != nil {
		if coreDb.IsForeignKeyViolation(err) {
			return nil, domain.ErrDependencyNotReady.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("create upload session: %w", err)
	}
	return out, nil
}

func (db *UploadSessionDB) ReadUploadSessionForComplete(ctx context.Context, uploadID, userID uuid.UUID) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB ReadUploadSessionForComplete")

	query := `SELECT ` + uploadSessionColumns() + `
		FROM ` + db.Name + `.upload_sessions
		WHERE upload_id = @upload_id AND org_id = @org_id`
	session, err := scanUploadSession(db.Pool.QueryRow(ctx, query, uploadSessionIDsDAO(ctx, uploadID, userID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrResourceNotFound
		}
		return nil, fmt.Errorf("read upload session: %w", err)
	}
	return session, nil
}

func (db *UploadSessionDB) PromoteUploadSession(ctx context.Context, tx pgx.Tx, session *model.UploadSession) (*model.UploadSession, bool, error) {
	log.Trace("UploadSessionDB PromoteUploadSession")

	query := `UPDATE ` + db.Name + `.upload_sessions SET
		storage_location = @storage_location,
		actual_size_bytes = @actual_size_bytes,
		checksum = @checksum,
		status = 'PROMOTED'::upload_session_status_enum,
		updated_at = now()
		WHERE upload_id = @upload_id AND org_id = @org_id AND status = 'PENDING'
		RETURNING ` + uploadSessionColumns()
	promoted, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionDAO(session)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			existing, readErr := db.readUploadSessionTx(ctx, tx, session.UploadID, session.UserID)
			if readErr != nil {
				return nil, false, readErr
			}
			if existing.Status == model.UploadSessionPromoted {
				return existing, false, nil
			}
			return nil, false, domain.ErrValidationFailed.Extend("upload session is not pending")
		}
		return nil, false, fmt.Errorf("promote upload session: %w", err)
	}
	return promoted, true, nil
}

func (db *UploadSessionDB) RejectUploadSession(ctx context.Context, tx pgx.Tx, uploadID, userID uuid.UUID) error {
	log.Trace("UploadSessionDB RejectUploadSession")

	return db.setUploadSessionStatus(ctx, tx, uploadID, userID, model.UploadSessionRejected)
}

func (db *UploadSessionDB) ExpireUploadSession(ctx context.Context, tx pgx.Tx, uploadID, userID uuid.UUID) error {
	log.Trace("UploadSessionDB ExpireUploadSession")

	return db.setUploadSessionStatus(ctx, tx, uploadID, userID, model.UploadSessionExpired)
}

func (db *UploadSessionDB) RecordUploadedFile(ctx context.Context, tx pgx.Tx, upload *model.DataFile, storageLocation string, uploadID uuid.UUID) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB RecordUploadedFile")

	session := &model.UploadSession{
		UploadID:            uploadID,
		ResourceType:        model.UploadResourceDataFile,
		ResourceID:          upload.DatasetID,
		DatasetID:           upload.DatasetID,
		UserID:              upload.UserID,
		OrgID:               upload.OrgID,
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

	query := `INSERT INTO ` + db.Name + `.upload_sessions (
		upload_id, resource_type, resource_id, dataset_id, user_id, org_id, file_name, storage_location, declared_format,
		declared_content_type, status, table_namespace, table_name, table_format,
		catalog_provider, processing_profile, artifact_type, model_name, model_version, base_model, created_at, expires_at
	) VALUES (
		COALESCE(@upload_id, uuid_generate_v4()), @resource_type::upload_resource_type_enum, @resource_id, @dataset_id, @user_id, @org_id, @file_name, @storage_location, @declared_format,
		@declared_content_type, 'PROMOTED'::upload_session_status_enum, @table_namespace, @table_name, @table_format::table_format_enum,
		@catalog_provider::catalog_provider_enum, @processing_profile::processing_profile_enum, @artifact_type, @model_name, @model_version, @base_model, now(), now()
	)
	ON CONFLICT (upload_id) DO UPDATE SET upload_id = EXCLUDED.upload_id
	RETURNING ` + uploadSessionColumns()
	recorded, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionDAO(session)))
	if err != nil {
		if coreDb.IsForeignKeyViolation(err) {
			return nil, domain.ErrDependencyNotReady.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("record direct upload session: %w", err)
	}
	return recorded, nil
}

func (db *UploadSessionDB) RecordModelArtifact(ctx context.Context, tx pgx.Tx, session *model.UploadSession) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB RecordModelArtifact")

	session.ResourceType = model.UploadResourceModelArtifact
	session.Status = model.UploadSessionPromoted
	now := time.Now().UTC()
	if session.CreatedAt.IsZero() {
		session.CreatedAt = now
	}
	if session.ExpiresAt.IsZero() {
		session.ExpiresAt = now
	}
	query := `INSERT INTO ` + db.Name + `.upload_sessions (
		upload_id, resource_type, resource_id, dataset_id, user_id, org_id, client_nonce, file_name, storage_location,
		declared_format, declared_content_type, declared_size_bytes, actual_size_bytes, checksum, status,
		table_namespace, table_name, table_format, catalog_provider, processing_profile,
		artifact_type, model_name, model_version, base_model,
		source, source_uri, manifest_location, hf_repo_id, hf_revision, hf_commit_sha, created_at, expires_at
	) VALUES (
		COALESCE(@upload_id, uuid_generate_v4()), @resource_type::upload_resource_type_enum, COALESCE(@resource_id, uuid_generate_v4()), @dataset_id, @user_id, @org_id, @client_nonce, @file_name, @storage_location,
		@declared_format, @declared_content_type, @declared_size_bytes, @actual_size_bytes, @checksum, 'PROMOTED'::upload_session_status_enum,
		@table_namespace, @table_name, NULLIF(@table_format, '')::table_format_enum, NULLIF(@catalog_provider, '')::catalog_provider_enum, NULLIF(@processing_profile, '')::processing_profile_enum,
		@artifact_type, @model_name, @model_version, @base_model,
		@source::model_source_enum, @source_uri, @manifest_location, @hf_repo_id, @hf_revision, @hf_commit_sha, @created_at, @expires_at
	)
	ON CONFLICT (upload_id) DO UPDATE SET
		resource_type = EXCLUDED.resource_type,
		resource_id = EXCLUDED.resource_id,
		dataset_id = EXCLUDED.dataset_id,
		user_id = EXCLUDED.user_id,
		org_id = EXCLUDED.org_id,
		client_nonce = EXCLUDED.client_nonce,
		file_name = EXCLUDED.file_name,
		storage_location = EXCLUDED.storage_location,
		declared_format = EXCLUDED.declared_format,
		declared_content_type = EXCLUDED.declared_content_type,
		declared_size_bytes = EXCLUDED.declared_size_bytes,
		actual_size_bytes = EXCLUDED.actual_size_bytes,
		checksum = EXCLUDED.checksum,
		status = EXCLUDED.status,
		table_namespace = EXCLUDED.table_namespace,
		table_name = EXCLUDED.table_name,
		table_format = EXCLUDED.table_format,
		catalog_provider = EXCLUDED.catalog_provider,
		processing_profile = EXCLUDED.processing_profile,
		artifact_type = EXCLUDED.artifact_type,
		model_name = EXCLUDED.model_name,
		model_version = EXCLUDED.model_version,
		base_model = EXCLUDED.base_model,
		source = EXCLUDED.source,
		source_uri = EXCLUDED.source_uri,
		manifest_location = EXCLUDED.manifest_location,
		hf_repo_id = EXCLUDED.hf_repo_id,
		hf_revision = EXCLUDED.hf_revision,
		hf_commit_sha = EXCLUDED.hf_commit_sha,
		updated_at = now()
	RETURNING ` + uploadSessionColumns()
	recorded, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionDAO(session)))
	if err != nil {
		if coreDb.IsForeignKeyViolation(err) {
			return nil, domain.ErrDependencyNotReady.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("record model artifact session: %w", err)
	}
	return recorded, nil
}

func (db *UploadSessionDB) readUploadSessionTx(ctx context.Context, tx pgx.Tx, uploadID, userID uuid.UUID) (*model.UploadSession, error) {
	log.Trace("UploadSessionDB readUploadSessionTx")

	query := `SELECT ` + uploadSessionColumns() + `
		FROM ` + db.Name + `.upload_sessions
		WHERE upload_id = @upload_id AND org_id = @org_id`
	session, err := scanUploadSession(tx.QueryRow(ctx, query, uploadSessionIDsDAO(ctx, uploadID, userID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrResourceNotFound
		}
		return nil, fmt.Errorf("read upload session in tx: %w", err)
	}
	return session, nil
}

func (db *UploadSessionDB) setUploadSessionStatus(ctx context.Context, tx pgx.Tx, uploadID, userID uuid.UUID, status model.UploadSessionStatus) error {
	log.Trace("UploadSessionDB setUploadSessionStatus")

	query := `UPDATE ` + db.Name + `.upload_sessions SET status = @status::upload_session_status_enum, updated_at = now()
		WHERE upload_id = @upload_id AND org_id = @org_id AND status = 'PENDING'`
	cmd, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"upload_id": pgtype.UUID{Bytes: uploadID, Valid: true},
		"user_id":   pgtype.UUID{Bytes: userID, Valid: true},
		"org_id":    pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
		"status":    string(status),
	})
	if err != nil {
		return fmt.Errorf("set upload session status: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		current, readErr := db.readUploadSessionTx(ctx, tx, uploadID, userID)
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

func uploadSessionDAO(session *model.UploadSession) pgx.NamedArgs {
	return pgx.NamedArgs{
		"upload_id":             pgtype.UUID{Bytes: session.UploadID, Valid: session.UploadID != uuid.Nil},
		"resource_type":         string(session.ResourceType),
		"resource_id":           pgtype.UUID{Bytes: session.ResourceID, Valid: session.ResourceID != uuid.Nil},
		"dataset_id":            pgtype.UUID{Bytes: session.DatasetID, Valid: session.DatasetID != uuid.Nil},
		"user_id":               pgtype.UUID{Bytes: session.UserID, Valid: session.UserID != uuid.Nil},
		"org_id":                pgtype.UUID{Bytes: session.OrgID, Valid: session.OrgID != uuid.Nil},
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
		"artifact_type":         session.ArtifactType,
		"model_name":            session.ModelName,
		"model_version":         session.ModelVersion,
		"base_model":            session.BaseModel,
		"source":                session.Source,
		"source_uri":            session.SourceURI,
		"manifest_location":     session.ManifestLocation,
		"hf_repo_id":            session.HFRepoID,
		"hf_revision":           session.HFRevision,
		"hf_commit_sha":         session.HFCommitSHA,
		"created_at":            session.CreatedAt,
		"expires_at":            session.ExpiresAt,
	}
}

func uploadSessionIDsDAO(ctx context.Context, uploadID, userID uuid.UUID) pgx.NamedArgs {
	return pgx.NamedArgs{
		"upload_id": pgtype.UUID{Bytes: uploadID, Valid: true},
		"user_id":   pgtype.UUID{Bytes: userID, Valid: true},
		"org_id":    pgtype.UUID{Bytes: orgIDFromContext(ctx), Valid: orgIDFromContext(ctx) != uuid.Nil},
	}
}

func uploadSessionColumns() string {
	log.Trace("uploadSessionColumns")

	return `upload_id::text, resource_type::text, resource_id::text, COALESCE(dataset_id::text, ''), user_id::text, org_id::text, client_nonce, file_name,
		staging_key, final_key, storage_location, declared_format, declared_content_type,
		declared_size_bytes, actual_size_bytes, checksum, status::text, table_namespace, table_name,
		COALESCE(table_format::text, ''), COALESCE(catalog_provider::text, ''), COALESCE(processing_profile::text, ''),
		artifact_type, model_name, model_version,
			base_model, source::text, source_uri, manifest_location, hf_repo_id, hf_revision, hf_commit_sha,
		created_at, expires_at`
}

func scanUploadSession(row pgx.Row) (*model.UploadSession, error) {
	log.Trace("scanUploadSession")

	var uploadID, resourceType, resourceID, datasetID, userID, orgID, status string
	session := &model.UploadSession{}
	if err := row.Scan(
		&uploadID,
		&resourceType,
		&resourceID,
		&datasetID,
		&userID,
		&orgID,
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
		&session.ArtifactType,
		&session.ModelName,
		&session.ModelVersion,
		&session.BaseModel,
		&session.Source,
		&session.SourceURI,
		&session.ManifestLocation,
		&session.HFRepoID,
		&session.HFRevision,
		&session.HFCommitSHA,
		&session.CreatedAt,
		&session.ExpiresAt,
	); err != nil {
		return nil, err
	}
	session.UploadID = uuid.MustParse(uploadID)
	session.ResourceType = model.UploadResourceType(resourceType)
	session.ResourceID = uuid.MustParse(resourceID)
	if datasetID != "" {
		session.DatasetID = uuid.MustParse(datasetID)
	}
	session.UserID = uuid.MustParse(userID)
	session.OrgID = uuid.MustParse(orgID)
	session.Status = model.UploadSessionStatus(status)
	return session, nil
}
