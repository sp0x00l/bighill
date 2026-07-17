package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type EffectiveBaseRepository struct {
	coreDB.Database
}

func NewEffectiveBaseRepository(db *coreDB.Database) *EffectiveBaseRepository {
	log.Trace("NewEffectiveBaseRepository")

	return &EffectiveBaseRepository{Database: *db}
}

func (r *EffectiveBaseRepository) RecordEffectiveBase(ctx context.Context, tx pgx.Tx, effectiveBase *model.EffectiveBaseVersion) (*model.EffectiveBaseVersion, error) {
	log.Trace("EffectiveBaseRepository RecordEffectiveBase")

	query := `INSERT INTO ` + r.Name + `.effective_base_versions (
		model_id, org_id, base_model,
		source_artifact_location, source_artifact_format, source_artifact_checksum,
		serving_target, serving_model, serving_protocol
	)
	VALUES (
		@model_id, @org_id, @base_model,
		@source_artifact_location, @source_artifact_format, @source_artifact_checksum,
		@serving_target, @serving_model, @serving_protocol::serving_protocol_enum
	)
	ON CONFLICT (model_id, source_artifact_checksum, serving_target, serving_model, serving_protocol)
	DO UPDATE SET
		org_id = EXCLUDED.org_id,
		base_model = EXCLUDED.base_model,
		source_artifact_location = EXCLUDED.source_artifact_location,
		source_artifact_format = EXCLUDED.source_artifact_format,
		updated_at = now()
	RETURNING ` + effectiveBaseColumns()

	record, err := scanEffectiveBase(tx.QueryRow(ctx, query, effectiveBaseArgs(effectiveBase)))
	if err != nil {
		if coreDB.IsForeignKeyViolation(err) {
			return nil, fmt.Errorf("%w: effective base references an unknown model", domain.ErrValidationFailed)
		}
		r.LogPoolStatsOnError(ctx, "record effective base failed", err)
		return nil, fmt.Errorf("record effective base: %w", err)
	}
	return record, nil
}

func (r *EffectiveBaseRepository) ReadLatestByModelID(ctx context.Context, modelID uuid.UUID) (*model.EffectiveBaseVersion, error) {
	log.Trace("EffectiveBaseRepository ReadLatestByModelID")

	query := `SELECT ` + effectiveBaseColumns() + ` FROM ` + r.Name + `.effective_base_versions
		WHERE model_id = @model_id
		ORDER BY updated_at DESC, created_at DESC, effective_base_id DESC
		LIMIT 1`
	record, err := scanEffectiveBase(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"model_id": pgtype.UUID{Bytes: modelID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		r.LogPoolStatsOnError(ctx, "read effective base failed", err)
		return nil, fmt.Errorf("read effective base: %w", err)
	}
	return record, nil
}

func effectiveBaseArgs(effectiveBase *model.EffectiveBaseVersion) pgx.NamedArgs {
	log.Trace("effectiveBaseArgs")

	return pgx.NamedArgs{
		"model_id":                 pgtype.UUID{Bytes: effectiveBase.ModelID, Valid: true},
		"org_id":                   pgtype.UUID{Bytes: effectiveBase.OrgID, Valid: effectiveBase.OrgID != uuid.Nil},
		"base_model":               effectiveBase.BaseModel,
		"source_artifact_location": effectiveBase.SourceArtifactLocation,
		"source_artifact_format":   effectiveBase.SourceArtifactFormat,
		"source_artifact_checksum": effectiveBase.SourceArtifactChecksum,
		"serving_target":           effectiveBase.ServingTarget,
		"serving_model":            effectiveBase.ServingModel,
		"serving_protocol":         effectiveBase.ServingProtocol.String(),
	}
}

func effectiveBaseColumns() string {
	log.Trace("effectiveBaseColumns")

	return `effective_base_id::text, model_id::text, org_id::text, base_model,
		source_artifact_location, source_artifact_format, source_artifact_checksum,
		serving_target, serving_model, serving_protocol::text, created_at, updated_at`
}

func scanEffectiveBase(row pgx.Row) (*model.EffectiveBaseVersion, error) {
	log.Trace("scanEffectiveBase")

	var effectiveBaseID, modelID, orgID, servingProtocolRaw string
	var createdAt, updatedAt time.Time
	record := &model.EffectiveBaseVersion{}
	if err := row.Scan(
		&effectiveBaseID,
		&modelID,
		&orgID,
		&record.BaseModel,
		&record.SourceArtifactLocation,
		&record.SourceArtifactFormat,
		&record.SourceArtifactChecksum,
		&record.ServingTarget,
		&record.ServingModel,
		&servingProtocolRaw,
		&createdAt,
		&updatedAt,
	); err != nil {
		return nil, err
	}
	servingProtocol, err := model.ToServingProtocol(servingProtocolRaw)
	if err != nil {
		return nil, err
	}
	record.EffectiveBaseID = uuid.MustParse(effectiveBaseID)
	record.ModelID = uuid.MustParse(modelID)
	record.OrgID = uuid.MustParse(orgID)
	record.ServingProtocol = servingProtocol
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	return record, nil
}
