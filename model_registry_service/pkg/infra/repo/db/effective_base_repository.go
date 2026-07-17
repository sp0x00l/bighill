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
		effective_base_id, foundation_model_id, descriptor_schema_version,
		foundation_checksum, descriptor
	)
	VALUES (
		@effective_base_id, @foundation_model_id, @descriptor_schema_version,
		@foundation_checksum, @descriptor::jsonb
	)
	ON CONFLICT (effective_base_id) DO NOTHING
	RETURNING ` + effectiveBaseColumns()

	record, err := scanEffectiveBase(tx.QueryRow(ctx, query, effectiveBaseArgs(effectiveBase)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return r.ReadByIDTx(ctx, tx, effectiveBase.EffectiveBaseID)
		}
		if coreDB.IsForeignKeyViolation(err) {
			return nil, fmt.Errorf("%w: effective base references an unknown model", domain.ErrValidationFailed)
		}
		r.LogPoolStatsOnError(ctx, "record effective base failed", err)
		return nil, fmt.Errorf("record effective base: %w", err)
	}
	return record, nil
}

func (r *EffectiveBaseRepository) ReadByID(ctx context.Context, effectiveBaseID string) (*model.EffectiveBaseVersion, error) {
	log.Trace("EffectiveBaseRepository ReadByID")

	query := `SELECT ` + effectiveBaseColumns() + ` FROM ` + r.Name + `.effective_base_versions
		WHERE effective_base_id = @effective_base_id
		LIMIT 1`
	record, err := scanEffectiveBase(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"effective_base_id": effectiveBaseID,
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

func (r *EffectiveBaseRepository) ReadByIDTx(ctx context.Context, tx pgx.Tx, effectiveBaseID string) (*model.EffectiveBaseVersion, error) {
	log.Trace("EffectiveBaseRepository ReadByIDTx")

	query := `SELECT ` + effectiveBaseColumns() + ` FROM ` + r.Name + `.effective_base_versions
		WHERE effective_base_id = @effective_base_id
		LIMIT 1`
	record, err := scanEffectiveBase(tx.QueryRow(ctx, query, pgx.NamedArgs{
		"effective_base_id": effectiveBaseID,
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		return nil, fmt.Errorf("read effective base in transaction: %w", err)
	}
	return record, nil
}

func (r *EffectiveBaseRepository) ReadLatestByFoundationModelID(ctx context.Context, modelID uuid.UUID) (*model.EffectiveBaseVersion, error) {
	log.Trace("EffectiveBaseRepository ReadLatestByFoundationModelID")

	query := `SELECT ` + effectiveBaseColumns() + ` FROM ` + r.Name + `.effective_base_versions
		WHERE foundation_model_id = @foundation_model_id
		ORDER BY updated_at DESC, created_at DESC, effective_base_id DESC
		LIMIT 1`
	record, err := scanEffectiveBase(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"foundation_model_id": pgtype.UUID{Bytes: modelID, Valid: true},
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
		"effective_base_id":         effectiveBase.EffectiveBaseID,
		"foundation_model_id":       pgtype.UUID{Bytes: effectiveBase.FoundationModelID, Valid: true},
		"descriptor_schema_version": effectiveBase.DescriptorSchemaVersion,
		"foundation_checksum":       effectiveBase.FoundationChecksum,
		"descriptor":                effectiveBase.Descriptor,
	}
}

func effectiveBaseColumns() string {
	log.Trace("effectiveBaseColumns")

	return `effective_base_id, foundation_model_id::text, descriptor_schema_version,
		foundation_checksum, descriptor::text, created_at, updated_at`
}

func scanEffectiveBase(row pgx.Row) (*model.EffectiveBaseVersion, error) {
	log.Trace("scanEffectiveBase")

	var effectiveBaseID, foundationModelID string
	var createdAt, updatedAt time.Time
	record := &model.EffectiveBaseVersion{}
	if err := row.Scan(
		&effectiveBaseID,
		&foundationModelID,
		&record.DescriptorSchemaVersion,
		&record.FoundationChecksum,
		&record.Descriptor,
		&createdAt,
		&updatedAt,
	); err != nil {
		return nil, err
	}
	parsedFoundationModelID, err := uuid.Parse(foundationModelID)
	if err != nil {
		return nil, fmt.Errorf("parse foundation model id: %w", err)
	}
	record.EffectiveBaseID = effectiveBaseID
	record.FoundationModelID = parsedFoundationModelID
	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	return record, nil
}
