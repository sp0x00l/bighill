package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type LineageEvalRepository struct {
	coreDB.Database
}

func NewLineageEvalRepository(db *coreDB.Database) *LineageEvalRepository {
	log.Trace("NewLineageEvalRepository")

	return &LineageEvalRepository{Database: *db}
}

func (r *LineageEvalRepository) ReadActiveEvalSet(ctx context.Context, orgID uuid.UUID, lineageName string) (*model.LineageEvalSet, error) {
	log.Trace("LineageEvalRepository ReadActiveEvalSet")

	query := `SELECT org_id::text, lineage_name, eval_set_version, eval_dataset_uri, checksum, example_count, source, is_active, frozen_at
		FROM ` + r.Name + `.lineage_eval_sets
		WHERE org_id = @org_id
		  AND lineage_name = @lineage_name
		  AND is_active = true`
	record, err := scanLineageEvalSet(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":       nullableUUID(orgID),
		"lineage_name": strings.TrimSpace(lineageName),
	}))
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrEvalSetNotFound
		}
		return nil, fmt.Errorf("read active lineage eval set: %w", err)
	}
	return record, nil
}

func (r *LineageEvalRepository) FreezeEvalSet(ctx context.Context, tx pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error) {
	log.Trace("LineageEvalRepository FreezeEvalSet")

	if evalSet.Source == "" {
		evalSet.Source = model.LineageEvalSetSourceFrozenGen0
	}
	evalSet.Active = true
	return r.writeEvalSet(ctx, tx, evalSet, exampleIDs)
}

func (r *LineageEvalRepository) RegisterCuratedEvalSet(ctx context.Context, tx pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error) {
	log.Trace("LineageEvalRepository RegisterCuratedEvalSet")

	version, err := r.nextEvalSetVersion(ctx, tx, evalSet.OrgID, evalSet.LineageName)
	if err != nil {
		return nil, err
	}
	if err := r.deactivateActiveEvalSet(ctx, tx, evalSet.OrgID, evalSet.LineageName); err != nil {
		return nil, err
	}
	evalSet.Version = version
	evalSet.Source = model.LineageEvalSetSourceCurated
	evalSet.Active = true
	return r.writeEvalSet(ctx, tx, evalSet, exampleIDs)
}

func (r *LineageEvalRepository) nextEvalSetVersion(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, lineageName string) (int, error) {
	log.Trace("LineageEvalRepository nextEvalSetVersion")

	query := `SELECT COALESCE(MAX(eval_set_version), 0) + 1
		FROM ` + r.Name + `.lineage_eval_sets
		WHERE org_id = @org_id
		  AND lineage_name = @lineage_name`
	version := 0
	if err := tx.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":       nullableUUID(orgID),
		"lineage_name": strings.TrimSpace(lineageName),
	}).Scan(&version); err != nil {
		return 0, fmt.Errorf("read next lineage eval set version: %w", err)
	}
	return version, nil
}

func (r *LineageEvalRepository) deactivateActiveEvalSet(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, lineageName string) error {
	log.Trace("LineageEvalRepository deactivateActiveEvalSet")

	query := `UPDATE ` + r.Name + `.lineage_eval_sets
		SET is_active = false
		WHERE org_id = @org_id
		  AND lineage_name = @lineage_name
		  AND is_active = true`
	if _, err := tx.Exec(ctx, query, pgx.NamedArgs{
		"org_id":       nullableUUID(orgID),
		"lineage_name": strings.TrimSpace(lineageName),
	}); err != nil {
		return fmt.Errorf("deactivate active lineage eval set: %w", err)
	}
	return nil
}

func (r *LineageEvalRepository) writeEvalSet(ctx context.Context, tx pgx.Tx, evalSet *model.LineageEvalSet, exampleIDs []uuid.UUID) (*model.LineageEvalSet, error) {
	log.Trace("LineageEvalRepository writeEvalSet")

	if evalSet.Version == 0 {
		evalSet.Version = 1
	}
	if evalSet.ExampleCount == 0 {
		evalSet.ExampleCount = len(exampleIDs)
	}
	if evalSet.FrozenAt.IsZero() {
		evalSet.FrozenAt = time.Now().UTC()
	}
	query := `INSERT INTO ` + r.Name + `.lineage_eval_sets (
			org_id, lineage_name, eval_set_version, eval_dataset_uri, checksum, example_count, source, is_active, frozen_at
		) VALUES (
			@org_id, @lineage_name, @eval_set_version, @eval_dataset_uri, @checksum, @example_count, @source, @is_active, @frozen_at
		)
		ON CONFLICT (org_id, lineage_name, eval_set_version) DO UPDATE SET
			eval_dataset_uri = EXCLUDED.eval_dataset_uri,
			checksum = EXCLUDED.checksum,
			example_count = EXCLUDED.example_count,
			source = EXCLUDED.source,
			is_active = EXCLUDED.is_active,
			frozen_at = EXCLUDED.frozen_at`
	if _, err := tx.Exec(ctx, query, lineageEvalSetArgs(evalSet)); err != nil {
		return nil, fmt.Errorf("write lineage eval set: %w", err)
	}
	if len(exampleIDs) == 0 {
		return evalSet, nil
	}
	exampleQuery := `INSERT INTO ` + r.Name + `.lineage_eval_examples (
			org_id, lineage_name, eval_set_version, preference_example_id
		)
		SELECT @org_id, @lineage_name, @eval_set_version, unnest(@preference_example_ids::uuid[])
		ON CONFLICT (org_id, lineage_name, eval_set_version, preference_example_id) DO NOTHING`
	if _, err := tx.Exec(ctx, exampleQuery, pgx.NamedArgs{
		"org_id":                 nullableUUID(evalSet.OrgID),
		"lineage_name":           strings.TrimSpace(evalSet.LineageName),
		"eval_set_version":       evalSet.Version,
		"preference_example_ids": exampleIDs,
	}); err != nil {
		return nil, fmt.Errorf("write lineage eval examples: %w", err)
	}
	return evalSet, nil
}

func lineageEvalSetArgs(evalSet *model.LineageEvalSet) pgx.NamedArgs {
	log.Trace("lineageEvalSetArgs")

	return pgx.NamedArgs{
		"org_id":           nullableUUID(evalSet.OrgID),
		"lineage_name":     strings.TrimSpace(evalSet.LineageName),
		"eval_set_version": evalSet.Version,
		"eval_dataset_uri": strings.TrimSpace(evalSet.EvalDatasetURI),
		"checksum":         strings.TrimSpace(evalSet.Checksum),
		"example_count":    evalSet.ExampleCount,
		"source":           string(evalSet.Source),
		"is_active":        evalSet.Active,
		"frozen_at":        evalSet.FrozenAt,
	}
}

func scanLineageEvalSet(row pgx.Row) (*model.LineageEvalSet, error) {
	log.Trace("scanLineageEvalSet")

	var orgID string
	var source string
	record := &model.LineageEvalSet{}
	if err := row.Scan(&orgID, &record.LineageName, &record.Version, &record.EvalDatasetURI, &record.Checksum, &record.ExampleCount, &source, &record.Active, &record.FrozenAt); err != nil {
		return nil, err
	}
	record.OrgID = uuid.MustParse(orgID)
	record.Source = model.LineageEvalSetSource(source)
	return record, nil
}
