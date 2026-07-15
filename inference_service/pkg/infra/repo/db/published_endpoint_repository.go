package db

import (
	"context"
	"errors"
	"fmt"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type PublishedEndpointRepository struct {
	coreDB.Database
}

func NewPublishedEndpointRepository(db *coreDB.Database) *PublishedEndpointRepository {
	log.Trace("NewPublishedEndpointRepository")

	return &PublishedEndpointRepository{Database: *db}
}

func (r *PublishedEndpointRepository) UpsertEndpoint(ctx context.Context, endpoint *model.PublishedEndpoint) (*model.PublishedEndpoint, error) {
	log.Trace("PublishedEndpointRepository UpsertEndpoint")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin published endpoint transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `INSERT INTO ` + r.Name + `.published_inference_endpoints (
		org_id, model_id, mode, agent_spec_id, agent_spec_hash, status, rag_merge_strategy, display_name, created_by_user_id
	) VALUES (
		@org_id, @model_id, @mode::agent_endpoint_mode_enum, @agent_spec_id, @agent_spec_hash, @status, @rag_merge_strategy, @display_name, @created_by_user_id
	)
	ON CONFLICT (org_id, model_id) DO UPDATE SET
		mode = EXCLUDED.mode,
		agent_spec_id = EXCLUDED.agent_spec_id,
		agent_spec_hash = EXCLUDED.agent_spec_hash,
		status = EXCLUDED.status,
		rag_merge_strategy = EXCLUDED.rag_merge_strategy,
		display_name = EXCLUDED.display_name,
		created_by_user_id = EXCLUDED.created_by_user_id
	RETURNING endpoint_id::text`

	var endpointIDText string
	err = tx.QueryRow(ctx, query, endpointArgs(endpoint)).Scan(&endpointIDText)
	if err != nil {
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("upsert published endpoint: %w", err)
	}
	endpointID := uuid.MustParse(endpointIDText)
	if endpoint.DatasetIDs != nil {
		if err := replaceEndpointDatasets(ctx, tx, r.Name, endpointID, endpoint.DatasetIDs); err != nil {
			return nil, err
		}
	}
	record, err := readEndpointTx(ctx, tx, r.Name, endpoint.OrgID, endpointID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit published endpoint transaction: %w", err)
	}
	return record, nil
}

func (r *PublishedEndpointRepository) SetEndpointDatasets(ctx context.Context, orgID uuid.UUID, endpointID uuid.UUID, datasetIDs []uuid.UUID) (*model.PublishedEndpoint, error) {
	log.Trace("PublishedEndpointRepository SetEndpointDatasets")

	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin published endpoint dataset transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := readEndpointTx(ctx, tx, r.Name, orgID, endpointID); err != nil {
		return nil, err
	}
	if err := replaceEndpointDatasets(ctx, tx, r.Name, endpointID, datasetIDs); err != nil {
		return nil, err
	}
	record, err := readEndpointTx(ctx, tx, r.Name, orgID, endpointID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit published endpoint dataset transaction: %w", err)
	}
	return record, nil
}

func (r *PublishedEndpointRepository) ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error) {
	log.Trace("PublishedEndpointRepository ListEndpoints")

	query := endpointReadQuery(r.Name, `endpoint.org_id = @org_id`) + `
		ORDER BY endpoint.display_name ASC, endpoint.created_at DESC`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{"org_id": pgtype.UUID{Bytes: orgID, Valid: true}})
	if err != nil {
		return nil, fmt.Errorf("list published endpoints: %w", err)
	}
	defer rows.Close()
	out := []*model.PublishedEndpoint{}
	for rows.Next() {
		record, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *PublishedEndpointRepository) ReadEndpoint(ctx context.Context, orgID uuid.UUID, endpointID uuid.UUID) (*model.PublishedEndpoint, error) {
	log.Trace("PublishedEndpointRepository ReadEndpoint")

	record, err := readEndpointRow(ctx, r.Pool, r.Name, orgID, endpointID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		return nil, fmt.Errorf("read published endpoint: %w", err)
	}
	return record, nil
}

type endpointRowReader interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func readEndpointRow(ctx context.Context, reader endpointRowReader, schemaName string, orgID uuid.UUID, endpointID uuid.UUID) (*model.PublishedEndpoint, error) {
	log.Trace("readEndpointRow")

	query := endpointReadQuery(schemaName, `endpoint.endpoint_id = @endpoint_id AND endpoint.org_id = @org_id`)
	return scanEndpoint(reader.QueryRow(ctx, query, pgx.NamedArgs{
		"endpoint_id": pgtype.UUID{Bytes: endpointID, Valid: true},
		"org_id":      pgtype.UUID{Bytes: orgID, Valid: true},
	}))
}

func readEndpointTx(ctx context.Context, tx pgx.Tx, schemaName string, orgID uuid.UUID, endpointID uuid.UUID) (*model.PublishedEndpoint, error) {
	log.Trace("readEndpointTx")

	record, err := readEndpointRow(ctx, tx, schemaName, orgID, endpointID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		return nil, fmt.Errorf("read published endpoint: %w", err)
	}
	return record, nil
}

func replaceEndpointDatasets(ctx context.Context, tx pgx.Tx, schemaName string, endpointID uuid.UUID, datasetIDs []uuid.UUID) error {
	log.Trace("replaceEndpointDatasets")

	if _, err := tx.Exec(ctx, `DELETE FROM `+schemaName+`.published_endpoint_datasets WHERE endpoint_id = @endpoint_id`, pgx.NamedArgs{
		"endpoint_id": pgtype.UUID{Bytes: endpointID, Valid: true},
	}); err != nil {
		return fmt.Errorf("replace published endpoint datasets: delete existing: %w", err)
	}
	for position, datasetID := range datasetIDs {
		if datasetID == uuid.Nil {
			return domain.ErrValidationFailed.Extend("published endpoint dataset_id is required")
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+schemaName+`.published_endpoint_datasets (
			endpoint_id, dataset_id, position
		) VALUES (
			@endpoint_id, @dataset_id, @position
		)`, pgx.NamedArgs{
			"endpoint_id": pgtype.UUID{Bytes: endpointID, Valid: true},
			"dataset_id":  pgtype.UUID{Bytes: datasetID, Valid: true},
			"position":    position,
		})
		if err != nil {
			return fmt.Errorf("replace published endpoint datasets: insert dataset: %w", err)
		}
	}
	return nil
}

func endpointReadQuery(schemaName string, predicate string) string {
	log.Trace("endpointReadQuery")

	return `SELECT ` + endpointColumns() + `
		FROM ` + schemaName + `.published_inference_endpoints endpoint
		LEFT JOIN ` + schemaName + `.published_endpoint_datasets endpoint_dataset
			ON endpoint_dataset.endpoint_id = endpoint.endpoint_id
		WHERE ` + predicate + `
		GROUP BY endpoint.endpoint_id`
}

func endpointColumns() string {
	log.Trace("endpointColumns")

	return `endpoint.endpoint_id::text,
		endpoint.org_id::text,
		endpoint.model_id::text,
		endpoint.mode::text,
		COALESCE(endpoint.agent_spec_id::text, ''),
		endpoint.agent_spec_hash,
		endpoint.status,
		endpoint.rag_merge_strategy,
		endpoint.display_name,
		endpoint.created_by_user_id::text,
		COALESCE(
			array_agg(endpoint_dataset.dataset_id::text ORDER BY endpoint_dataset.position, endpoint_dataset.dataset_id::text)
				FILTER (WHERE endpoint_dataset.dataset_id IS NOT NULL),
			ARRAY[]::text[]
		)`
}

func endpointArgs(endpoint *model.PublishedEndpoint) pgx.NamedArgs {
	log.Trace("endpointArgs")

	return pgx.NamedArgs{
		"org_id":             pgtype.UUID{Bytes: endpoint.OrgID, Valid: true},
		"model_id":           pgtype.UUID{Bytes: endpoint.ModelID, Valid: true},
		"mode":               endpointMode(endpoint.Mode),
		"agent_spec_id":      nullableUUID(endpoint.AgentSpecID),
		"agent_spec_hash":    endpoint.AgentSpecHash,
		"status":             string(endpoint.Status),
		"rag_merge_strategy": string(endpoint.MergeStrategy),
		"display_name":       endpoint.DisplayName,
		"created_by_user_id": pgtype.UUID{Bytes: endpoint.CreatedByUserID, Valid: true},
	}
}

func endpointMode(mode model.AgentEndpointMode) string {
	log.Trace("endpointMode")

	return mode.String()
}

func scanEndpoint(row pgx.Row) (*model.PublishedEndpoint, error) {
	log.Trace("scanEndpoint")

	var endpointID, orgID, modelID, mode, agentSpecID, status, mergeStrategy, createdByUserID string
	var datasetIDs []string
	record := &model.PublishedEndpoint{}
	if err := row.Scan(
		&endpointID,
		&orgID,
		&modelID,
		&mode,
		&agentSpecID,
		&record.AgentSpecHash,
		&status,
		&mergeStrategy,
		&record.DisplayName,
		&createdByUserID,
		&datasetIDs,
	); err != nil {
		return nil, err
	}
	record.EndpointID = uuid.MustParse(endpointID)
	record.OrgID = uuid.MustParse(orgID)
	record.ModelID = uuid.MustParse(modelID)
	parsedMode, err := model.ToAgentEndpointMode(mode)
	if err != nil {
		return nil, fmt.Errorf("parse published endpoint mode: %w", err)
	}
	record.Mode = parsedMode
	if agentSpecID != "" {
		record.AgentSpecID = uuid.MustParse(agentSpecID)
	}
	record.Status = model.PublishedEndpointStatus(status)
	record.MergeStrategy = model.RAGMergeStrategy(mergeStrategy)
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	record.DatasetIDs = make([]uuid.UUID, 0, len(datasetIDs))
	for _, datasetID := range datasetIDs {
		if datasetID == "" {
			continue
		}
		record.DatasetIDs = append(record.DatasetIDs, uuid.MustParse(datasetID))
	}
	return record, nil
}
