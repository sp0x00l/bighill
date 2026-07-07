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

	query := `INSERT INTO ` + r.Name + `.published_inference_endpoints (
		endpoint_id, org_id, model_id, dataset_id, status, display_name, created_by_user_id
	) VALUES (
		@endpoint_id, @org_id, @model_id, @dataset_id, @status, @display_name, @created_by_user_id
	)
	ON CONFLICT (endpoint_id) DO UPDATE SET
		org_id = EXCLUDED.org_id,
		model_id = EXCLUDED.model_id,
		dataset_id = EXCLUDED.dataset_id,
		status = EXCLUDED.status,
		display_name = EXCLUDED.display_name,
		created_by_user_id = EXCLUDED.created_by_user_id
	RETURNING ` + endpointColumns()

	record, err := scanEndpoint(r.Pool.QueryRow(ctx, query, endpointArgs(endpoint)))
	if err != nil {
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("upsert published endpoint: %w", err)
	}
	return record, nil
}

func (r *PublishedEndpointRepository) ListEndpoints(ctx context.Context, orgID uuid.UUID) ([]*model.PublishedEndpoint, error) {
	log.Trace("PublishedEndpointRepository ListEndpoints")

	query := `SELECT ` + endpointColumns() + ` FROM ` + r.Name + `.published_inference_endpoints
		WHERE org_id = @org_id
		ORDER BY display_name ASC, created_at DESC`
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

	query := `SELECT ` + endpointColumns() + ` FROM ` + r.Name + `.published_inference_endpoints
		WHERE endpoint_id = @endpoint_id AND org_id = @org_id`
	record, err := scanEndpoint(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"endpoint_id": pgtype.UUID{Bytes: endpointID, Valid: true},
		"org_id":      pgtype.UUID{Bytes: orgID, Valid: true},
	}))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrModelNotFound
		}
		return nil, fmt.Errorf("read published endpoint: %w", err)
	}
	return record, nil
}

func endpointColumns() string {
	log.Trace("endpointColumns")

	return `endpoint_id::text, org_id::text, model_id::text, dataset_id::text, status, display_name, created_by_user_id::text`
}

func endpointArgs(endpoint *model.PublishedEndpoint) pgx.NamedArgs {
	log.Trace("endpointArgs")

	return pgx.NamedArgs{
		"endpoint_id":        pgtype.UUID{Bytes: endpoint.EndpointID, Valid: true},
		"org_id":             pgtype.UUID{Bytes: endpoint.OrgID, Valid: true},
		"model_id":           pgtype.UUID{Bytes: endpoint.ModelID, Valid: true},
		"dataset_id":         pgtype.UUID{Bytes: endpoint.DatasetID, Valid: true},
		"status":             string(endpoint.Status),
		"display_name":       endpoint.DisplayName,
		"created_by_user_id": pgtype.UUID{Bytes: endpoint.CreatedByUserID, Valid: true},
	}
}

func scanEndpoint(row pgx.Row) (*model.PublishedEndpoint, error) {
	log.Trace("scanEndpoint")

	var endpointID, orgID, modelID, datasetID, status, createdByUserID string
	record := &model.PublishedEndpoint{}
	if err := row.Scan(
		&endpointID,
		&orgID,
		&modelID,
		&datasetID,
		&status,
		&record.DisplayName,
		&createdByUserID,
	); err != nil {
		return nil, err
	}
	record.EndpointID = uuid.MustParse(endpointID)
	record.OrgID = uuid.MustParse(orgID)
	record.ModelID = uuid.MustParse(modelID)
	record.DatasetID = uuid.MustParse(datasetID)
	record.Status = model.PublishedEndpointStatus(status)
	record.CreatedByUserID = uuid.MustParse(createdByUserID)
	return record, nil
}
