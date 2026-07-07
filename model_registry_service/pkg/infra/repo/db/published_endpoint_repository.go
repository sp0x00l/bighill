package db

import (
	"context"
	"fmt"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	coreDB "lib/shared_lib/db"

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

func (r *PublishedEndpointRepository) UpsertEndpoint(ctx context.Context, tx pgx.Tx, endpoint *model.PublishedEndpoint) error {
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
		created_by_user_id = EXCLUDED.created_by_user_id;`
	if _, err := tx.Exec(ctx, query, publishedEndpointArgs(endpoint)); err != nil {
		if coreDB.IsForeignKeyViolation(err) {
			return fmt.Errorf("%w: published endpoint reference is not ready", domain.ErrValidationFailed)
		}
		return fmt.Errorf("upsert published endpoint: %w", err)
	}
	return nil
}

func publishedEndpointArgs(endpoint *model.PublishedEndpoint) pgx.NamedArgs {
	log.Trace("publishedEndpointArgs")

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
