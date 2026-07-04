package tenant

import (
	"context"
	"errors"
	"fmt"
	"lib/shared_lib/ctxutil"
	"strings"

	sharedDB "lib/shared_lib/db"
	sharedDomain "lib/shared_lib/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

var ErrTenantNotFound = errors.New("tenant not found")

type PostgresProjectionStore struct {
	sharedDB.Database
}

func NewPostgresProjectionStore(db *sharedDB.Database) *PostgresProjectionStore {
	log.Trace("NewPostgresProjectionStore")

	return &PostgresProjectionStore{Database: *db}
}

func (s *PostgresProjectionStore) Upsert(ctx context.Context, tenant *sharedDomain.Tenant) error {
	log.Trace("PostgresProjectionStore Upsert")

	ctx = ctxutil.WithSystemContext(ctx)
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO `+s.Name+`.tenants (id, email, huggingface_token_ciphertext, deleted, updated_at)
		VALUES (@id, @email, @huggingface_token_ciphertext, false, now())
		ON CONFLICT (id)
		DO UPDATE SET
			email = EXCLUDED.email,
			huggingface_token_ciphertext = EXCLUDED.huggingface_token_ciphertext,
			deleted = false,
			updated_at = now();`,
		tenantDAO(tenant),
	)
	if err != nil {
		return fmt.Errorf("upsert tenant projection: %w", err)
	}
	return nil
}

func (s *PostgresProjectionStore) Delete(ctx context.Context, tenantID uuid.UUID) error {
	log.Trace("PostgresProjectionStore Delete")

	ctx = ctxutil.WithSystemContext(ctx)
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO `+s.Name+`.tenants (id, deleted, updated_at)
		VALUES (@id, true, now())
		ON CONFLICT (id)
		DO UPDATE SET deleted = true, updated_at = now();`,
		pgx.NamedArgs{
			"id": pgtype.UUID{Bytes: tenantID, Valid: true},
		},
	)
	if err != nil {
		return fmt.Errorf("delete tenant projection: %w", err)
	}
	return nil
}

func (s *PostgresProjectionStore) Read(ctx context.Context, tenantID uuid.UUID) (*sharedDomain.Tenant, error) {
	log.Trace("PostgresProjectionStore Read")

	ctx = ctxutil.WithSystemContext(ctx)
	var tenant sharedDomain.Tenant
	var tenantIDText string
	err := s.Pool.QueryRow(ctx, `
		SELECT id::text, email, huggingface_token_ciphertext, deleted, updated_at
		FROM `+s.Name+`.tenants
		WHERE id = @id AND deleted = false;`,
		pgx.NamedArgs{
			"id": pgtype.UUID{Bytes: tenantID, Valid: true},
		},
	).Scan(&tenantIDText, &tenant.Email, &tenant.HuggingFaceTokenCiphertext, &tenant.Deleted, &tenant.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read tenant projection: %w", err)
	}
	parsed, err := uuid.Parse(strings.TrimSpace(tenantIDText))
	if err != nil {
		return nil, fmt.Errorf("read tenant projection returned invalid id: %w", err)
	}
	tenant.TenantID = parsed
	return &tenant, nil
}

func tenantDAO(tenant *sharedDomain.Tenant) pgx.NamedArgs {
	log.Trace("tenantDAO")

	return pgx.NamedArgs{
		"id":                           pgtype.UUID{Bytes: tenant.TenantID, Valid: true},
		"email":                        pgtype.Text{String: strings.TrimSpace(tenant.Email), Valid: true},
		"huggingface_token_ciphertext": pgtype.Text{String: strings.TrimSpace(tenant.HuggingFaceTokenCiphertext), Valid: true},
	}
}
