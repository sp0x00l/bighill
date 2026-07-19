package app

import (
	"context"

	shareduow "lib/shared_lib/uow"
	"tool_catalog_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ToolCatalogRepository interface {
	UpsertCapabilityVersion(ctx context.Context, tx pgx.Tx, capability *model.ToolCapabilityVersion) (*model.ToolCapabilityVersion, error)
	ReadCapabilityVersion(ctx context.Context, capabilityVersionID uuid.UUID) (*model.ToolCapabilityVersion, error)
	ReadCapabilityByCapabilityID(ctx context.Context, capabilityID string) (*model.ToolCapabilityVersion, error)
	UpsertTenantGrant(ctx context.Context, tx pgx.Tx, grant *model.TenantCapabilityGrant) (*model.TenantCapabilityGrant, error)
	UpsertCredentialBinding(ctx context.Context, tx pgx.Tx, binding *model.ToolCredentialBinding) (*model.ToolCredentialBinding, error)
}

type ToolCatalogUnitOfWork interface {
	Do(ctx context.Context, fn shareduow.TxFunc) error
}

type ToolCatalogEventBuilder interface {
	CapabilityUpdatedMessage(capability *model.ToolCapabilityVersion) shareduow.OutboundMessage
	GrantUpdatedMessage(grant *model.TenantCapabilityGrant) shareduow.OutboundMessage
	CredentialBindingUpdatedMessage(binding *model.ToolCredentialBinding) shareduow.OutboundMessage
}

type CapabilityManifestVerifier interface {
	VerifyCapabilityManifest(ctx context.Context, command model.PublishCapabilityCommand) error
}

type ToolCatalogUsecase interface {
	PublishCapability(ctx context.Context, command model.PublishCapabilityCommand) (*model.ToolCapabilityVersion, error)
	GrantCapability(ctx context.Context, command model.GrantCapabilityCommand) (*model.TenantCapabilityGrant, error)
	BindCredential(ctx context.Context, command model.BindCredentialCommand) (*model.ToolCredentialBinding, error)
	ReadCapabilityVersion(ctx context.Context, capabilityVersionID uuid.UUID) (*model.ToolCapabilityVersion, error)
}
