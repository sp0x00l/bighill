package app

import (
	"context"

	"tool_execution_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type toolCatalogProjectionUsecase struct {
	repository ToolCatalogProjectionRepository
}

func NewToolCatalogProjectionUsecase(repository ToolCatalogProjectionRepository) ToolCatalogProjectionUsecase {
	log.Trace("NewToolCatalogProjectionUsecase")

	return &toolCatalogProjectionUsecase{repository: repository}
}

func (u *toolCatalogProjectionUsecase) ApplyCapabilityProjection(ctx context.Context, projection model.ToolCapabilityProjection) error {
	log.Trace("toolCatalogProjectionUsecase ApplyCapabilityProjection")

	return u.repository.ApplyCapabilityProjection(ctx, projection)
}

func (u *toolCatalogProjectionUsecase) ApplyGrantProjection(ctx context.Context, projection model.ToolGrantProjection) error {
	log.Trace("toolCatalogProjectionUsecase ApplyGrantProjection")

	return u.repository.ApplyGrantProjection(ctx, projection)
}

func (u *toolCatalogProjectionUsecase) ApplyCredentialBindingProjection(ctx context.Context, projection model.ToolCredentialBindingProjection) error {
	log.Trace("toolCatalogProjectionUsecase ApplyCredentialBindingProjection")

	return u.repository.ApplyCredentialBindingProjection(ctx, projection)
}
