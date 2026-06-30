package client

import (
	"context"
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type LocalCatalogClient struct{}

func NewLocalCatalogClient() *LocalCatalogClient {
	log.Trace("NewLocalCatalogClient")
	return &LocalCatalogClient{}
}

func (c *LocalCatalogClient) CreateResource(ctx context.Context, name string, sourceConnCfg model.ConnectorConfig) (uuid.UUID, error) {
	log.WithContext(ctx).WithField("catalog_resource_name", name).Trace("LocalCatalogClient CreateResource")
	return uuid.New(), nil
}

func (c *LocalCatalogClient) ReplaceResource(ctx context.Context, name string, catalogID uuid.UUID, sourceConnCfg model.ConnectorConfig) error {
	log.WithContext(ctx).WithFields(log.Fields{
		"catalog_resource_name": name,
		"catalog_id":            catalogID.String(),
	}).Trace("LocalCatalogClient ReplaceResource")
	return nil
}

func (c *LocalCatalogClient) DeleteResource(ctx context.Context, catalogID uuid.UUID) error {
	log.WithContext(ctx).WithField("catalog_id", catalogID.String()).Trace("LocalCatalogClient DeleteResource")
	return nil
}
