package client

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"strings"

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
	return parseLocalCatalogResourceName(name)
}

func (c *LocalCatalogClient) ReplaceResource(ctx context.Context, name string, catalogID uuid.UUID, sourceConnCfg model.ConnectorConfig) error {
	log.WithContext(ctx).WithFields(log.Fields{
		"catalog_resource_name": name,
		"catalog_id":            catalogID.String(),
	}).Trace("LocalCatalogClient ReplaceResource")
	resourceID, err := parseLocalCatalogResourceName(name)
	if err != nil {
		return err
	}
	if catalogID == uuid.Nil || catalogID != resourceID {
		return domainErrors.ErrValidationFailed.Extend("local catalog id must match resource name")
	}
	return nil
}

func (c *LocalCatalogClient) DeleteResource(ctx context.Context, catalogID uuid.UUID) error {
	log.WithContext(ctx).WithField("catalog_id", catalogID.String()).Trace("LocalCatalogClient DeleteResource")
	if catalogID == uuid.Nil {
		return domainErrors.ErrValidationFailed.Extend("local catalog id is required")
	}
	return nil
}

func (c *LocalCatalogClient) ValidateDatasetTable(ctx context.Context, dataset *model.Dataset) error {
	log.WithContext(ctx).Trace("LocalCatalogClient ValidateDatasetTable")

	if dataset == nil {
		return nil
	}
	if dataset.CatalogProvider == model.PolarisCatalog || dataset.TableFormat == model.Iceberg {
		return domainErrors.ErrValidationFailed.Extend("local catalog cannot validate polaris iceberg tables")
	}
	return nil
}

func parseLocalCatalogResourceName(name string) (uuid.UUID, error) {
	log.Trace("parseLocalCatalogResourceName")

	resourceID, err := uuid.Parse(strings.TrimSpace(name))
	if err != nil || resourceID == uuid.Nil {
		return uuid.Nil, domainErrors.ErrValidationFailed.Extend("local catalog resource name must be a uuid")
	}
	return resourceID, nil
}
