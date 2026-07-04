package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type PolarisCatalogConfig struct {
	BaseURL             string
	TokenURL            string
	ClientID            string
	ClientSecret        string
	Scope               string
	Catalog             string
	DefaultBaseLocation string
	StorageRegion       string
	StorageEndpoint     string
	StoragePathStyle    bool
	Timeout             time.Duration
}

type PolarisCatalogClient struct {
	config      PolarisCatalogConfig
	transport   http.RoundTripper
	mu          sync.Mutex
	restCatalog *rest.Catalog
}

func NewPolarisCatalogClient(config PolarisCatalogConfig, client *http.Client) *PolarisCatalogClient {
	log.Trace("NewPolarisCatalogClient")

	if client == nil {
		client = &http.Client{Timeout: config.Timeout}
	}
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &PolarisCatalogClient{
		config:    normalizePolarisCatalogConfig(config),
		transport: transport,
	}
}

func (c *PolarisCatalogClient) CreateResource(ctx context.Context, name string, sourceConnCfg model.ConnectorConfig) (uuid.UUID, error) {
	log.WithContext(ctx).WithField("catalog_resource_name", name).Trace("PolarisCatalogClient CreateResource")

	resourceID, err := parseLocalCatalogResourceName(name)
	if err != nil {
		return uuid.Nil, err
	}
	if err := c.EnsureCatalog(ctx); err != nil {
		return uuid.Nil, err
	}
	if err := c.EnsureNamespace(ctx, catalogResourceNamespace(resourceID)); err != nil {
		return uuid.Nil, err
	}
	return resourceID, nil
}

func (c *PolarisCatalogClient) ReplaceResource(ctx context.Context, name string, catalogID uuid.UUID, sourceConnCfg model.ConnectorConfig) error {
	log.WithContext(ctx).WithFields(log.Fields{
		"catalog_resource_name": name,
		"catalog_id":            catalogID.String(),
	}).Trace("PolarisCatalogClient ReplaceResource")

	resourceID, err := parseLocalCatalogResourceName(name)
	if err != nil {
		return err
	}
	if catalogID == uuid.Nil || catalogID != resourceID {
		return domainErrors.ErrValidationFailed.Extend("polaris catalog id must match resource name")
	}
	if err := c.EnsureCatalog(ctx); err != nil {
		return err
	}
	return c.EnsureNamespace(ctx, catalogResourceNamespace(resourceID))
}

func (c *PolarisCatalogClient) DeleteResource(ctx context.Context, catalogID uuid.UUID) error {
	log.WithContext(ctx).WithField("catalog_id", catalogID.String()).Trace("PolarisCatalogClient DeleteResource")

	if catalogID == uuid.Nil {
		return domainErrors.ErrValidationFailed.Extend("polaris catalog id is required")
	}
	return c.DeleteNamespace(ctx, catalogResourceNamespace(catalogID))
}

func (c *PolarisCatalogClient) ValidateDatasetTable(ctx context.Context, dataset *model.Dataset) error {
	log.Trace("PolarisCatalogClient ValidateDatasetTable")

	if dataset == nil {
		return nil
	}
	if dataset.CatalogProvider == model.PolarisCatalog && dataset.TableFormat != model.Iceberg {
		return domainErrors.ErrValidationFailed.Extend("polaris catalog requires iceberg table format")
	}
	if dataset.TableFormat == model.Iceberg && dataset.CatalogProvider != model.PolarisCatalog {
		return domainErrors.ErrValidationFailed.Extend("iceberg table format requires polaris catalog")
	}
	if dataset.CatalogProvider != model.PolarisCatalog {
		return nil
	}
	if err := c.EnsureNamespace(ctx, dataset.TableNamespace); err != nil {
		return err
	}
	return c.LoadTable(ctx, dataset.TableNamespace, dataset.TableName)
}

func (c *PolarisCatalogClient) EnsureCatalog(ctx context.Context) error {
	log.Trace("PolarisCatalogClient EnsureCatalog")

	_, err := c.catalog(ctx)
	return err
}

func (c *PolarisCatalogClient) EnsureNamespace(ctx context.Context, namespace string) error {
	log.Trace("PolarisCatalogClient EnsureNamespace")

	catalogClient, err := c.catalog(ctx)
	if err != nil {
		return err
	}
	namespaceIdent, err := namespaceIdent(namespace)
	if err != nil {
		return err
	}
	exists, err := catalogClient.CheckNamespaceExists(ctx, namespaceIdent)
	if err != nil {
		return fmt.Errorf("check polaris namespace: %w", err)
	}
	if exists {
		return nil
	}
	if err := catalogClient.CreateNamespace(ctx, namespaceIdent, iceberg.Properties{}); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return fmt.Errorf("create polaris namespace: %w", err)
	}
	return nil
}

func (c *PolarisCatalogClient) LoadTable(ctx context.Context, namespace, tableName string) error {
	log.Trace("PolarisCatalogClient LoadTable")

	catalogClient, err := c.catalog(ctx)
	if err != nil {
		return err
	}
	tableIdent, err := tableIdent(namespace, tableName)
	if err != nil {
		return err
	}
	exists, err := catalogClient.CheckTableExists(ctx, tableIdent)
	if err != nil {
		return fmt.Errorf("check polaris table: %w", err)
	}
	if exists {
		return nil
	}
	return domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("polaris iceberg table %s.%s is not registered", namespace, tableName))
}

func (c *PolarisCatalogClient) DeleteNamespace(ctx context.Context, namespace string) error {
	log.Trace("PolarisCatalogClient DeleteNamespace")

	catalogClient, err := c.catalog(ctx)
	if err != nil {
		return err
	}
	namespaceIdent, err := namespaceIdent(namespace)
	if err != nil {
		return err
	}
	if err := catalogClient.DropNamespace(ctx, namespaceIdent); err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			return nil
		}
		return fmt.Errorf("delete polaris namespace: %w", err)
	}
	return nil
}

func (c *PolarisCatalogClient) catalog(ctx context.Context) (*rest.Catalog, error) {
	log.Trace("PolarisCatalogClient catalog")

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.restCatalog != nil {
		return c.restCatalog, nil
	}

	ctx, cancel := c.contextWithTimeout(ctx)
	defer cancel()

	options := []rest.Option{
		rest.WithWarehouseLocation(c.config.DefaultBaseLocation),
		rest.WithPrefix(c.config.Catalog),
		rest.WithScope(c.config.Scope),
		rest.WithCustomTransport(c.transport),
	}
	if c.config.ClientID != "" || c.config.ClientSecret != "" {
		options = append(options, rest.WithCredential(c.config.ClientID+":"+c.config.ClientSecret))
	}
	if c.config.TokenURL != "" {
		tokenURL, err := url.Parse(c.config.TokenURL)
		if err != nil {
			return nil, fmt.Errorf("parse polaris token url: %w", err)
		}
		options = append(options, rest.WithAuthURI(tokenURL))
	}

	catalogClient, err := rest.NewCatalog(ctx, c.config.Catalog, normalizeCatalogURI(c.config.BaseURL), options...)
	if err != nil {
		return nil, fmt.Errorf("load polaris catalog: %w", err)
	}
	c.restCatalog = catalogClient
	return catalogClient, nil
}

func (c *PolarisCatalogClient) contextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	log.Trace("PolarisCatalogClient contextWithTimeout")

	if c.config.Timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.config.Timeout)
}

func namespaceIdent(namespace string) (table.Identifier, error) {
	log.Trace("namespaceIdent")

	parts := namespaceParts(namespace)
	if len(parts) == 0 {
		return nil, domainErrors.ErrValidationFailed.Extend("polaris namespace is required")
	}
	return table.Identifier(parts), nil
}

func tableIdent(namespace string, tableName string) (table.Identifier, error) {
	log.Trace("tableIdent")

	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("polaris table reference is required")
	}
	parts := namespaceParts(namespace)
	if len(parts) == 0 {
		return nil, domainErrors.ErrValidationFailed.Extend("polaris table reference is required")
	}
	return table.Identifier(append(parts, tableName)), nil
}

func namespaceParts(namespace string) []string {
	log.Trace("namespaceParts")

	out := []string{}
	for _, part := range strings.Split(strings.TrimSpace(namespace), ".") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func normalizeCatalogURI(uri string) string {
	log.Trace("normalizeCatalogURI")

	uri = strings.TrimRight(strings.TrimSpace(uri), "/")
	if strings.HasSuffix(uri, "/api/catalog") {
		return uri
	}
	return uri + "/api/catalog"
}

func normalizeTokenURL(baseURL string, tokenURL string) string {
	log.Trace("normalizeTokenURL")

	tokenURL = strings.TrimSpace(tokenURL)
	if tokenURL != "" {
		return tokenURL
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if strings.HasSuffix(baseURL, "/api/catalog") {
		return baseURL + "/v1/oauth/tokens"
	}
	if baseURL != "" {
		return baseURL + "/api/catalog/v1/oauth/tokens"
	}
	return ""
}

func normalizePolarisCatalogConfig(config PolarisCatalogConfig) PolarisCatalogConfig {
	log.Trace("normalizePolarisCatalogConfig")

	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	config.TokenURL = normalizeTokenURL(config.BaseURL, config.TokenURL)
	config.ClientID = strings.TrimSpace(config.ClientID)
	config.ClientSecret = strings.TrimSpace(config.ClientSecret)
	config.Scope = strings.TrimSpace(config.Scope)
	config.Catalog = strings.TrimSpace(config.Catalog)
	config.DefaultBaseLocation = strings.TrimSpace(config.DefaultBaseLocation)
	config.StorageRegion = strings.TrimSpace(config.StorageRegion)
	config.StorageEndpoint = strings.TrimSpace(config.StorageEndpoint)
	return config
}

func catalogResourceNamespace(resourceID uuid.UUID) string {
	return "source_connector_" + strings.ReplaceAll(resourceID.String(), "-", "_")
}
