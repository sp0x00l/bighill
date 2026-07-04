package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

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
	config PolarisCatalogConfig
	client *http.Client
}

func NewPolarisCatalogClient(config PolarisCatalogConfig, client *http.Client) *PolarisCatalogClient {
	log.Trace("NewPolarisCatalogClient")

	if client == nil {
		client = &http.Client{Timeout: config.Timeout}
	}
	return &PolarisCatalogClient{
		config: normalizePolarisCatalogConfig(config),
		client: client,
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
	if err := c.EnsureCatalog(ctx); err != nil {
		return err
	}
	if err := c.EnsureNamespace(ctx, dataset.TableNamespace); err != nil {
		return err
	}
	if err := c.LoadTable(ctx, dataset.TableNamespace, dataset.TableName); err != nil {
		return err
	}
	return nil
}

func (c *PolarisCatalogClient) EnsureCatalog(ctx context.Context) error {
	log.Trace("PolarisCatalogClient EnsureCatalog")

	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/management/v1/catalogs/%s", url.PathEscape(c.config.Catalog))
	resp, body, err := c.do(ctx, http.MethodGet, path, token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("read polaris catalog: status %d: %s", resp.StatusCode, body)
	}

	payload := polarisCreateCatalogRequest{
		Catalog: polarisCatalog{
			Type: "INTERNAL",
			Name: c.config.Catalog,
			Properties: map[string]string{
				"default-base-location": c.config.DefaultBaseLocation,
			},
			StorageConfigInfo: polarisStorageConfig{
				StorageType:      "S3",
				AllowedLocations: []string{c.config.DefaultBaseLocation},
				Region:           c.config.StorageRegion,
				Endpoint:         c.config.StorageEndpoint,
				PathStyleAccess:  c.config.StoragePathStyle,
			},
		},
	}
	resp, body, err = c.do(ctx, http.MethodPost, "/api/management/v1/catalogs", token, payload)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("create polaris catalog: status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (c *PolarisCatalogClient) EnsureNamespace(ctx context.Context, namespace string) error {
	log.Trace("PolarisCatalogClient EnsureNamespace")

	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return domainErrors.ErrValidationFailed.Extend("polaris namespace is required")
	}
	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"namespace":  []string{namespace},
		"properties": map[string]string{},
	}
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces", url.PathEscape(c.config.Catalog))
	resp, body, err := c.do(ctx, http.MethodPost, path, token, payload)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("create polaris namespace: status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (c *PolarisCatalogClient) LoadTable(ctx context.Context, namespace, table string) error {
	log.Trace("PolarisCatalogClient LoadTable")

	namespace = strings.TrimSpace(namespace)
	table = strings.TrimSpace(table)
	if namespace == "" || table == "" {
		return domainErrors.ErrValidationFailed.Extend("polaris table reference is required")
	}
	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces/%s/tables/%s", url.PathEscape(c.config.Catalog), url.PathEscape(namespace), url.PathEscape(table))
	resp, body, err := c.do(ctx, http.MethodGet, path, token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("polaris iceberg table %s.%s is not registered", namespace, table))
	}
	return fmt.Errorf("load polaris table: status %d: %s", resp.StatusCode, body)
}

func (c *PolarisCatalogClient) DeleteNamespace(ctx context.Context, namespace string) error {
	log.Trace("PolarisCatalogClient DeleteNamespace")

	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return domainErrors.ErrValidationFailed.Extend("polaris namespace is required")
	}
	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/api/catalog/v1/%s/namespaces/%s", url.PathEscape(c.config.Catalog), url.PathEscape(namespace))
	resp, body, err := c.do(ctx, http.MethodDelete, path, token, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("delete polaris namespace: status %d: %s", resp.StatusCode, body)
}

func (c *PolarisCatalogClient) token(ctx context.Context) (string, error) {
	log.Trace("PolarisCatalogClient token")

	values := url.Values{}
	values.Set("grant_type", "client_credentials")
	values.Set("client_id", c.config.ClientID)
	values.Set("client_secret", c.config.ClientSecret)
	if c.config.Scope != "" {
		values.Set("scope", c.config.Scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.TokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request polaris token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read polaris token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("request polaris token: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("decode polaris token response: %w", err)
	}
	if strings.TrimSpace(out.AccessToken) == "" {
		return "", fmt.Errorf("polaris token response did not include access_token")
	}
	return out.AccessToken, nil
}

func (c *PolarisCatalogClient) do(ctx context.Context, method, path, token string, payload any) (*http.Response, string, error) {
	log.Trace("PolarisCatalogClient do")

	var body io.Reader
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, "", err
		}
		body = bytes.NewReader(bodyBytes)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.config.BaseURL+path, body)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request polaris %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read polaris response: %w", err)
	}
	return resp, strings.TrimSpace(string(bytes)), nil
}

func normalizePolarisCatalogConfig(config PolarisCatalogConfig) PolarisCatalogConfig {
	log.Trace("normalizePolarisCatalogConfig")

	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	config.TokenURL = strings.TrimSpace(config.TokenURL)
	if config.TokenURL == "" && config.BaseURL != "" {
		config.TokenURL = config.BaseURL + "/api/catalog/v1/oauth/tokens"
	}
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

type polarisCreateCatalogRequest struct {
	Catalog polarisCatalog `json:"catalog"`
}

type polarisCatalog struct {
	Type              string               `json:"type"`
	Name              string               `json:"name"`
	Properties        map[string]string    `json:"properties"`
	StorageConfigInfo polarisStorageConfig `json:"storageConfigInfo"`
}

type polarisStorageConfig struct {
	StorageType      string   `json:"storageType"`
	AllowedLocations []string `json:"allowedLocations"`
	Region           string   `json:"region,omitempty"`
	Endpoint         string   `json:"endpoint,omitempty"`
	PathStyleAccess  bool     `json:"pathStyleAccess"`
}
