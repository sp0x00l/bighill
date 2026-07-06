package materialization

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"
	domainmodel "feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/processrunner"

	log "github.com/sirupsen/logrus"
)

const (
	icebergStagingDirPattern  = "bighill-iceberg-write-*"
	icebergStagingParquetFile = "data.parquet"
	icebergSourceParquet      = "parquet"
)

const (
	icebergArgMode              = "--mode"
	icebergArgModeWrite         = "write-iceberg"
	icebergArgSource            = "--source"
	icebergArgDataRoot          = "--data-root"
	icebergArgCatalog           = "--catalog"
	icebergArgCatalogPolaris    = "polaris"
	icebergArgCatalogURI        = "--catalog-uri"
	icebergArgCatalogName       = "--catalog-name"
	icebergArgWarehouse         = "--warehouse"
	icebergArgNamespace         = "--namespace"
	icebergArgTable             = "--table"
	icebergArgCatalogCredential = "--catalog-credential"
	icebergArgCatalogToken      = "--catalog-token"
	icebergArgCatalogScope      = "--catalog-scope"
	icebergArgS3Endpoint        = "--s3-endpoint"
	icebergArgS3AccessKeyID     = "--s3-access-key-id"
	icebergArgS3SecretAccessKey = "--s3-secret-access-key"
	icebergArgS3Region          = "--s3-region"
	icebergArgS3PathStyle       = "--s3-path-style"
)

type IcebergTableWriter interface {
	WriteTable(context.Context, IcebergTableWriteRequest) (*IcebergTableWriteResult, error)
}

type IcebergTableWriteRequest struct {
	Namespace   string
	Table       string
	ParquetData []byte
}

type IcebergTableWriteResult struct {
	Catalog    string `json:"catalog"`
	Namespace  string `json:"namespace"`
	Table      string `json:"table"`
	Warehouse  string `json:"warehouse"`
	SourceRows int64  `json:"source_rows"`
	TableRows  int64  `json:"table_rows"`
}

type ExternalIcebergTableWriterConfig struct {
	BinaryPath        string
	Timeout           time.Duration
	PolarisBaseURL    string
	PolarisCatalog    string
	PolarisWarehouse  string
	PolarisCredential string
	PolarisToken      string
	PolarisScope      string
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
	S3PathStyle       bool
}

type ExternalIcebergTableWriter struct {
	config ExternalIcebergTableWriterConfig
}

func NewExternalIcebergTableWriter(config ExternalIcebergTableWriterConfig) *ExternalIcebergTableWriter {
	log.Trace("NewExternalIcebergTableWriter")

	return &ExternalIcebergTableWriter{config: config}
}

func (w *ExternalIcebergTableWriter) WriteTable(ctx context.Context, request IcebergTableWriteRequest) (*IcebergTableWriteResult, error) {
	log.Trace("ExternalIcebergTableWriter WriteTable")

	if strings.TrimSpace(w.config.PolarisCredential) == "" && strings.TrimSpace(w.config.PolarisToken) == "" {
		return nil, domain.ErrCatalogRegister.Extend("polaris credential or token is required")
	}

	tmpDir, err := os.MkdirTemp("", icebergStagingDirPattern)
	if err != nil {
		return nil, fmt.Errorf("%w: create iceberg staging dir: %w", domain.ErrCatalogRegister, err)
	}
	defer os.RemoveAll(tmpDir)

	parquetPath := filepath.Join(tmpDir, icebergStagingParquetFile)
	if err := os.WriteFile(parquetPath, request.ParquetData, 0600); err != nil {
		return nil, fmt.Errorf("%w: stage parquet for iceberg: %w", domain.ErrCatalogRegister, err)
	}

	args := []string{
		icebergArgMode, icebergArgModeWrite,
		icebergArgSource, icebergSourceParquet,
		icebergArgDataRoot, parquetPath,
		icebergArgCatalog, icebergArgCatalogPolaris,
		icebergArgCatalogURI, w.config.PolarisBaseURL,
		icebergArgCatalogName, w.config.PolarisCatalog,
		icebergArgWarehouse, w.config.PolarisWarehouse,
		icebergArgNamespace, strings.TrimSpace(request.Namespace),
		icebergArgTable, strings.TrimSpace(request.Table),
		icebergArgCatalogCredential, w.config.PolarisCredential,
		icebergArgCatalogToken, w.config.PolarisToken,
		icebergArgCatalogScope, w.config.PolarisScope,
		icebergArgS3Endpoint, w.config.S3Endpoint,
		icebergArgS3AccessKeyID, w.config.S3AccessKeyID,
		icebergArgS3SecretAccessKey, w.config.S3SecretAccessKey,
		icebergArgS3Region, w.config.S3Region,
		icebergArgS3PathStyle, fmt.Sprintf("%t", w.config.S3PathStyle),
	}

	runResult, err := processrunner.Run(ctx, processrunner.Command{
		Name:    w.config.BinaryPath,
		Args:    args,
		Timeout: w.config.Timeout,
	})
	if err != nil {
		details := strings.TrimSpace(runResult.Stderr)
		if details != "" {
			return nil, fmt.Errorf("%w: run iceberg writer: %w: %s", domain.ErrCatalogRegister, err, details)
		}
		return nil, fmt.Errorf("%w: run iceberg writer: %w", domain.ErrCatalogRegister, err)
	}

	var writeResult IcebergTableWriteResult
	if err := json.Unmarshal(runResult.Stdout, &writeResult); err != nil {
		return nil, fmt.Errorf("%w: decode iceberg writer result: %w", domain.ErrCatalogRegister, err)
	}
	return &writeResult, nil
}

func isPolarisIceberg(catalogProvider, tableFormat string) bool {
	log.Trace("isPolarisIceberg")

	return strings.EqualFold(strings.TrimSpace(catalogProvider), domainmodel.CatalogProviderPolaris) &&
		strings.EqualFold(strings.TrimSpace(tableFormat), domainmodel.TableFormatIceberg)
}
