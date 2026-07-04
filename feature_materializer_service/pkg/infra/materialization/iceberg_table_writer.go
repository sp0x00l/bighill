package materialization

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"feature_materializer_service/pkg/domain"

	log "github.com/sirupsen/logrus"
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

	tmpDir, err := os.MkdirTemp("", "bighill-iceberg-write-*")
	if err != nil {
		return nil, fmt.Errorf("%w: create iceberg staging dir: %w", domain.ErrCatalogRegister, err)
	}
	defer os.RemoveAll(tmpDir)

	parquetPath := filepath.Join(tmpDir, "data.parquet")
	if err := os.WriteFile(parquetPath, request.ParquetData, 0600); err != nil {
		return nil, fmt.Errorf("%w: stage parquet for iceberg: %w", domain.ErrCatalogRegister, err)
	}

	runCtx := ctx
	cancel := func() {}
	if w.config.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, w.config.Timeout)
	}
	defer cancel()

	args := []string{
		"--mode", "write-iceberg",
		"--source", "parquet",
		"--data-root", parquetPath,
		"--catalog", "polaris",
		"--catalog-uri", w.config.PolarisBaseURL,
		"--catalog-name", w.config.PolarisCatalog,
		"--warehouse", w.config.PolarisWarehouse,
		"--namespace", strings.TrimSpace(request.Namespace),
		"--table", strings.TrimSpace(request.Table),
		"--catalog-credential", w.config.PolarisCredential,
		"--catalog-token", w.config.PolarisToken,
		"--catalog-scope", w.config.PolarisScope,
		"--s3-endpoint", w.config.S3Endpoint,
		"--s3-access-key-id", w.config.S3AccessKeyID,
		"--s3-secret-access-key", w.config.S3SecretAccessKey,
		"--s3-region", w.config.S3Region,
		"--s3-path-style", fmt.Sprintf("%t", w.config.S3PathStyle),
	}

	cmd := exec.CommandContext(runCtx, w.config.BinaryPath, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		details := strings.TrimSpace(stderr.String())
		if details != "" {
			return nil, fmt.Errorf("%w: run iceberg writer: %w: %s", domain.ErrCatalogRegister, err, details)
		}
		return nil, fmt.Errorf("%w: run iceberg writer: %w", domain.ErrCatalogRegister, err)
	}

	var result IcebergTableWriteResult
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("%w: decode iceberg writer result: %w", domain.ErrCatalogRegister, err)
	}
	return &result, nil
}

func isPolarisIceberg(catalogProvider, tableFormat string) bool {
	log.Trace("isPolarisIceberg")

	return strings.EqualFold(strings.TrimSpace(catalogProvider), "POLARIS") &&
		strings.EqualFold(strings.TrimSpace(tableFormat), "ICEBERG")
}
