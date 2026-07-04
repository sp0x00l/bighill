package data

import (
	"bytes"
	"context"
	domainErrors "data_stream_service/pkg/domain"
	"data_stream_service/pkg/infra"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	log "github.com/sirupsen/logrus"
)

const defaultDataFusionBinaryPath = "internal/infra/queryengine/datafusion_query_engine/target/release/datafusion_query_engine"

type dataFusionQueryEngine struct {
	allocator  memory.Allocator
	binaryPath string
	dataRoot   string
	timeout    time.Duration
	polaris    polarisExecutionConfig
}

type polarisExecutionConfig struct {
	BaseURL     string
	Catalog     string
	Warehouse   string
	Credential  string
	Token       string
	Scope       string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3PathStyle bool
}

func NewDataFusionQueryEngine(config infra.QueryEngineConfig) (QueryEngine, error) {
	log.Trace("NewDataFusionQueryEngine")

	binaryPath := strings.TrimSpace(config.BinaryPath)
	if binaryPath == "" {
		binaryPath = defaultDataFusionBinaryPath
	}

	return &dataFusionQueryEngine{
		allocator:  memory.NewGoAllocator(),
		binaryPath: binaryPath,
		dataRoot:   strings.TrimSpace(config.DataRoot),
		timeout:    queryEngineTimeout(config),
		polaris: polarisExecutionConfig{
			BaseURL:     strings.TrimSpace(config.PolarisBaseURL),
			Catalog:     strings.TrimSpace(config.PolarisCatalog),
			Warehouse:   strings.TrimSpace(config.PolarisWarehouse),
			Credential:  strings.TrimSpace(config.PolarisCredential),
			Token:       strings.TrimSpace(config.PolarisToken),
			Scope:       strings.TrimSpace(config.PolarisScope),
			S3Endpoint:  strings.TrimSpace(config.PolarisS3Endpoint),
			S3AccessKey: strings.TrimSpace(config.PolarisS3AccessKey),
			S3SecretKey: strings.TrimSpace(config.PolarisS3SecretKey),
			S3Region:    strings.TrimSpace(config.PolarisS3Region),
			S3PathStyle: config.PolarisS3PathStyle,
		},
	}, nil
}

func (e *dataFusionQueryEngine) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*arrow.Schema, error) {
	log.Trace("dataFusionQueryEngine GetSchema")

	if err := validateDescriptor(descriptor); err != nil {
		return nil, err
	}
	result, err := e.executeSQL(ctx, descriptorCommand(descriptor))
	if err != nil {
		return nil, err
	}
	for _, record := range result.Records {
		record.Release()
	}
	return result.Schema, nil
}

func (e *dataFusionQueryEngine) Execute(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine Execute")

	query := ticketCommand(ticket)
	if strings.TrimSpace(query) == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("flight ticket requires query command")
	}
	return e.executeSQL(ctx, query)
}

func (e *dataFusionQueryEngine) executeSQL(ctx context.Context, query string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine executeSQL")

	return e.executeSQLWithDataRoot(ctx, query, e.dataRoot)
}

func (e *dataFusionQueryEngine) executeSQLWithDataRoot(ctx context.Context, query, dataRoot string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine executeSQLWithDataRoot")

	query = strings.TrimSpace(query)
	if query == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("query command is required")
	}
	dataRoot = strings.TrimSpace(dataRoot)

	runCtx := ctx
	cancel := func() {}
	if e.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.timeout)
	}
	defer cancel()

	args := []string{"--sql", query}
	if dataRoot != "" {
		args = append(args, "--data-root", dataRoot)
	}

	cmd := exec.CommandContext(runCtx, e.binaryPath, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		details := strings.TrimSpace(stderr.String())
		if details != "" {
			return nil, fmt.Errorf("run datafusion query engine: %w: %s", err, details)
		}
		return nil, fmt.Errorf("run datafusion query engine: %w", err)
	}

	return e.decodeIPC(output)
}

func (e *dataFusionQueryEngine) executeIceberg(ctx context.Context, query, namespace, table string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine executeIceberg")

	query = strings.TrimSpace(query)
	namespace = strings.TrimSpace(namespace)
	table = strings.TrimSpace(table)
	if query == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("query command is required")
	}
	if namespace == "" || table == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("iceberg table reference is required")
	}
	if strings.TrimSpace(e.polaris.Credential) == "" && strings.TrimSpace(e.polaris.Token) == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("polaris credential or token is required")
	}

	runCtx := ctx
	cancel := func() {}
	if e.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.timeout)
	}
	defer cancel()

	args := []string{
		"--source", "iceberg",
		"--catalog", "polaris",
		"--catalog-uri", e.polaris.BaseURL,
		"--catalog-name", e.polaris.Catalog,
		"--warehouse", e.polaris.Warehouse,
		"--namespace", namespace,
		"--table", table,
		"--catalog-credential", e.polaris.Credential,
		"--catalog-token", e.polaris.Token,
		"--catalog-scope", e.polaris.Scope,
		"--s3-endpoint", e.polaris.S3Endpoint,
		"--s3-access-key-id", e.polaris.S3AccessKey,
		"--s3-secret-access-key", e.polaris.S3SecretKey,
		"--s3-region", e.polaris.S3Region,
		"--s3-path-style", fmt.Sprintf("%t", e.polaris.S3PathStyle),
		"--sql", query,
	}
	cmd := exec.CommandContext(runCtx, e.binaryPath, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		details := strings.TrimSpace(stderr.String())
		if details != "" {
			return nil, fmt.Errorf("run datafusion iceberg query engine: %w: %s", err, details)
		}
		return nil, fmt.Errorf("run datafusion iceberg query engine: %w", err)
	}
	return e.decodeIPC(output)
}

func (e *dataFusionQueryEngine) decodeIPC(output []byte) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine decodeIPC")

	reader, err := ipc.NewReader(bytes.NewReader(output), ipc.WithAllocator(e.allocator))
	if err != nil {
		return nil, fmt.Errorf("read query engine arrow stream: %w", err)
	}
	defer reader.Release()

	var records []arrow.Record
	var totalRecords int64
	for reader.Next() {
		record := reader.Record()
		record.Retain()
		records = append(records, record)
		totalRecords += record.NumRows()
	}
	if err := reader.Err(); err != nil {
		for _, record := range records {
			record.Release()
		}
		return nil, fmt.Errorf("read query engine record batch: %w", err)
	}

	return &QueryResult{
		Schema:       reader.Schema(),
		Records:      records,
		TotalRecords: totalRecords,
	}, nil
}
