package data

import (
	"bufio"
	"context"
	domainErrors "data_stream_service/pkg/domain"
	"data_stream_service/pkg/infra"
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"lib/shared_lib/processrunner"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	log "github.com/sirupsen/logrus"
)

const queryEngineIPCHeader = "BHIPC001"
const queryEngineIPCFooter = "BHIPCEND"

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
		return nil, domainErrors.ErrValidationFailed.Extend("query engine binary path is required")
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

func (e *dataFusionQueryEngine) Stream(ctx context.Context, ticket *flight.Ticket, outStream flight.FlightService_DoGetServer) error {
	log.Trace("dataFusionQueryEngine Stream")

	query := ticketCommand(ticket)
	if strings.TrimSpace(query) == "" {
		return domainErrors.ErrValidationFailed.Extend("flight ticket requires query command")
	}
	return e.streamSQLWithDataRoot(ctx, query, e.dataRoot, outStream)
}

func (e *dataFusionQueryEngine) executeSQL(ctx context.Context, query string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine executeSQL")

	return e.executeSQLWithDataRoot(ctx, query, e.dataRoot)
}

func (e *dataFusionQueryEngine) executeSQLWithDataRoot(ctx context.Context, query, dataRoot string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine executeSQLWithDataRoot")

	args, err := sqlArgs(query, e.resolveDataRoot(dataRoot))
	if err != nil {
		return nil, err
	}
	return e.collectIPC(ctx, args, "run datafusion query engine")
}

func (e *dataFusionQueryEngine) streamSQLWithDataRoot(ctx context.Context, query, dataRoot string, outStream flight.FlightService_DoGetServer) error {
	log.Trace("dataFusionQueryEngine streamSQLWithDataRoot")

	args, err := sqlArgs(query, e.resolveDataRoot(dataRoot))
	if err != nil {
		return err
	}
	return e.streamIPC(ctx, args, "run datafusion query engine", outStream)
}

func (e *dataFusionQueryEngine) resolveDataRoot(dataRoot string) string {
	log.Trace("dataFusionQueryEngine resolveDataRoot")

	dataRoot = strings.TrimSpace(dataRoot)
	if dataRoot == "" || e.dataRoot == "" {
		return dataRoot
	}
	parsed, err := url.Parse(dataRoot)
	if err != nil || !strings.EqualFold(parsed.Scheme, "s3") {
		return dataRoot
	}
	if parsed.Host != "local-dev-bucket" {
		return dataRoot
	}
	key := strings.TrimPrefix(parsed.Path, "/")
	if strings.TrimSpace(key) == "" {
		return dataRoot
	}
	return filepath.Join(e.dataRoot, parsed.Host, filepath.FromSlash(key))
}

func sqlArgs(query, dataRoot string) ([]string, error) {
	log.Trace("sqlArgs")

	query = strings.TrimSpace(query)
	if query == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("query command is required")
	}
	dataRoot = strings.TrimSpace(dataRoot)

	args := []string{"--sql", query}
	if dataRoot != "" {
		args = append(args, "--data-root", dataRoot)
	}
	return args, nil
}

func (e *dataFusionQueryEngine) collectIPC(ctx context.Context, args []string, runLabel string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine collectIPC")

	var records []arrow.Record
	var schema *arrow.Schema
	var totalRecords int64
	result, err := processrunner.StreamStdout(ctx, processrunner.Command{
		Name:    e.binaryPath,
		Args:    args,
		Timeout: e.timeout,
	}, func(stdout io.Reader) error {
		var decodeErr error
		schema, totalRecords, decodeErr = e.decodeIPC(stdout, nil, func(record arrow.Record) error {
			record.Retain()
			records = append(records, record)
			return nil
		})
		return decodeErr
	})
	if err != nil {
		releaseRecords(records)
		return nil, formatCommandError(runLabel, err, result.Stderr)
	}

	return &QueryResult{
		Schema:       schema,
		Records:      records,
		TotalRecords: totalRecords,
	}, nil
}

func (e *dataFusionQueryEngine) streamIPC(ctx context.Context, args []string, runLabel string, outStream flight.FlightService_DoGetServer) error {
	log.Trace("dataFusionQueryEngine streamIPC")

	var writer *flight.Writer
	var decodeErr error
	result, err := processrunner.StreamStdout(ctx, processrunner.Command{
		Name:    e.binaryPath,
		Args:    args,
		Timeout: e.timeout,
	}, func(stdout io.Reader) error {
		_, _, decodeErr = e.decodeIPC(stdout, func(schema *arrow.Schema) error {
			writer = flight.NewRecordWriter(outStream, ipc.WithSchema(schema), ipc.WithAllocator(e.allocator))
			return nil
		}, func(record arrow.Record) error {
			return writer.Write(record)
		})
		return decodeErr
	})
	if writer != nil {
		if err := writer.Close(); decodeErr == nil && err != nil {
			decodeErr = fmt.Errorf("close flight stream writer: %w", err)
		}
	}
	if decodeErr != nil {
		return formatCommandError(runLabel, decodeErr, result.Stderr)
	}
	if err != nil {
		return formatCommandError(runLabel, err, result.Stderr)
	}
	return nil
}

func (e *dataFusionQueryEngine) executeIceberg(ctx context.Context, query, namespace, table string) (*QueryResult, error) {
	log.Trace("dataFusionQueryEngine executeIceberg")

	args, err := e.icebergArgs(query, namespace, table)
	if err != nil {
		return nil, err
	}
	return e.collectIPC(ctx, args, "run datafusion iceberg query engine")
}

func (e *dataFusionQueryEngine) streamIceberg(ctx context.Context, query, namespace, table string, outStream flight.FlightService_DoGetServer) error {
	log.Trace("dataFusionQueryEngine streamIceberg")

	args, err := e.icebergArgs(query, namespace, table)
	if err != nil {
		return err
	}
	return e.streamIPC(ctx, args, "run datafusion iceberg query engine", outStream)
}

func (e *dataFusionQueryEngine) icebergArgs(query, namespace, table string) ([]string, error) {
	log.Trace("dataFusionQueryEngine icebergArgs")

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
	return args, nil
}

func (e *dataFusionQueryEngine) decodeIPC(input io.Reader, onSchema func(*arrow.Schema) error, onRecord func(arrow.Record) error) (*arrow.Schema, int64, error) {
	log.Trace("dataFusionQueryEngine decodeIPC")

	buffered := bufio.NewReader(input)
	header := make([]byte, len(queryEngineIPCHeader))
	if _, err := io.ReadFull(buffered, header); err != nil {
		return nil, 0, fmt.Errorf("read query engine envelope header: %w", err)
	}
	if string(header) != queryEngineIPCHeader {
		return nil, 0, fmt.Errorf("read query engine envelope header: invalid magic %q", string(header))
	}

	rowCountBytes := make([]byte, 8)
	if _, err := io.ReadFull(buffered, rowCountBytes); err != nil {
		return nil, 0, fmt.Errorf("read query engine expected row count: %w", err)
	}
	expectedRows := int64(binary.LittleEndian.Uint64(rowCountBytes))

	reader, err := ipc.NewReader(buffered, ipc.WithAllocator(e.allocator))
	if err != nil {
		return nil, 0, fmt.Errorf("read query engine arrow stream: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if onSchema != nil {
		if err := onSchema(schema); err != nil {
			return nil, 0, err
		}
	}

	var totalRecords int64
	for reader.Next() {
		record := reader.Record()
		totalRecords += record.NumRows()
		if onRecord != nil {
			if err := onRecord(record); err != nil {
				return nil, 0, err
			}
		}
	}
	if err := reader.Err(); err != nil {
		return nil, 0, fmt.Errorf("read query engine record batch: %w", err)
	}

	footer := make([]byte, len(queryEngineIPCFooter))
	if _, err := io.ReadFull(buffered, footer); err != nil {
		return nil, 0, fmt.Errorf("read query engine envelope footer: %w", err)
	}
	if string(footer) != queryEngineIPCFooter {
		return nil, 0, fmt.Errorf("read query engine envelope footer: invalid magic %q", string(footer))
	}
	if _, err := buffered.Peek(1); err != io.EOF {
		if err == nil {
			return nil, 0, fmt.Errorf("read query engine envelope footer: unexpected trailing stdout bytes")
		}
		return nil, 0, fmt.Errorf("read query engine envelope footer: %w", err)
	}
	if totalRecords != expectedRows {
		return nil, 0, fmt.Errorf("query engine row count mismatch: expected %d records, decoded %d", expectedRows, totalRecords)
	}

	return schema, totalRecords, nil
}

func formatCommandError(runLabel string, err error, stderr string) error {
	log.Trace("formatCommandError")

	details := strings.TrimSpace(stderr)
	if details != "" {
		return fmt.Errorf("%s: %w: %s", runLabel, err, details)
	}
	return fmt.Errorf("%s: %w", runLabel, err)
}

func releaseRecords(records []arrow.Record) {
	log.Trace("releaseRecords")

	for _, record := range records {
		record.Release()
	}
}
