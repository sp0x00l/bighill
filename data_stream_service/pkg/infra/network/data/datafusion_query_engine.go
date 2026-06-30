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

const defaultDataFusionBinaryPath = "../query_engine/datafusion_query_engine/target/release/datafusion_query_engine"

type dataFusionQueryEngine struct {
	allocator  memory.Allocator
	binaryPath string
	dataRoot   string
	timeout    time.Duration
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

	query = strings.TrimSpace(query)
	if query == "" {
		return nil, domainErrors.ErrValidationFailed.Extend("query command is required")
	}

	runCtx := ctx
	cancel := func() {}
	if e.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.timeout)
	}
	defer cancel()

	args := []string{"--sql", query}
	if e.dataRoot != "" {
		args = append(args, "--data-root", e.dataRoot)
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
