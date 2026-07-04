package data

import (
	"context"
	streamdomain "data_stream_service/pkg/domain"
	"data_stream_service/pkg/infra"
	registrygrpc "data_stream_service/pkg/infra/network/grpc"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type lakehouseQueryEngine struct {
	registryClient registrygrpc.DataRegistryClient
	parquetEngine  *dataFusionQueryEngine
	timeout        time.Duration
}

func NewLakehouseQueryEngine(config infra.QueryEngineConfig) (QueryEngine, error) {
	log.Trace("NewLakehouseQueryEngine")

	registryClient, err := registrygrpc.NewDataRegistryClient(context.Background(), config)
	if err != nil {
		return nil, err
	}
	parquetEngine, err := NewDataFusionQueryEngine(config)
	if err != nil {
		registryClient.Close()
		return nil, err
	}
	return newLakehouseQueryEngine(registryClient, parquetEngine.(*dataFusionQueryEngine), queryEngineTimeout(config)), nil
}

func NewLakehouseQueryEngineWithClient(client registrygrpc.DataRegistryClient, parquetEngine *dataFusionQueryEngine, timeout time.Duration) QueryEngine {
	log.Trace("NewLakehouseQueryEngineWithClient")

	return newLakehouseQueryEngine(client, parquetEngine, timeout)
}

func newLakehouseQueryEngine(client registrygrpc.DataRegistryClient, parquetEngine *dataFusionQueryEngine, timeout time.Duration) *lakehouseQueryEngine {
	log.Trace("newLakehouseQueryEngine")

	return &lakehouseQueryEngine{
		registryClient: client,
		parquetEngine:  parquetEngine,
		timeout:        timeout,
	}
}

func (e *lakehouseQueryEngine) Close() error {
	log.Trace("lakehouseQueryEngine Close")

	if e.registryClient == nil {
		return nil
	}
	return e.registryClient.Close()
}

func (e *lakehouseQueryEngine) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*arrow.Schema, error) {
	log.Trace("lakehouseQueryEngine GetSchema")

	if err := validateDescriptor(descriptor); err != nil {
		return nil, err
	}
	result, err := e.executeCommand(ctx, descriptorCommand(descriptor))
	if err != nil {
		return nil, err
	}
	for _, record := range result.Records {
		record.Release()
	}
	return result.Schema, nil
}

func (e *lakehouseQueryEngine) Execute(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	log.Trace("lakehouseQueryEngine Execute")

	command := ticketCommand(ticket)
	if strings.TrimSpace(command) == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("flight ticket requires query command")
	}
	return e.executeCommand(ctx, command)
}

func (e *lakehouseQueryEngine) executeCommand(ctx context.Context, command string) (*QueryResult, error) {
	log.Trace("lakehouseQueryEngine executeCommand")

	query, err := parseLakehouseQueryCommand(command)
	if err != nil {
		return nil, err
	}

	datasetID, err := uuid.Parse(query.DatasetID)
	if err != nil || datasetID == uuid.Nil {
		return nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command has invalid datasetId")
	}
	userID, err := uuid.Parse(query.UserID)
	if err != nil || userID == uuid.Nil {
		return nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command has invalid userId")
	}

	runCtx := ctx
	cancel := func() {}
	if e.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.timeout)
	}
	defer cancel()

	table, err := e.registryClient.ReadDatasetTable(runCtx, datasetID, userID, query.SnapshotID)
	if err != nil {
		return nil, err
	}
	return e.executeDatasetTable(runCtx, table, query.SQL)
}

func (e *lakehouseQueryEngine) executeDatasetTable(ctx context.Context, table datasetTableMetadata, sql string) (*QueryResult, error) {
	log.Trace("lakehouseQueryEngine executeDatasetTable")

	catalogProvider := strings.ToUpper(strings.TrimSpace(table.GetCatalogProvider()))
	tableFormat := strings.ToUpper(strings.TrimSpace(table.GetTableFormat()))
	storageLocation := strings.TrimSpace(table.GetStorageLocation())

	switch {
	case catalogProvider == "LOCAL" && tableFormat == "PARQUET":
		if storageLocation == "" {
			return nil, streamdomain.ErrValidationFailed.Extend("dataset table storage location is required")
		}
		return e.parquetEngine.executeSQLWithDataRoot(ctx, sql, storageLocation)
	case catalogProvider == "POLARIS" && tableFormat == "ICEBERG":
		return e.parquetEngine.executeIceberg(ctx, sql, table.GetTableNamespace(), table.GetTableName())
	default:
		return nil, streamdomain.ErrValidationFailed.Extend(fmt.Sprintf("unsupported lakehouse table %s/%s", catalogProvider, tableFormat))
	}
}

type datasetTableMetadata interface {
	GetCatalogProvider() string
	GetTableFormat() string
	GetStorageLocation() string
	GetTableNamespace() string
	GetTableName() string
}
