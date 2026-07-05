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

func (e *lakehouseQueryEngine) Stream(ctx context.Context, ticket *flight.Ticket, outStream flight.FlightService_DoGetServer) error {
	log.Trace("lakehouseQueryEngine Stream")

	command := ticketCommand(ticket)
	if strings.TrimSpace(command) == "" {
		return streamdomain.ErrValidationFailed.Extend("flight ticket requires query command")
	}
	return e.streamCommand(ctx, command, outStream)
}

func (e *lakehouseQueryEngine) executeCommand(ctx context.Context, command string) (*QueryResult, error) {
	log.Trace("lakehouseQueryEngine executeCommand")

	query, runCtx, cancel, err := e.resolveLakehouseQuery(ctx, command)
	if err != nil {
		return nil, err
	}
	defer cancel()

	table, err := e.registryClient.ReadDatasetTable(runCtx, query.datasetID, query.userID, query.command.SnapshotID)
	if err != nil {
		return nil, err
	}
	return e.executeDatasetTable(runCtx, table, query.command.SQL)
}

func (e *lakehouseQueryEngine) streamCommand(ctx context.Context, command string, outStream flight.FlightService_DoGetServer) error {
	log.Trace("lakehouseQueryEngine streamCommand")

	query, runCtx, cancel, err := e.resolveLakehouseQuery(ctx, command)
	if err != nil {
		return err
	}
	defer cancel()

	table, err := e.registryClient.ReadDatasetTable(runCtx, query.datasetID, query.userID, query.command.SnapshotID)
	if err != nil {
		return err
	}
	return e.streamDatasetTable(runCtx, table, query.command.SQL, outStream)
}

type resolvedLakehouseQuery struct {
	command   *lakehouseQueryCommand
	datasetID uuid.UUID
	userID    uuid.UUID
}

func (e *lakehouseQueryEngine) resolveLakehouseQuery(ctx context.Context, command string) (resolvedLakehouseQuery, context.Context, context.CancelFunc, error) {
	log.Trace("lakehouseQueryEngine resolveLakehouseQuery")

	query, err := parseLakehouseQueryCommand(command)
	if err != nil {
		return resolvedLakehouseQuery{}, nil, nil, err
	}

	datasetID, err := uuid.Parse(query.DatasetID)
	if err != nil || datasetID == uuid.Nil {
		return resolvedLakehouseQuery{}, nil, nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command has invalid datasetId")
	}
	userID, err := uuid.Parse(query.UserID)
	if err != nil || userID == uuid.Nil {
		return resolvedLakehouseQuery{}, nil, nil, streamdomain.ErrValidationFailed.Extend("lakehouse query command has invalid userId")
	}

	runCtx := ctx
	cancel := func() {}
	if e.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.timeout)
	}
	return resolvedLakehouseQuery{
		command:   query,
		datasetID: datasetID,
		userID:    userID,
	}, runCtx, cancel, nil
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

func (e *lakehouseQueryEngine) streamDatasetTable(ctx context.Context, table datasetTableMetadata, sql string, outStream flight.FlightService_DoGetServer) error {
	log.Trace("lakehouseQueryEngine streamDatasetTable")

	catalogProvider := strings.ToUpper(strings.TrimSpace(table.GetCatalogProvider()))
	tableFormat := strings.ToUpper(strings.TrimSpace(table.GetTableFormat()))
	storageLocation := strings.TrimSpace(table.GetStorageLocation())

	switch {
	case catalogProvider == "LOCAL" && tableFormat == "PARQUET":
		if storageLocation == "" {
			return streamdomain.ErrValidationFailed.Extend("dataset table storage location is required")
		}
		return e.parquetEngine.streamSQLWithDataRoot(ctx, sql, storageLocation, outStream)
	case catalogProvider == "POLARIS" && tableFormat == "ICEBERG":
		return e.parquetEngine.streamIceberg(ctx, sql, table.GetTableNamespace(), table.GetTableName(), outStream)
	default:
		return streamdomain.ErrValidationFailed.Extend(fmt.Sprintf("unsupported lakehouse table %s/%s", catalogProvider, tableFormat))
	}
}

type datasetTableMetadata interface {
	GetCatalogProvider() string
	GetTableFormat() string
	GetStorageLocation() string
	GetTableNamespace() string
	GetTableName() string
}
