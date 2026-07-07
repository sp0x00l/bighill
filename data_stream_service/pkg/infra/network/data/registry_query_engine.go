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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	dataregistrypb "lib/data_contracts_lib/data_registry"

	log "github.com/sirupsen/logrus"
)

type registryQueryEngine struct {
	allocator      memory.Allocator
	registryClient registrygrpc.DataRegistryClient
	timeout        time.Duration
}

func NewRegistryQueryEngine(config infra.QueryEngineConfig) (QueryEngine, error) {
	log.Trace("NewRegistryQueryEngine")

	registryClient, err := registrygrpc.NewDataRegistryClient(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &registryQueryEngine{
		allocator:      memory.NewGoAllocator(),
		registryClient: registryClient,
		timeout:        queryEngineTimeout(config),
	}, nil
}

func NewRegistryQueryEngineWithClient(client registrygrpc.DataRegistryClient, timeout time.Duration) QueryEngine {
	log.Trace("NewRegistryQueryEngineWithClient")

	return &registryQueryEngine{
		allocator:      memory.NewGoAllocator(),
		registryClient: client,
		timeout:        timeout,
	}
}

func (e *registryQueryEngine) Close() error {
	log.Trace("registryQueryEngine Close")

	if e.registryClient == nil {
		return nil
	}
	return e.registryClient.Close()
}

func (e *registryQueryEngine) GetSchema(ctx context.Context, descriptor *flight.FlightDescriptor) (*arrow.Schema, error) {
	log.Trace("registryQueryEngine GetSchema")

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

func (e *registryQueryEngine) Execute(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	log.Trace("registryQueryEngine Execute")

	command := ticketCommand(ticket)
	if strings.TrimSpace(command) == "" {
		return nil, streamdomain.ErrValidationFailed.Extend("flight ticket requires query command")
	}
	return e.executeCommand(ctx, command)
}

func (e *registryQueryEngine) executeCommand(ctx context.Context, command string) (*QueryResult, error) {
	log.Trace("registryQueryEngine executeCommand")

	query, err := parseSourceQueryCommand(command)
	if err != nil {
		return nil, err
	}

	connectorID, err := uuid.Parse(query.SourceConnectorID)
	if err != nil || connectorID == uuid.Nil {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command has invalid sourceConnectorId")
	}
	userID, err := uuid.Parse(query.UserID)
	if err != nil || userID == uuid.Nil {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command has invalid userId")
	}
	orgID, err := uuid.Parse(query.OrgID)
	if err != nil || orgID == uuid.Nil {
		return nil, streamdomain.ErrValidationFailed.Extend("registry query command has invalid orgId")
	}

	runCtx := ctx
	cancel := func() {}
	if e.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, e.timeout)
	}
	defer cancel()

	connector, err := e.registryClient.ReadSourceConnector(runCtx, connectorID, userID, orgID, query.SourceType.String())
	if err != nil {
		return nil, err
	}

	switch query.SourceType {
	case streamdomain.SourceTypePostgres:
		return e.executePostgres(runCtx, connector.GetPostgresConfig(), query.SQL)
	case streamdomain.SourceTypeMySQL:
		return e.executeMySQL(runCtx, connector.GetMysqlConfig(), query.SQL)
	case streamdomain.SourceTypeClickHouse:
		return e.executeClickHouse(runCtx, connector.GetClickhouseConfig(), query.SQL)
	case streamdomain.SourceTypeMongoDB:
		return e.executeMongo(runCtx, connector.GetMongoConfig(), query)
	case streamdomain.SourceTypeOracle:
		return e.executeOracle(runCtx, connector.GetOracleConfig(), query.SQL)
	default:
		return nil, streamdomain.ErrValidationFailed.Extend(fmt.Sprintf("unsupported source type %q", query.SourceType.String()))
	}
}

func (e *registryQueryEngine) executePostgres(ctx context.Context, cfg *dataregistrypb.PostgresSourceConfig, sql string) (*QueryResult, error) {
	log.Trace("registryQueryEngine executePostgres")

	if cfg == nil {
		return nil, streamdomain.ErrValidationFailed.Extend("source connector does not include postgres config")
	}
	connConfig, err := pgx.ParseConfig(postgresConnectionString(cfg))
	if err != nil {
		return nil, fmt.Errorf("parse postgres source connection config: %w", err)
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("connect postgres source: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query postgres source: %w", err)
	}
	defer rows.Close()

	result, err := e.rowsToArrow(rows)
	if err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		for _, record := range result.Records {
			record.Release()
		}
		return nil, fmt.Errorf("read postgres source rows: %w", err)
	}
	return result, nil
}

func postgresConnectionString(cfg *dataregistrypb.PostgresSourceConfig) string {
	log.Trace("postgresConnectionString")

	host := strings.TrimSpace(cfg.GetHostname())
	if host == "" {
		host = "localhost"
	}
	port := int(cfg.GetPort())
	if port == 0 {
		port = 5432
	}

	parts := []string{
		fmt.Sprintf("host=%s", host),
		fmt.Sprintf("port=%d", port),
		fmt.Sprintf("dbname=%s", cfg.GetDatabaseName()),
		"sslmode=disable",
	}
	if cfg.GetUsername() != "" {
		parts = append(parts, fmt.Sprintf("user=%s", cfg.GetUsername()))
	}
	if cfg.GetPassword() != "" {
		parts = append(parts, fmt.Sprintf("password=%s", cfg.GetPassword()))
	}
	return strings.Join(parts, " ")
}

func (e *registryQueryEngine) rowsToArrow(rows pgx.Rows) (*QueryResult, error) {
	log.Trace("registryQueryEngine rowsToArrow")

	fields := rows.FieldDescriptions()
	if len(fields) == 0 {
		return nil, fmt.Errorf("postgres source query returned no columns")
	}

	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		arrowFields[i] = arrow.Field{
			Name:     string(field.Name),
			Type:     postgresOIDToArrowType(field.DataTypeOID),
			Nullable: true,
		}
	}

	schema := arrow.NewSchema(arrowFields, nil)
	builder := array.NewRecordBuilder(e.allocator, schema)
	defer builder.Release()

	var rowCount int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read postgres source values: %w", err)
		}
		for i, value := range values {
			appendPostgresValue(builder.Field(i), value)
		}
		rowCount++
	}

	record := builder.NewRecord()
	return &QueryResult{
		Schema:       schema,
		Records:      []arrow.Record{record},
		TotalRecords: rowCount,
	}, nil
}

func postgresOIDToArrowType(oid uint32) arrow.DataType {
	log.Trace("postgresOIDToArrowType")

	switch oid {
	case 16:
		return arrow.FixedWidthTypes.Boolean
	case 21:
		return arrow.PrimitiveTypes.Int16
	case 23:
		return arrow.PrimitiveTypes.Int32
	case 20:
		return arrow.PrimitiveTypes.Int64
	case 700:
		return arrow.PrimitiveTypes.Float32
	case 701:
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}

func appendPostgresValue(builder array.Builder, value any) {
	log.Trace("appendPostgresValue")

	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		v, ok := value.(bool)
		if !ok {
			b.AppendNull()
			return
		}
		b.Append(v)
	case *array.Int16Builder:
		b.Append(int16Value(value))
	case *array.Int32Builder:
		b.Append(int32Value(value))
	case *array.Int64Builder:
		b.Append(int64Value(value))
	case *array.Float32Builder:
		b.Append(float32Value(value))
	case *array.Float64Builder:
		b.Append(float64Value(value))
	case *array.StringBuilder:
		b.Append(fmt.Sprint(value))
	default:
		builder.AppendNull()
	}
}

func int16Value(value any) int16 {
	log.Trace("int16Value")

	switch v := value.(type) {
	case int16:
		return v
	case int32:
		return int16(v)
	case int64:
		return int16(v)
	case int:
		return int16(v)
	default:
		return 0
	}
}

func int32Value(value any) int32 {
	log.Trace("int32Value")

	switch v := value.(type) {
	case int16:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	case int:
		return int32(v)
	default:
		return 0
	}
}

func int64Value(value any) int64 {
	log.Trace("int64Value")

	switch v := value.(type) {
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

func float32Value(value any) float32 {
	log.Trace("float32Value")

	switch v := value.(type) {
	case float32:
		return v
	case float64:
		return float32(v)
	default:
		return 0
	}
}

func float64Value(value any) float64 {
	log.Trace("float64Value")

	switch v := value.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0
	}
}
