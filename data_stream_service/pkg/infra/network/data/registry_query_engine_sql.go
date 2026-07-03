package data

import (
	"context"
	domainErrors "data_stream_service/pkg/domain"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	mysql "github.com/go-sql-driver/mysql"
	_ "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"

	dataregistrypb "lib/data_contracts_lib/data_registry"
)

func (e *registryQueryEngine) executeMySQL(ctx context.Context, cfg *dataregistrypb.MySQLSourceConfig, query string) (*QueryResult, error) {
	log.Trace("registryQueryEngine executeMySQL")

	if cfg == nil {
		return nil, domainErrors.ErrValidationFailed.Extend("source connector does not include mysql config")
	}
	return e.executeSQL(ctx, "mysql", mysqlConnectionString(cfg), "mysql", query)
}

func (e *registryQueryEngine) executeClickHouse(ctx context.Context, cfg *dataregistrypb.ClickHouseSourceConfig, query string) (*QueryResult, error) {
	log.Trace("registryQueryEngine executeClickHouse")

	if cfg == nil {
		return nil, domainErrors.ErrValidationFailed.Extend("source connector does not include clickhouse config")
	}
	return e.executeSQL(ctx, "clickhouse", clickHouseConnectionString(cfg), "clickhouse", query)
}

func (e *registryQueryEngine) executeOracle(ctx context.Context, cfg *dataregistrypb.OracleSourceConfig, query string) (*QueryResult, error) {
	log.Trace("registryQueryEngine executeOracle")

	if cfg == nil {
		return nil, domainErrors.ErrValidationFailed.Extend("source connector does not include oracle config")
	}
	return e.executeSQL(ctx, "oracle", oracleConnectionString(cfg), "oracle", query)
}

func (e *registryQueryEngine) executeSQL(ctx context.Context, driverName, dsn, sourceName, query string) (*QueryResult, error) {
	log.Trace("registryQueryEngine executeSQL")

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s source: %w", sourceName, err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connect %s source: %w", sourceName, err)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query %s source: %w", sourceName, err)
	}
	defer rows.Close()

	result, err := e.sqlRowsToArrow(rows, sourceName)
	if err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		for _, record := range result.Records {
			record.Release()
		}
		return nil, fmt.Errorf("read %s source rows: %w", sourceName, err)
	}
	return result, nil
}

func mysqlConnectionString(cfg *dataregistrypb.MySQLSourceConfig) string {
	log.Trace("mysqlConnectionString")

	host := strings.TrimSpace(cfg.GetHostname())
	if host == "" {
		host = "localhost"
	}
	port := int(cfg.GetPort())
	if port == 0 {
		port = 3306
	}

	config := mysql.NewConfig()
	config.User = cfg.GetUsername()
	config.Passwd = cfg.GetPassword()
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%d", host, port)
	config.DBName = cfg.GetDatabaseName()
	config.ParseTime = true
	return config.FormatDSN()
}

func clickHouseConnectionString(cfg *dataregistrypb.ClickHouseSourceConfig) string {
	log.Trace("clickHouseConnectionString")

	host := strings.TrimSpace(cfg.GetHostname())
	if host == "" {
		host = "localhost"
	}
	port := int(cfg.GetPort())
	if port == 0 {
		port = 9000
	}
	databaseName := strings.TrimSpace(cfg.GetDatabaseName())
	if databaseName == "" {
		databaseName = "default"
	}

	u := &url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(cfg.GetUsername(), cfg.GetPassword()),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   "/" + databaseName,
	}
	q := u.Query()
	q.Set("dial_timeout", "5s")
	q.Set("read_timeout", "30s")
	u.RawQuery = q.Encode()
	return u.String()
}

func oracleConnectionString(cfg *dataregistrypb.OracleSourceConfig) string {
	log.Trace("oracleConnectionString")

	host := strings.TrimSpace(cfg.GetHostname())
	if host == "" {
		host = "localhost"
	}
	port := int(cfg.GetPort())
	if port == 0 {
		port = 1521
	}
	instance := strings.Trim(strings.TrimSpace(cfg.GetInstance()), "/")

	return fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		url.QueryEscape(cfg.GetUsername()),
		url.QueryEscape(cfg.GetPassword()),
		host,
		port,
		url.PathEscape(instance),
	)
}

func (e *registryQueryEngine) sqlRowsToArrow(rows *sql.Rows, sourceName string) (*QueryResult, error) {
	log.Trace("registryQueryEngine sqlRowsToArrow")

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read %s source columns: %w", sourceName, err)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%s source query returned no columns", sourceName)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("read %s source column types: %w", sourceName, err)
	}

	var values [][]any
	for rows.Next() {
		rowValues := make([]any, len(columns))
		scanTargets := make([]any, len(columns))
		for i := range rowValues {
			scanTargets[i] = &rowValues[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf("read %s source values: %w", sourceName, err)
		}
		values = append(values, rowValues)
	}

	fields := make([]arrow.Field, len(columns))
	for i, column := range columns {
		fields[i] = arrow.Field{
			Name:     column,
			Type:     sqlColumnArrowType(columnTypes, values, i),
			Nullable: true,
		}
	}

	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(e.allocator, schema)
	defer builder.Release()

	for _, row := range values {
		for i, value := range row {
			appendSQLValue(builder.Field(i), value)
		}
	}

	record := builder.NewRecord()
	return &QueryResult{
		Schema:       schema,
		Records:      []arrow.Record{record},
		TotalRecords: int64(len(values)),
	}, nil
}

func sqlColumnArrowType(columnTypes []*sql.ColumnType, values [][]any, index int) arrow.DataType {
	log.Trace("sqlColumnArrowType")

	if index < len(columnTypes) {
		dbType := strings.ToUpper(columnTypes[index].DatabaseTypeName())
		switch {
		case strings.Contains(dbType, "BOOL") || dbType == "BIT":
			return arrow.FixedWidthTypes.Boolean
		case strings.Contains(dbType, "INT"):
			return arrow.PrimitiveTypes.Int64
		case strings.Contains(dbType, "FLOAT") ||
			strings.Contains(dbType, "DOUBLE") ||
			strings.Contains(dbType, "DECIMAL") ||
			strings.Contains(dbType, "NUMERIC") ||
			strings.Contains(dbType, "NUMBER"):
			return arrow.PrimitiveTypes.Float64
		}
	}

	for _, row := range values {
		if index >= len(row) || row[index] == nil {
			continue
		}
		switch row[index].(type) {
		case bool:
			return arrow.FixedWidthTypes.Boolean
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return arrow.PrimitiveTypes.Int64
		case float32, float64:
			return arrow.PrimitiveTypes.Float64
		default:
			return arrow.BinaryTypes.String
		}
	}
	return arrow.BinaryTypes.String
}

func appendSQLValue(builder array.Builder, value any) {
	log.Trace("appendSQLValue")

	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		appendSQLBooleanValue(b, value)
	case *array.Int64Builder:
		appendSQLInt64Value(b, value)
	case *array.Float64Builder:
		appendSQLFloat64Value(b, value)
	case *array.StringBuilder:
		b.Append(sqlValueString(value))
	default:
		builder.AppendNull()
	}
}

func appendSQLBooleanValue(builder *array.BooleanBuilder, value any) {
	log.Trace("appendSQLBooleanValue")

	switch v := value.(type) {
	case bool:
		builder.Append(v)
	case []byte:
		parsed, err := strconv.ParseBool(string(v))
		if err != nil {
			builder.AppendNull()
			return
		}
		builder.Append(parsed)
	case string:
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			builder.AppendNull()
			return
		}
		builder.Append(parsed)
	default:
		builder.AppendNull()
	}
}

func appendSQLInt64Value(builder *array.Int64Builder, value any) {
	log.Trace("appendSQLInt64Value")

	switch v := value.(type) {
	case int64:
		builder.Append(v)
	case int:
		builder.Append(int64(v))
	case int32:
		builder.Append(int64(v))
	case int16:
		builder.Append(int64(v))
	case int8:
		builder.Append(int64(v))
	case uint64:
		builder.Append(int64(v))
	case uint:
		builder.Append(int64(v))
	case uint32:
		builder.Append(int64(v))
	case uint16:
		builder.Append(int64(v))
	case uint8:
		builder.Append(int64(v))
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			builder.AppendNull()
			return
		}
		builder.Append(parsed)
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			builder.AppendNull()
			return
		}
		builder.Append(parsed)
	default:
		builder.AppendNull()
	}
}

func appendSQLFloat64Value(builder *array.Float64Builder, value any) {
	log.Trace("appendSQLFloat64Value")

	switch v := value.(type) {
	case float64:
		builder.Append(v)
	case float32:
		builder.Append(float64(v))
	case int64:
		builder.Append(float64(v))
	case int:
		builder.Append(float64(v))
	case []byte:
		parsed, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			builder.AppendNull()
			return
		}
		builder.Append(parsed)
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			builder.AppendNull()
			return
		}
		builder.Append(parsed)
	default:
		builder.AppendNull()
	}
}

func sqlValueString(value any) string {
	log.Trace("sqlValueString")

	switch v := value.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}
