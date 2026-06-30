package database

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"

	env "lib/shared_lib/env"
	metrics "lib/shared_lib/metrics"

	pgxlogrus "github.com/jackc/pgx-logrus"
	logrus "github.com/sirupsen/logrus"
)

const (
	logLevel          = "LOG_LEVEL"
	dbLogLevelEnv     = "DB_LOG_LEVEL"
	defaultDBLogLevel = "WARN"
)

type ConnectionPool interface {
	Close()
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

func NewPgxConnection(ctx context.Context, dbName, connection string, logs *logrus.Logger) (*pgxpool.Pool, error) {
	logrus.Trace("NewPgxConnection")

	start := time.Now()
	config, err := pgxpool.ParseConfig(connection)
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "connect", metrics.ClassifyDB(err), "")
		metrics.Default().RecordRequest(ctx, metrics.BoundaryDB, "connect", "ERROR")
		metrics.Default().RecordDuration(ctx, metrics.BoundaryDB, "connect", "ERROR", time.Since(start).Seconds())
		return nil, err
	}

	var dbLogLevel tracelog.LogLevel
	logLevelValue := os.Getenv(dbLogLevelEnv)
	if logLevelValue == "" {
		logLevelValue = os.Getenv(logLevel)
	}
	if logLevelValue == "" {
		logLevelValue = defaultDBLogLevel
	}
	switch strings.ToUpper(strings.TrimSpace(logLevelValue)) {
	case "TRACE", "DEBUG":
		dbLogLevel = tracelog.LogLevelTrace
	case "INFO":
		dbLogLevel = tracelog.LogLevelInfo
	case "WARN", "WARNING":
		dbLogLevel = tracelog.LogLevelWarn
	case "ERROR":
		dbLogLevel = tracelog.LogLevelError
	case "NONE", "OFF", "DISABLED":
		dbLogLevel = tracelog.LogLevelNone
	default:
		dbLogLevel = tracelog.LogLevelInfo
	}

	fieldLogger := logs.WithFields(logrus.Fields{
		"database-name": dbName,
		"log_level":     logLevelValue,
	})

	config.ConnConfig.Tracer = multitracer.New(
		newPgxOtelTracer(dbName),
		&tracelog.TraceLog{
			Logger:   pgxlogrus.NewLogger(fieldLogger),
			LogLevel: dbLogLevel,
		},
	)

	// tell the driver to sanitize statement strings
	config.ConnConfig.RuntimeParams = map[string]string{
		"standard_conforming_strings": "on",
		"timezone":                    "UTC",
	}

	statementTimeoutMs := env.WithDefaultInt("SHARED_LIB_DB_STATEMENT_TIMEOUT_MS", "15000")
	lockTimeoutMs := env.WithDefaultInt("SHARED_LIB_DB_LOCK_TIMEOUT_MS", "5000")
	idleInTxTimeoutMs := env.WithDefaultInt("SHARED_LIB_DB_IDLE_IN_TX_TIMEOUT_MS", "10000")
	config.ConnConfig.RuntimeParams["statement_timeout"] = strconv.Itoa(statementTimeoutMs)
	config.ConnConfig.RuntimeParams["lock_timeout"] = strconv.Itoa(lockTimeoutMs)
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = strconv.Itoa(idleInTxTimeoutMs)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "connect", metrics.ClassifyDB(err), "")
		metrics.Default().RecordRequest(ctx, metrics.BoundaryDB, "connect", "ERROR")
		metrics.Default().RecordDuration(ctx, metrics.BoundaryDB, "connect", "ERROR", time.Since(start).Seconds())
		return nil, err
	}

	// check connection is open
	if err := pool.Ping(ctx); err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "connect", metrics.ClassifyDB(err), "")
		metrics.Default().RecordRequest(ctx, metrics.BoundaryDB, "connect", "ERROR")
		metrics.Default().RecordDuration(ctx, metrics.BoundaryDB, "connect", "ERROR", time.Since(start).Seconds())
		return nil, err
	}

	if err := metrics.RegisterPgxPoolGauges("infra", dbName, pool); err != nil {
		logrus.WithError(err).Warn("failed to register db pool gauges")
	}
	metrics.Default().RecordRequest(ctx, metrics.BoundaryDB, "connect", "OK")
	metrics.Default().RecordDuration(ctx, metrics.BoundaryDB, "connect", "OK", time.Since(start).Seconds())
	return pool, nil
}
