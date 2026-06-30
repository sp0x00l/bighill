package database

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5/pgxpool"
	logrus "github.com/sirupsen/logrus"
)

type Database struct {
	Pool ConnectionPool
	Name string
}

func InitDatabase(ctx context.Context, dbName, connection string, logs *logrus.Logger) (*Database, error) {
	logrus.Trace("InitDatabase")
	if !isSafeDatabaseName(dbName) {
		return nil, fmt.Errorf("invalid database/schema name %q", dbName)
	}

	conn, err := NewPgxConnection(ctx, dbName, connection, logs)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection pool: %w", err)
	}

	return NewDatabase(conn, dbName), nil
}

func NewDatabase(pool ConnectionPool, dbName string) *Database {
	logrus.Trace("NewDatabase")
	if !isSafeDatabaseName(dbName) {
		panic(fmt.Sprintf("invalid database/schema name %q", dbName))
	}

	return &Database{
		Pool: pool,
		Name: dbName,
	}
}

func isSafeDatabaseName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func (db *Database) Close() {
	logrus.Trace(fmt.Sprintf("%s Database Close", db.Name))
	db.Pool.Close()
}

func (db *Database) LogPoolStats(ctx context.Context, msg string, err error) {
	statsPool, ok := db.Pool.(*pgxpool.Pool)
	if !ok || statsPool == nil {
		return
	}

	stats := statsPool.Stat()
	fields := logrus.Fields{
		"database-name":    db.Name,
		"db_pool_acquired": stats.AcquiredConns(),
		"db_pool_idle":     stats.IdleConns(),
		"db_pool_total":    stats.TotalConns(),
		"db_pool_max":      stats.MaxConns(),
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		fields["ctx_err"] = ctxErr.Error()
	}
	if deadline, ok := ctx.Deadline(); ok {
		fields["ctx_deadline"] = deadline.UTC().Format(time.RFC3339Nano)
		fields["ctx_timeout"] = time.Until(deadline).String()
	}

	logrus.WithContext(ctx).WithFields(fields).Warn(msg)
}

func (db *Database) LogPoolStatsOnError(ctx context.Context, msg string, err error) {
	if err == nil {
		return
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		db.LogPoolStats(ctx, msg, err)
	}
}
