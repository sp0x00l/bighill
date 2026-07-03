package metrics

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func RegisterPgxPoolGauges(meterName, dbName string, pool *pgxpool.Pool) error {
	log.Trace("RegisterPgxPoolGauges")
	if pool == nil {
		return nil
	}
	if meterName == "" {
		meterName = "infra"
	}
	name := fmt.Sprintf("bighill.%s", meterName)
	meter := otel.Meter(name)
	fallbackName := fmt.Sprintf("%s-fallback", name)

	gauge := MustObservableGauge(meter, "db_pool_connections", "Database pool connections by state", "1", fallbackName)
	attrs := []attribute.KeyValue{attribute.String("db", dbName)}

	_, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		stats := pool.Stat()
		observer.ObserveInt64(gauge, int64(stats.TotalConns()), metric.WithAttributes(append(attrs, attribute.String("state", "total"))...))
		observer.ObserveInt64(gauge, int64(stats.IdleConns()), metric.WithAttributes(append(attrs, attribute.String("state", "idle"))...))
		observer.ObserveInt64(gauge, int64(stats.AcquiredConns()), metric.WithAttributes(append(attrs, attribute.String("state", "acquired"))...))
		observer.ObserveInt64(gauge, int64(stats.ConstructingConns()), metric.WithAttributes(append(attrs, attribute.String("state", "constructing"))...))
		observer.ObserveInt64(gauge, int64(stats.MaxConns()), metric.WithAttributes(append(attrs, attribute.String("state", "max"))...))
		return nil
	}, gauge)
	if err == nil {
		log.Infof("Registered db pool gauges for %s", dbName)
	}
	return err
}
