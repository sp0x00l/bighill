# Metrics Package

Unified observability metrics for the Exchange platform using OpenTelemetry.

## Overview

This package provides a consistent approach to collecting infrastructure and application metrics across all services. It integrates with the OpenTelemetry SDK and supports export to any OTLP-compatible backend (Prometheus, Grafana, Datadog, etc.).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Application                             │
├─────────────────────────────────────────────────────────────────┤
│  Recorder Interface                                             │
│  ├── RecordRequest(boundary, operation, status)                 │
│  ├── RecordError(boundary, operation, class, status)            │
│  ├── RecordDuration(boundary, operation, status, seconds)       │
│  └── RecordKafkaLag(topic, seconds)                             │
├─────────────────────────────────────────────────────────────────┤
│  Gauges                                                         │
│  ├── db_pool_connections (by state: total, idle, acquired)      │
│  └── http_active_requests                                       │
├─────────────────────────────────────────────────────────────────┤
│  OpenTelemetry SDK → OTLP Exporter → Backend                    │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Initialization

Call `Init()` in your service's `main.go`:

```go
import "lib/shared_lib/metrics"

func main() {
    ctx := context.Background()
    shutdown := metrics.Init(ctx, nil, "account-service", "v1.0.0")
    defer shutdown()
    
    // ... rest of service initialization
}
```

The OTLP endpoint is configured via:
1. `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable (preferred)
2. `otlpConfig["url"]` map parameter

If no endpoint is configured, metrics are collected in-memory (useful for testing).

### Recording Metrics

Use the default recorder singleton:

```go
import "lib/shared_lib/metrics"

// Record a successful request
metrics.Default().RecordRequest(ctx, metrics.BoundaryDB, "query_users", "OK")

// Record an error with classification
metrics.Default().RecordError(ctx, metrics.BoundaryDB, "query_users", metrics.ErrorClassTimeout, "TIMEOUT")

// Record request duration
start := time.Now()
// ... do work ...
metrics.Default().RecordDuration(ctx, metrics.BoundaryDB, "query_users", "OK", time.Since(start).Seconds())

// Record Kafka consumer lag
metrics.Default().RecordKafkaLag(ctx, "account-events", 0.5)

// Record committed Kafka consumer throughput and true offset lag
metrics.Default().RecordKafkaMessageConsumed(ctx, "account-group", "account", 0, "account_created")
metrics.Default().RecordKafkaConsumerLag(ctx, "account-group", "account", 0, 42)
```

## Files

| File | Purpose |
|------|---------|
| `metrics.go` | OTEL SDK initialization and provider setup |
| `counters.go` | `Recorder` interface and `Counters` implementation |
| `taxonomy.go` | `Boundary` and `ErrorClass` type definitions |
| `classifier.go` | Error classification for gRPC, HTTP, DB, Kafka, Redis |
| `helpers.go` | `MustCounter`, `MustHistogram`, `MustObservableGauge` helpers |
| `db_gauges.go` | PostgreSQL connection pool gauges |
| `http_gauges.go` | HTTP active requests gauge |

## Metrics Reference

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `infra_requests_total` | boundary, operation, status | Total infrastructure requests |
| `infra_errors_total` | boundary, operation, error_class, status | Total infrastructure errors |
| `kafka_messages_consumed_total` | group_id, topic, partition, message_type | Kafka messages successfully consumed and committed |

### Histograms

| Metric | Labels | Unit | Buckets |
|--------|--------|------|---------|
| `infra_request_duration_seconds` | boundary, operation, status | seconds | 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 75ms, 100ms, 250ms, 500ms, 1s, 2s, 5s, 10s |
| `kafka_message_lag_seconds` | topic | seconds | default |

### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `db_pool_connections` | db, state | Connection pool stats (total, idle, acquired, constructing, max) |
| `http_active_requests` | boundary | In-flight HTTP requests |
| `kafka_consumer_lag_messages` | group_id, topic, partition | Current Kafka consumer group offset lag in messages |

## Taxonomy

### Boundaries

Boundaries identify the infrastructure component being instrumented:

| Boundary | Description |
|----------|-------------|
| `db` | PostgreSQL database |
| `kafka` | Kafka messaging |
| `grpc_client` | Outbound gRPC calls |
| `grpc_server` | Inbound gRPC requests |
| `http_client` | Outbound HTTP calls |
| `http_server` | Inbound HTTP requests |
| `redis` | Redis cache/pubsub |
| `external` | External third-party APIs |

### Error Classes

Errors are classified into semantic categories for alerting and dashboards:

| Class | Description |
|-------|-------------|
| `timeout` | Request exceeded deadline |
| `network` | Network connectivity issue |
| `unavailable` | Service unavailable |
| `canceled` | Request was canceled |
| `rate_limit` | Rate limit exceeded |
| `auth` | Authentication failed |
| `permission` | Authorization denied |
| `not_found` | Resource not found |
| `conflict` | Conflict (duplicate, constraint violation) |
| `bad_response` | Malformed response |
| `serialization` | Serialization/deserialization error |
| `db` | Database error |
| `internal` | Internal server error |
| `unknown` | Unclassified error |

## Classifiers

Use classifiers to automatically categorize errors:

```go
// gRPC errors
class, statusCode := metrics.ClassifyGRPC(err)

// HTTP status codes
class := metrics.ClassifyHTTPStatus(resp.StatusCode)

// Database errors
class := metrics.ClassifyDB(err)

// Kafka errors
class := metrics.ClassifyKafka(err)

// Redis errors
class := metrics.ClassifyRedis(err)
```

## Integration

### HTTP Middleware

The `transport.Middleware` automatically records HTTP server metrics:

```go
// In shared_lib/transport/middleware.go
metrics.DefaultActiveRequests().Inc()
defer metrics.DefaultActiveRequests().Dec()

// Records: infra_requests_total, infra_errors_total, infra_request_duration_seconds
```

### gRPC Server Interceptors

Use the provided interceptors for gRPC servers:

```go
import rpc "lib/shared_lib/rpc"

grpcServer := grpc.NewServer(
    grpc.ChainUnaryInterceptor(rpc.MetricsUnaryServerInterceptor()),
    grpc.ChainStreamInterceptor(rpc.MetricsStreamServerInterceptor()),
)
```

### gRPC Client Interceptor

The `rpc.NewClient` automatically includes metrics instrumentation:

```go
// In shared_lib/rpc/client_connection.go
grpc.WithChainUnaryInterceptor(metricsUnaryInterceptor(), ...)
```

### Database Connection Pool

Register pool gauges when creating a database connection:

```go
pool, err := dbConn.InitDatabase(ctx, dbName, connStr, logger)
// Automatically registers db_pool_connections gauge
```

## Testing

Override the default recorder for testing:

```go
type stubRecorder struct {
    requests int
    errors   int
}

func (s *stubRecorder) RecordRequest(...) { s.requests++ }
func (s *stubRecorder) RecordError(...)   { s.errors++ }
func (s *stubRecorder) RecordDuration(...) {}
func (s *stubRecorder) RecordKafkaLag(...) {}

// In test setup
original := metrics.Default()
defer metrics.SetDefault(original)

stub := &stubRecorder{}
metrics.SetDefault(stub)

// Run code under test...

Expect(stub.requests).To(Equal(1))
```

## Domain Metrics

For business/domain metrics (e.g., deposits, trades), create service-specific counters:

```go
// In account_service/pkg/app/funds_usecase.go
type fundsMetrics struct {
    depositsCreated  metric.Int64Counter
    depositsCredited metric.Int64Counter
}

func newFundsMetrics() fundsMetrics {
    meter := otel.Meter("exchange.account-service")
    return fundsMetrics{
        depositsCreated: metrics.MustCounter(meter, "account_deposits_created_total", 
            "Total deposits created", "exchange.account-service-fallback"),
    }
}

// Usage
uc.metrics.depositsCreated.Add(ctx, 1, 
    metric.WithAttributes(attribute.String("asset", "BTC")))
```

## Prometheus Queries

Example PromQL queries for dashboards:

```promql
# Request rate by boundary
sum(rate(infra_requests_total[5m])) by (boundary)

# Error rate by error class
sum(rate(infra_errors_total[5m])) by (error_class)

# P99 latency by operation
histogram_quantile(0.99, 
    sum(rate(infra_request_duration_seconds_bucket[5m])) by (le, operation))

# Database connection pool utilization
db_pool_connections{state="acquired"} / db_pool_connections{state="max"}

# Kafka consumer lag
histogram_quantile(0.95, 
    sum(rate(kafka_message_lag_seconds_bucket[5m])) by (le, topic))

# Kafka consumer group offset lag
max by (group_id, topic, partition) (kafka_consumer_lag_messages)

# Kafka consumer throughput
sum by (group_id, topic) (rate(kafka_messages_consumed_total[5m]))
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint | (none - in-memory) |

### Histogram Buckets

Duration histogram uses custom buckets optimized for microservice latencies:
- Sub-millisecond: 1ms
- Fast operations: 2.5ms, 5ms, 10ms, 25ms
- Normal operations: 50ms, 75ms, 100ms, 250ms, 500ms
- Slow operations: 1s, 2s, 5s, 10s

## Best Practices

1. **Use classifiers** - Always use the provided classifiers rather than creating custom error classes
2. **Consistent operation names** - Use descriptive, hierarchical operation names (e.g., `query_users`, `create_order`)
3. **Record both success and failure** - Always call `RecordRequest` and `RecordDuration` regardless of outcome
4. **Defer duration recording** - Use defer pattern to ensure duration is always recorded:
   ```go
   start := time.Now()
   status := "OK"
   defer func() {
       metrics.Default().RecordDuration(ctx, boundary, op, status, time.Since(start).Seconds())
       metrics.Default().RecordRequest(ctx, boundary, op, status)
   }()
   ```
5. **Test with stubs** - Override `Default()` in tests to verify metric recording
