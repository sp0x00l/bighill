#! /usr/bin/env sh

export ENVIRONMENT=$1
export TARGETARCH=arm64

# These config settings are common to all services
if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    # Mirrors the seeded instrument catalog used in local/cicd:
    # - BTC_USD spot/perp/futures
    # - SN_HB_NORTH_USD power index
    # - SN_ERN_M_USD / SN_NEB_M_USD / SN_ER2_M_USD / SN_ECI_M_USD power futures
    export CELL_SHARD_MARKET_KEYS="BTC_USD,SN_HB_NORTH_USD,SN_ERN_M_USD,SN_NEB_M_USD,SN_ER2_M_USD,SN_ECI_M_USD"
    export KAFKA_BROKER=localhost:9092
    export AWS_REGION=eu-west-1
    export REDIS_ADDRESS=localhost:6379
    export OTEL_EXPORTER_OTLP_ENDPOINT=""
    export SHARED_LIB_DB_STATEMENT_TIMEOUT_MS=15000
    export SHARED_LIB_DB_LOCK_TIMEOUT_MS=5000
    export SHARED_LIB_DB_IDLE_IN_TX_TIMEOUT_MS=10000
elif [ "$1" = "staging" ]; then
    # Staging defaults should create the full seeded topic set unless Helm overrides it.
    export CELL_SHARD_MARKET_KEYS="BTC_USD,SN_HB_NORTH_USD,SN_ERN_M_USD,SN_NEB_M_USD,SN_ER2_M_USD,SN_ECI_M_USD"
    export KAFKA_BROKER=kafka:9092
    export AWS_REGION=eu-west-1
    export REDIS_ADDRESS=redis:6379
    export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.observability.svc.cluster.local:4318"
    export SHARED_LIB_DB_STATEMENT_TIMEOUT_MS=15000
    export SHARED_LIB_DB_LOCK_TIMEOUT_MS=5000
    export SHARED_LIB_DB_IDLE_IN_TX_TIMEOUT_MS=10000
elif [ "$1" = "prod" ]; then
    # Production is expected to override this via deployment config.
    export CELL_SHARD_MARKET_KEYS="BTC_USD"
    export KAFKA_BROKER=kafka:9092
    export AWS_REGION=eu-west-1
    export REDIS_ADDRESS=localhost:6379 # TODO
    export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.observability.svc.cluster.local:4318"
    export SHARED_LIB_DB_STATEMENT_TIMEOUT_MS=15000
    export SHARED_LIB_DB_LOCK_TIMEOUT_MS=5000
    export SHARED_LIB_DB_IDLE_IN_TX_TIMEOUT_MS=10000
else
    echo "Invalid environment provided to shared_lib config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi
