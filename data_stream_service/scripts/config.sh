#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export DATA_STREAM_SERVICE_DLQ=http://localhost:4566/data-stream-dev-env-queue/
    export DATA_STREAM_SERVICE_OUTBOX=postgres
elif [ "$1" = "staging" ]; then
    export DATA_STREAM_SERVICE_DLQ=http://localhost:4566/data-stream-dev-env-queue/ # TODO
    export DATA_STREAM_SERVICE_OUTBOX=postgres
elif [ "$1" = "prod" ]; then
    export DATA_STREAM_SERVICE_DLQ="" # TODO
    export DATA_STREAM_SERVICE_OUTBOX=postgres
else
    echo "Error: Invalid environment provided to data_stream_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

# Runtime variables
export DATA_STREAM_API_GRPC_HOST=localhost
export DATA_STREAM_API_GRPC_PORT=7070
export DATA_STREAM_SERVICE_NAME=data-stream-service
export DATA_STREAM_QUERY_ENGINE_MODE=registry
export DATA_STREAM_QUERY_ENGINE_DATA_ROOT=tmp/local_s3_storage
export DATA_STREAM_QUERY_ENGINE_BINARY_PATH=../query_engine/datafusion_query_engine/target/release/datafusion_query_engine
export DATA_STREAM_QUERY_ENGINE_TIMEOUT_SECONDS=30
export DATA_STREAM_DATA_REGISTRY_GRPC_ADDRESS=localhost:7071
export DATA_STREAM_DATA_REGISTRY_GRPC_DIAL_TIMEOUT_MS=500
export DATA_STREAM_DATA_REGISTRY_GRPC_CALL_TIMEOUT_MS=15000
export DATA_STREAM_DATA_REGISTRY_GRPC_RETRY_COUNT=3
export DATA_STREAM_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export DATA_STREAM_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export DATA_STREAM_HEALTHCHECK_PORT=5050
export DATA_STREAM_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export DATA_STREAM_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS=5
export DATA_STREAM_SERVICE_KAFKA_GROUP_ID=data-stream-group
export DATA_STREAM_SERVICE_OUTBOX_RELAY_POLL_MS=250
export DATA_STREAM_SERVICE_OUTBOX_RELAY_BATCH_SIZE=100
export DATA_STREAM_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS=2000


# The following are variables set at build time, intended to be used at runtime.
# They are used to set the version in the build in the binary and is available to the binary main package.
# It is then is used to identify the service instance in the logs.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export DATA_STREAM_SERVICE_BUILD_VERSION=0.0.1
