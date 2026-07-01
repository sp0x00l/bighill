#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
. $BIGHILL_ROOT/database/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export DATA_REGISTRY_SERVICE_DLQ=http://localhost:4566/data-registry-dev-env-queue/
    export DATA_REGISTRY_SERVICE_OUTBOX=postgres
elif [ "$1" = "staging" ]; then
    # TODO 
    export DATA_REGISTRY_SERVICE_DLQ=http://localhost:4566/data-registry-dev-env-queue/ # TODO
    export DATA_REGISTRY_SERVICE_OUTBOX=postgres
elif [ "$1" = "prod" ]; then
    export DATA_REGISTRY_SERVICE_DLQ="" # TODO
    export DATA_REGISTRY_SERVICE_OUTBOX=postgres
else 
    echo "Error: Invalid environment provided to data_registry_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

# Runtime variables
export DATA_REGISTRY_DB_NAME=bighill_data_registry_db
export DATA_REGISTRY_DB_USER=bighill_data_registry_db_user
export DATA_REGISTRY_DB_PASSWORD=$BIGHILL_DB_PASSWORD
export DATA_REGISTRY_DB_MAX_CONNECTIONS=20
export DATA_REGISTRY_API_HTTP_PORT=8081
export DATA_REGISTRY_API_GRPC_PORT=7071
export DATA_REGISTRY_SERVICE_NAME=data-registry-service
export DATA_REGISTRY_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export DATA_REGISTRY_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export DATA_REGISTRY_HEALTHCHECK_PORT=5051
export DATA_REGISTRY_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS=5
export DATA_REGISTRY_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS=5
export DATA_REGISTRY_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export DATA_REGISTRY_SERVICE_KAFKA_GROUP_ID=data-registry-group
export DATA_REGISTRY_SERVICE_TOPIC=data_registry
export DATA_REGISTRY_SERVICE_FEATURE_MATERIALIZER_SUBSCRIBER_TOPIC=feature_materializer
export DATA_REGISTRY_SERVICE_OUTBOX_RELAY_POLL_MS=250
export DATA_REGISTRY_SERVICE_OUTBOX_RELAY_BATCH_SIZE=100
export DATA_REGISTRY_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS=2000

# The following are variables set at build time, intended to be used at runtime.
# They are used to set the version in the build in the binary and is available to the binary main package.
# It is then is used to identify the service instance in the logs.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export DATA_REGISTRY_SERVICE_BUILD_VERSION=0.0.1
