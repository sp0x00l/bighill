#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
. $BIGHILL_ROOT/database/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export DATA_INGESTION_FILES_BUCKET_REGION=local-dev
    export DATA_INGESTION_FILES_BUCKET_NAME=local-dev-bucket
    export DATA_INGESTION_SERVICE_DLQ=http://localhost:4566/data-ingestion-dev-env-queue/
    export DATA_INGESTION_SERVICE_OUTBOX=postgres
    export DATA_INGESTION_SERVICE_REDIS_ADDRESS=localhost:6379
elif [ "$1" = "staging" ]; then
    export DATA_INGESTION_FILES_BUCKET_REGION=${AWS_DEFAULT_REGION}
    export DATA_INGESTION_FILES_BUCKET_NAME=adt-datalake-rawdata-prod-255525589248-eu-west-1 # TODO 
    export DATA_INGESTION_SERVICE_DLQ=http://localhost:4566/data-ingestion-dev-env-queue/ # TODO
    export DATA_INGESTION_SERVICE_OUTBOX=postgres
    export DATA_INGESTION_SERVICE_REDIS_ADDRESS=redis:6379
elif [ "$1" = "prod" ]; then
    export DATA_INGESTION_FILES_BUCKET_REGION=${AWS_DEFAULT_REGION}
    export DATA_INGESTION_FILES_BUCKET_NAME=adt-datalake-rawdata-prod-255525589248-eu-west-1
    export DATA_INGESTION_SERVICE_DLQ="" # TODO
    export DATA_INGESTION_SERVICE_OUTBOX=postgres
    export DATA_INGESTION_SERVICE_REDIS_ADDRESS=redis:6379
else 
    echo "Error: Invalid environment provided to data_ingestion_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export DATA_INGESTION_FILE_MAX_SIZE_MB=2000
export DATA_INGESTION_FILES_UPLOAD_PART_SIZE_MS=${AWS_DEFAULT_UPLOAD_PART_SIZE_MB:-10}
export DATA_INGESTION_DB_NAME=bighill_data_ingestion_db
export DATA_INGESTION_DB_USER=bighill_data_ingestion_db_user
export DATA_INGESTION_DB_PASSWORD=$BIGHILL_DB_PASSWORD
export DATA_INGESTION_DB_MAX_CONNECTIONS=20
export DATA_INGESTION_API_HTTP_PORT=8086
export DATA_INGESTION_SERVICE_NAME=data-ingestion-service
export DATA_INGESTION_SERVICE_REDIS_USERNAME=""
export DATA_INGESTION_SERVICE_REDIS_PASSWORD=""
export DATA_INGESTION_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export DATA_INGESTION_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export DATA_INGESTION_HEALTHCHECK_PORT=5056
export DATA_INGESTION_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export DATA_INGESTION_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS=5
export DATA_INGESTION_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS=5
export DATA_INGESTION_SERVICE_KAFKA_GROUP_ID=data-ingestion-group
export DATA_INGESTION_SERVICE_TOPIC=data_ingestion
export DATA_INGESTION_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC=data_registry
export DATA_INGESTION_SERVICE_OUTBOX_RELAY_POLL_MS=250
export DATA_INGESTION_SERVICE_OUTBOX_RELAY_BATCH_SIZE=100
export DATA_INGESTION_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS=2000


# The following are variables set at build time, intended to be used at runtime.
# They are used to set the version in the build in the binary and is available to the binary main package.
# It is then is used to identify the service instance in the logs.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export DATA_INGESTION_SERVICE_BUILD_VERSION=0.0.1
