#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
. $BIGHILL_ROOT/database/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    :
elif [ "$1" = "staging" ]; then
    :
elif [ "$1" = "prod" ]; then
    :
else
    echo "Error: Invalid environment provided to model_registry_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export MODEL_REGISTRY_SERVICE_NAME=model-registry-service
export MODEL_REGISTRY_DB_NAME=bighill_model_registry_db
export MODEL_REGISTRY_DB_USER=bighill_model_registry_db_user
export MODEL_REGISTRY_DB_PASSWORD=$BIGHILL_DB_PASSWORD
export MODEL_REGISTRY_DB_MAX_CONNECTIONS=20
export MODEL_REGISTRY_SERVICE_KAFKA_GROUP_ID=model-registry-group
export MODEL_REGISTRY_SERVICE_TOPIC=model_registry
export MODEL_REGISTRY_SERVICE_TRAINING_SUBSCRIBER_TOPIC=training
export MODEL_REGISTRY_SERVICE_DLQ=http://localhost:4566/model-registry-dev-env-queue/
export MODEL_REGISTRY_SERVICE_OUTBOX=postgres
export MODEL_REGISTRY_SERVICE_OUTBOX_RELAY_POLL_MS=250
export MODEL_REGISTRY_SERVICE_OUTBOX_RELAY_BATCH_SIZE=100
export MODEL_REGISTRY_SERVICE_OUTBOX_RELAY_FAILURE_BACKOFF_MS=2000
export MODEL_REGISTRY_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export MODEL_REGISTRY_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export MODEL_REGISTRY_HEALTHCHECK_PORT=5060
export MODEL_REGISTRY_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS=5
export MODEL_REGISTRY_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export MODEL_REGISTRY_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS=5

# The following are variables set at build time, intended to be used at runtime.
# They are used to set the version in the build in the binary and is available to the binary main package.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export MODEL_REGISTRY_SERVICE_BUILD_VERSION=0.0.1
