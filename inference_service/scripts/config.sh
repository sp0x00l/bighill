#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    true
elif [ "$1" = "staging" ]; then
    true
elif [ "$1" = "prod" ]; then
    true
else
    echo "Error: Invalid environment provided to inference_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export INFERENCE_SERVICE_NAME=inference-service
export INFERENCE_DB_NAME=bighill_inference_db
export INFERENCE_DB_USER=bighill_inference_db_user
export INFERENCE_DB_PASSWORD=$BIGHILL_DB_PASSWORD
export INFERENCE_DB_MAX_CONNECTIONS=20
export INFERENCE_API_GRPC_PORT=7073
export INFERENCE_SERVICE_KAFKA_GROUP_ID=inference-group
export INFERENCE_SERVICE_MODEL_REGISTRY_SUBSCRIBER_TOPIC=model_registry
export INFERENCE_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC=data_registry
export INFERENCE_SERVICE_DLQ=http://localhost:4566/inference-dev-env-queue/
export INFERENCE_FEATURE_MATERIALIZER_GRPC_ADDRESS=localhost:7072
export INFERENCE_FEATURE_MATERIALIZER_GRPC_DIAL_TIMEOUT_MS=500
export INFERENCE_FEATURE_MATERIALIZER_GRPC_CALL_TIMEOUT_MS=15000
export INFERENCE_FEATURE_MATERIALIZER_GRPC_RETRY_COUNT=3
export INFERENCE_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export INFERENCE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export INFERENCE_HEALTHCHECK_PORT=5059
export INFERENCE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS=5
export INFERENCE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export INFERENCE_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS=5

# The following are variables set at build time, intended to be used at runtime.
# They are used to set the version in the build in the binary and is available to the binary main package.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export INFERENCE_SERVICE_BUILD_VERSION=0.0.1
