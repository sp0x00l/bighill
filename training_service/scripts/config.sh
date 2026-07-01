#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export TRAINING_SERVICE_TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-localhost:7233}
elif [ "$1" = "staging" ]; then
    export TRAINING_SERVICE_TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-temporal:7233}
elif [ "$1" = "prod" ]; then
    export TRAINING_SERVICE_TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-temporal:7233}
else
    echo "Error: Invalid environment provided to training_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export TRAINING_SERVICE_NAME=training-service
export TRAINING_SERVICE_TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-default}
export TRAINING_SERVICE_TEMPORAL_TASK_QUEUE=training-service
export TRAINING_SERVICE_KAFKA_GROUP_ID=training-group
export TRAINING_SERVICE_TOPIC=training
export TRAINING_SERVICE_DATA_REGISTRY_SUBSCRIBER_TOPIC=data_registry
export TRAINING_SERVICE_DLQ=http://localhost:4566/training-dev-env-queue/
export TRAINING_SERVICE_BASE_MODEL=local-dev-base-model
export TRAINING_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export TRAINING_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export TRAINING_HEALTHCHECK_PORT=5058
export TRAINING_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export TRAINING_HEALTHCHECK_MSG_BROKER_LATENCY_THRESHOLD_SECONDS=5

# The following are variables set at build time, intended to be used at runtime.
# They are used to set the version in the build in the binary and is available to the binary main package.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export TRAINING_SERVICE_BUILD_VERSION=0.0.1
