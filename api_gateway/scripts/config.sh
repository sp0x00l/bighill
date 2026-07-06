#!/usr/bin/env bash

set -euo pipefail

# Prevent the AWS SAM CLI from sending telemetry data to the regional AWS serverless telemetry endpoint.
export SAM_CLI_TELEMETRY=0
export AWS_SAM_CLI_TELEMETRY=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENVIRONMENT="${1:-local-dev}"

case "$ENVIRONMENT" in
    local-dev|cicd)
        if [ "$(uname -s)" = "Linux" ]; then
            if [ -n "${CI:-}" ] || [ -n "${GITHUB_ACTIONS:-}" ]; then
                API_GATEWAY_SERVICE_HOST="172.17.0.1"
            else
                API_GATEWAY_SERVICE_HOST="127.0.0.1"
            fi
        else
            API_GATEWAY_SERVICE_HOST="host.docker.internal"
        fi
        ;;
    staging|prod)
        API_GATEWAY_SERVICE_HOST=""
        ;;
    *)
        echo "Error: invalid environment param in api_gateway config"
        echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
        exit 1
        ;;
esac

# shellcheck disable=SC1090
. "$PROJECT_ROOT/shared_lib/scripts/config.sh" "$ENVIRONMENT"

if [ "$ENVIRONMENT" = "local-dev" ] || [ "$ENVIRONMENT" = "cicd" ]; then
    export DATA_REGISTRY_SERVICE_HTTP_HOST="$API_GATEWAY_SERVICE_HOST"
    export INGESTION_SERVICE_HTTP_HOST="$API_GATEWAY_SERVICE_HOST"
    export MODEL_REGISTRY_SERVICE_HTTP_HOST="$API_GATEWAY_SERVICE_HOST"
    export PROFILE_SERVICE_HTTP_HOST="$API_GATEWAY_SERVICE_HOST"
    export TRAINING_SERVICE_HTTP_HOST="$API_GATEWAY_SERVICE_HOST"
    export REDIS_ADDRESS="$API_GATEWAY_SERVICE_HOST:6379"
else
    export DATA_REGISTRY_SERVICE_HTTP_HOST=data-registry-service
    export INGESTION_SERVICE_HTTP_HOST=ingestion-service
    export MODEL_REGISTRY_SERVICE_HTTP_HOST=model-registry-service
    export PROFILE_SERVICE_HTTP_HOST=profile-service
    export TRAINING_SERVICE_HTTP_HOST=training-service
fi

export DATA_REGISTRY_SERVICE_HTTP_PORT=8081
export INGESTION_SERVICE_HTTP_PORT=8086
export MODEL_REGISTRY_SERVICE_HTTP_PORT=8084
export PROFILE_SERVICE_HTTP_PORT=8082
export TRAINING_SERVICE_HTTP_PORT=8085
export API_GATEWAY_SERVICE_HTTP_CLIENT_TIMEOUT_SECONDS=10
export REDIS_TLS="${REDIS_TLS:-false}"
export REDIS_USERNAME="${REDIS_USERNAME:-}"
export REDIS_PASSWORD="${REDIS_PASSWORD:-}"
export AUTH_KMS_KEY_ID="${AUTH_KMS_KEY_ID:-}"
