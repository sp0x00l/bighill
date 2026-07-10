#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENVIRONMENT="${ENVIRONMENT:-local-dev}"

cd "${PROJECT_ROOT}/api_gateway" || exit 1

if ! command -v yq >/dev/null 2>&1; then
  echo "yq is not installed. Run \`make install-dev\` from the project root directory." >&2
  exit 1
fi

# shellcheck disable=SC1090
. "${PROJECT_ROOT}/api_gateway/scripts/config.sh" "${ENVIRONMENT}"

REDIS_HOST="${REDIS_ADDRESS%:*}"
REDIS_PORT="${REDIS_ADDRESS##*:}"

yq "
  (.Parameters.StageNameParam.Default) = \"${ENVIRONMENT}\" |
  (.Resources.BighillApiFunction.Properties.CodeUri) = \"./build/api_binary\" |
  (.Resources.BighillAuthFunction.Properties.CodeUri) = \"./build/auth_binary\" |
  (.Parameters.DataRegistryServiceHttpDomain.Default) = \"${DATA_REGISTRY_SERVICE_HTTP_HOST}\" |
  (.Parameters.DataRegistryServiceHttpPort.Default) = \"${DATA_REGISTRY_SERVICE_HTTP_PORT}\" |
  (.Parameters.IngestionServiceHttpDomain.Default) = \"${INGESTION_SERVICE_HTTP_HOST}\" |
  (.Parameters.IngestionServiceHttpPort.Default) = \"${INGESTION_SERVICE_HTTP_PORT}\" |
  (.Parameters.ModelRegistryServiceHttpDomain.Default) = \"${MODEL_REGISTRY_SERVICE_HTTP_HOST}\" |
  (.Parameters.ModelRegistryServiceHttpPort.Default) = \"${MODEL_REGISTRY_SERVICE_HTTP_PORT}\" |
  (.Parameters.TenantServiceHttpDomain.Default) = \"${TENANT_SERVICE_HTTP_HOST}\" |
  (.Parameters.TenantServiceHttpPort.Default) = \"${TENANT_SERVICE_HTTP_PORT}\" |
  (.Parameters.TrainingServiceHttpDomain.Default) = \"${TRAINING_SERVICE_HTTP_HOST}\" |
  (.Parameters.TrainingServiceHttpPort.Default) = \"${TRAINING_SERVICE_HTTP_PORT}\" |
  (.Parameters.InferenceServiceHttpDomain.Default) = \"${INFERENCE_SERVICE_HTTP_HOST}\" |
  (.Parameters.InferenceServiceHttpPort.Default) = \"${INFERENCE_SERVICE_HTTP_PORT}\" |
  (.Parameters.RedisHost.Default) = \"${REDIS_HOST}\" |
  (.Parameters.RedisPort.Default) = \"${REDIS_PORT}\" |
  (.Parameters.RedisTLS.Default) = \"${REDIS_TLS}\" |
  (.Parameters.RedisUsername.Default) = \"${REDIS_USERNAME}\" |
  (.Parameters.RedisPassword.Default) = \"${REDIS_PASSWORD}\" |
  (.Parameters.AuthKmsKeyId.Default) = \"${AUTH_KMS_KEY_ID}\" |
  (.Parameters.BighillApiFunctionTimeout.Default) = ${API_GATEWAY_FUNCTION_TIMEOUT_SECONDS} |
  (.Parameters.BighillAuthFunctionTimeout.Default) = ${API_GATEWAY_AUTH_FUNCTION_TIMEOUT_SECONDS} |
  (.Parameters.BighillApiHttpClientTimeoutSeconds.Default) = ${API_GATEWAY_SERVICE_HTTP_CLIENT_TIMEOUT_SECONDS}
" template.yml > template.local.generated.yml
