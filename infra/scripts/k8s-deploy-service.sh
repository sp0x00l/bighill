#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ENVIRONMENT="${1:-staging}"
SERVICE_INPUT="${2:-}"

usage() {
  echo "Usage: $0 <staging|prod> <service>"
  echo "Examples:"
  echo "  $0 staging account"
  echo "  $0 staging account-service"
  echo "  $0 staging account_service"
}

if [ -z "${SERVICE_INPUT}" ]; then
  usage
  exit 1
fi

SERVICE_DIR="${SERVICE_INPUT//-/_}"
if [[ "${SERVICE_DIR}" != *_service ]]; then
  SERVICE_DIR="${SERVICE_DIR}_service"
fi

SERVICE_NAME="${SERVICE_DIR//_/-}"

if [ ! -d "${PROJECT_ROOT}/${SERVICE_DIR}" ]; then
  echo "Error: service '${SERVICE_INPUT}' resolved to '${SERVICE_DIR}' but directory was not found."
  exit 1
fi

if [ ! -d "${PROJECT_ROOT}/${SERVICE_DIR}/helm" ]; then
  echo "Error: service '${SERVICE_INPUT}' resolved to '${SERVICE_DIR}' but helm chart directory is missing."
  exit 1
fi

echo "Deploying single service '${SERVICE_NAME}' (dir: ${SERVICE_DIR}) to environment '${ENVIRONMENT}'..."
DEPLOY_SERVICE_DIRS="${SERVICE_DIR}" "${SCRIPT_DIR}/k8s-deploy-services.sh" "${ENVIRONMENT}"

