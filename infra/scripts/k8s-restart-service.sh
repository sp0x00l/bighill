#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source common functions for service discovery
source "${SCRIPT_DIR}/k8s-common.sh"

ENV="${1:-staging}"
SERVICE_INPUT="${2:-}"
NAMESPACE="ml-ops-${ENV}"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="ml-ops-${ENV}"

if [ -z "${SERVICE_INPUT}" ]; then
  echo "Error: service name required"
  echo "Usage: $0 <env> <service-name>"
  exit 1
fi

# Accept service input in any of these forms:
# - account
# - account-service
# - account_service
SERVICE_DIR="${SERVICE_INPUT//-/_}"
if [[ "${SERVICE_DIR}" != *_service ]]; then
  SERVICE_DIR="${SERVICE_DIR}_service"
fi
SERVICE_NAME="${SERVICE_DIR//_/-}"

if [ ! -d "${PROJECT_ROOT}/${SERVICE_DIR}" ]; then
  echo "Error: service '${SERVICE_INPUT}' resolved to '${SERVICE_DIR}' but directory was not found"
  exit 1
fi

configure_kubectl() {
  local CLUSTER="$1"
  local REGION_ARG="$2"

  echo "Configuring kubectl for EKS cluster '${CLUSTER}' in region '${REGION_ARG}'..."
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}" --profile "${AWS_PROFILE}"
  else
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}"
  fi

  if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Error: Cannot connect to EKS cluster ${CLUSTER}"
    exit 1
  fi
}

print_rollout_diagnostics() {
  local TARGET_NAMESPACE="$1"
  local TARGET_SERVICE="$2"
  local POD_NAME=""

  echo "Diagnostics for ${TARGET_SERVICE}:"
  kubectl get pods -n "${TARGET_NAMESPACE}" -l "app=${TARGET_SERVICE}" -o wide || true
  POD_NAME=$(kubectl get pods -n "${TARGET_NAMESPACE}" -l "app=${TARGET_SERVICE}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "${POD_NAME}" ]; then
    echo "Recent logs for pod/${POD_NAME}:"
    kubectl logs -n "${TARGET_NAMESPACE}" "${POD_NAME}" --tail=80 || true
    echo "Previous logs for pod/${POD_NAME}:"
    kubectl logs -n "${TARGET_NAMESPACE}" "${POD_NAME}" --previous --tail=80 || true
  fi
}

restart_deployment() {
  local TARGET_NAMESPACE="$1"
  local TARGET_SERVICE="$2"
  local CURRENT_REPLICAS=""
  local RESTARTED_AT=""
  local RESTART_NONCE=""

  CURRENT_REPLICAS=$(kubectl get deployment "${TARGET_SERVICE}" -n "${TARGET_NAMESPACE}" -o jsonpath='{.spec.replicas}')
  if [ -z "${CURRENT_REPLICAS}" ] || [ "${CURRENT_REPLICAS}" = "0" ]; then
    echo "deployment/${TARGET_SERVICE} is scaled to 0, scaling to 1 before restart..."
    kubectl scale deployment "${TARGET_SERVICE}" -n "${TARGET_NAMESPACE}" --replicas=1
  fi

  RESTARTED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  RESTART_NONCE=$(date -u +"%Y%m%dT%H%M%S%N")
  kubectl patch deployment "${TARGET_SERVICE}" -n "${TARGET_NAMESPACE}" --type merge -p \
    "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"kubectl.kubernetes.io/restartedAt\":\"${RESTARTED_AT}\",\"exchange.dev/restart-nonce\":\"${RESTART_NONCE}\"}}}}}"
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
echo "Restarting deployment/${SERVICE_NAME} in namespace '${NAMESPACE}'..."
restart_deployment "${NAMESPACE}" "${SERVICE_NAME}"
if ! kubectl rollout status deployment "${SERVICE_NAME}" -n "${NAMESPACE}" --timeout=180s; then
  echo "Rollout failed for deployment/${SERVICE_NAME}."
  print_rollout_diagnostics "${NAMESPACE}" "${SERVICE_NAME}"
  exit 1
fi
