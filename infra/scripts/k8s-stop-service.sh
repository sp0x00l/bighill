#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ENV="${1:-staging}"
SERVICE_INPUT="${2:-}"
NAMESPACE="ml-ops-${ENV}"
REGION="${AWS_REGION:-eu-west-1}"
CLUSTER_NAME="bighill-${ENV}"

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

wait_for_pods_to_stop() {
  local TARGET_NAMESPACE="$1"
  local TARGET_SERVICE="$2"
  local DEADLINE=$((SECONDS + 180))
  local POD_NAMES=""

  while true; do
    POD_NAMES="$(kubectl get pods -n "${TARGET_NAMESPACE}" -l "app=${TARGET_SERVICE}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
    if [ -z "${POD_NAMES}" ]; then
      echo "No running pods remain for app=${TARGET_SERVICE}."
      return 0
    fi

    if [ "${SECONDS}" -ge "${DEADLINE}" ]; then
      echo "Timed out waiting for pods to stop for app=${TARGET_SERVICE}: ${POD_NAMES}"
      kubectl get pods -n "${TARGET_NAMESPACE}" -l "app=${TARGET_SERVICE}" -o wide || true
      return 1
    fi

    sleep 3
  done
}

stop_deployment() {
  local TARGET_NAMESPACE="$1"
  local TARGET_SERVICE="$2"

  if ! kubectl get deployment "${TARGET_SERVICE}" -n "${TARGET_NAMESPACE}" >/dev/null 2>&1; then
    echo "Error: deployment/${TARGET_SERVICE} not found in namespace ${TARGET_NAMESPACE}"
    exit 1
  fi

  echo "Scaling deployment/${TARGET_SERVICE} to 0 in namespace '${TARGET_NAMESPACE}'..."
  kubectl scale deployment "${TARGET_SERVICE}" -n "${TARGET_NAMESPACE}" --replicas=0
  wait_for_pods_to_stop "${TARGET_NAMESPACE}" "${TARGET_SERVICE}"
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
stop_deployment "$NAMESPACE" "$SERVICE_NAME"
