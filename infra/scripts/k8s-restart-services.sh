#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source common functions for service discovery
source "${SCRIPT_DIR}/k8s-common.sh"

ENV="${1:-staging}"
NAMESPACE="ml-ops-${ENV}"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="ml-ops-${ENV}"

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
  local SERVICE_NAME="$2"
  local POD_NAME=""

  echo "Diagnostics for ${SERVICE_NAME}:"
  kubectl get pods -n "${TARGET_NAMESPACE}" -l "app=${SERVICE_NAME}" -o wide || true
  POD_NAME=$(kubectl get pods -n "${TARGET_NAMESPACE}" -l "app=${SERVICE_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "${POD_NAME}" ]; then
    echo "Recent logs for pod/${POD_NAME}:"
    kubectl logs -n "${TARGET_NAMESPACE}" "${POD_NAME}" --tail=80 || true
    echo "Previous logs for pod/${POD_NAME}:"
    kubectl logs -n "${TARGET_NAMESPACE}" "${POD_NAME}" --previous --tail=80 || true
  fi
}

restart_deployment() {
  local TARGET_NAMESPACE="$1"
  local SERVICE_NAME="$2"
  local CURRENT_REPLICAS=""
  local RESTARTED_AT=""
  local RESTART_NONCE=""

  CURRENT_REPLICAS=$(kubectl get deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" -o jsonpath='{.spec.replicas}')
  if [ -z "${CURRENT_REPLICAS}" ] || [ "${CURRENT_REPLICAS}" = "0" ]; then
    echo "deployment/${SERVICE_NAME} is scaled to 0, scaling to 1 before restart..."
    kubectl scale deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" --replicas=1
  fi

  RESTARTED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  RESTART_NONCE=$(date -u +"%Y%m%dT%H%M%S%N")
  kubectl patch deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" --type merge -p \
    "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"kubectl.kubernetes.io/restartedAt\":\"${RESTARTED_AT}\",\"bighill.dev/restart-nonce\":\"${RESTART_NONCE}\"}}}}}"
}

restart_services() {
  local TARGET_NAMESPACE="$1"
  local TARGET_SERVICES
  local RESTART_SERVICE_NAMES=()
  local MARKET_MAKER_SELECTED="false"
  local FAILED_SERVICES=()

  if [ "${DEPLOY_SERVICE_DIRS:-}" = "none" ]; then
    echo "No services selected for restart."
    return 0
  fi

  if [ -n "${DEPLOY_SERVICE_DIRS:-}" ]; then
    TARGET_SERVICES="$(echo "${DEPLOY_SERVICE_DIRS}" | tr ',' ' ')"
  else
    TARGET_SERVICES="$(get_services_list "$PROJECT_ROOT")"
  fi

  for SERVICE_SELECTOR in ${TARGET_SERVICES}; do
    local SERVICE_NAME
    SERVICE_NAME=$(echo "${SERVICE_SELECTOR}" | sed 's/_/-/g')

    if [ "${SERVICE_NAME}" = "market-maker-service" ]; then
      MARKET_MAKER_SELECTED="true"
      continue
    fi

    RESTART_SERVICE_NAMES+=("${SERVICE_NAME}")
  done

  if [ "${MARKET_MAKER_SELECTED}" = "true" ]; then
    RESTART_SERVICE_NAMES+=("market-maker-service")
  fi

  echo "Rolling out restarts for services in namespace '${TARGET_NAMESPACE}'..."
  if [ "${MARKET_MAKER_SELECTED}" = "true" ]; then
    echo "market-maker-service selected; it will be restarted last."
  fi
  for SERVICE_NAME in "${RESTART_SERVICE_NAMES[@]}"; do
    if ! kubectl get deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" >/dev/null 2>&1; then
      echo "Skipping deployment/${SERVICE_NAME}: deployment not found in namespace ${TARGET_NAMESPACE}."
      continue
    fi

    echo "Restarting deployment/${SERVICE_NAME}..."
    restart_deployment "${TARGET_NAMESPACE}" "${SERVICE_NAME}"

    if ! kubectl rollout status deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" --timeout=180s; then
      echo "Rollout failed for deployment/${SERVICE_NAME}."
      print_rollout_diagnostics "${TARGET_NAMESPACE}" "${SERVICE_NAME}"
      FAILED_SERVICES+=("${SERVICE_NAME}")
    fi
  done

  if [ "${#FAILED_SERVICES[@]}" -gt 0 ]; then
    echo "Restart failed for services: ${FAILED_SERVICES[*]}"
    return 1
  fi
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
restart_services "$NAMESPACE"
