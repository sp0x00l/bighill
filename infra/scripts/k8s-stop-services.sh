#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/k8s-common.sh"

ENV="${1:-staging}"
NAMESPACE="ml-ops-${ENV}"
REGION="${AWS_REGION:-eu-west-1}"
CLUSTER_NAME="bighill-${ENV}"

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

stop_services() {
  local TARGET_NAMESPACE="$1"
  local TARGET_SERVICES

  if [ "${DEPLOY_SERVICE_DIRS:-}" = "none" ]; then
    echo "No services selected for stop."
    return 0
  fi

  if [ -n "${DEPLOY_SERVICE_DIRS:-}" ]; then
    TARGET_SERVICES="$(echo "${DEPLOY_SERVICE_DIRS}" | tr ',' ' ')"
  else
    TARGET_SERVICES="$(get_services_list "$PROJECT_ROOT")"
  fi

  echo "Scaling services to zero in namespace '${TARGET_NAMESPACE}'..."
  for SERVICE_DIR in ${TARGET_SERVICES}; do
    local SERVICE_NAME
    SERVICE_NAME=$(echo "${SERVICE_DIR}" | sed 's/_/-/g')

    if ! kubectl get deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" >/dev/null 2>&1; then
      echo "Skipping deployment/${SERVICE_NAME}: deployment not found in namespace ${TARGET_NAMESPACE}."
      continue
    fi

    echo "Scaling deployment/${SERVICE_NAME} to 0..."
    kubectl scale deployment "${SERVICE_NAME}" -n "${TARGET_NAMESPACE}" --replicas=0
  done
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
stop_services "$NAMESPACE"
