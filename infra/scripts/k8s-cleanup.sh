#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source common functions for service discovery
source "${SCRIPT_DIR}/k8s-common.sh"

ENVIRONMENT="${1:-}"
REGION="${AWS_REGION:-eu-west-1}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 [staging|prod]"
  exit 1
fi

NAMESPACE="ml-ops-${ENVIRONMENT}"
CLUSTER_NAME="ml-ops-${ENVIRONMENT}"

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

uninstall_bighill_infra() {
  local TARGET_NAMESPACE="$1"
  echo "Uninstalling ml-ops-infra helm chart..."
  helm uninstall ml-ops-infra -n "$TARGET_NAMESPACE" --wait 2>/dev/null || true
}

uninstall_services() {
  local TARGET_NAMESPACE="$1"
  
  # Discover services from repo structure
  mapfile -t SERVICES < <(get_services_list "$PROJECT_ROOT")

  echo "Uninstalling service helm charts..."
  for SERVICE in "${SERVICES[@]}"; do
    # Remove both bare and versioned release names to cover past deploy scripts
    helm uninstall "${SERVICE}" -n "$TARGET_NAMESPACE" --ignore-not-found 2>/dev/null || true
    helm uninstall "${SERVICE}.0.0.1" -n "$TARGET_NAMESPACE" --ignore-not-found 2>/dev/null || true
  done
}

uninstall_addons() {
  local TARGET_NAMESPACE="$1"
  echo "Uninstalling add-ons (ALB controller, ExternalDNS)..."
  helm uninstall aws-load-balancer-controller -n "$TARGET_NAMESPACE" --ignore-not-found 2>/dev/null || true
  helm uninstall external-dns -n "$TARGET_NAMESPACE" --ignore-not-found 2>/dev/null || true
}

delete_pods_and_jobs() {
  local TARGET_NAMESPACE="$1"
  echo "Deleting all pods to force image re-pull..."
  kubectl delete pods -n "$TARGET_NAMESPACE" --all --force --grace-period=0 2>/dev/null || true

  echo "Deleting all jobs..."
  kubectl delete job --all -n "$TARGET_NAMESPACE" 2>/dev/null || true

  echo "Deleting succeeded pods..."
  kubectl delete pod --field-selector=status.phase=Succeeded -n "$TARGET_NAMESPACE" 2>/dev/null || true
}

wait_for_termination() {
  local TARGET_NAMESPACE="$1"
  echo "Waiting for resources to terminate..."
  kubectl wait --for=delete pod --all -n "$TARGET_NAMESPACE" --timeout=60s 2>/dev/null || true
  sleep 5
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
uninstall_bighill_infra "$NAMESPACE"
uninstall_services "$NAMESPACE"
uninstall_addons "kube-system"
delete_pods_and_jobs "$NAMESPACE"
wait_for_termination "$NAMESPACE"
