#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${AWS_REGION:-eu-west-1}"

if [ -z "${ENVIRONMENT}" ]; then
  echo "Usage: $0 <staging|prod>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_NAME="ml-ops-${ENVIRONMENT}"
NAMESPACE="observability"
RESET_TIMEOUT="${OBSERVABILITY_RESET_TIMEOUT:-180s}"

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

namespace_exists() {
  kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1
}

uninstall_observability_releases() {
  local RELEASES=(
    otel-collector
    promtail
    kube-prometheus-stack
    loki
    tempo
  )

  echo "Uninstalling observability Helm releases in namespace '${NAMESPACE}'..."
  for RELEASE in "${RELEASES[@]}"; do
    helm uninstall "${RELEASE}" -n "${NAMESPACE}" --ignore-not-found --timeout 2m 2>/dev/null || true
  done
}

delete_leftover_controllers() {
  echo "Deleting leftover observability controllers..."
  kubectl delete statefulset --all -n "${NAMESPACE}" --ignore-not-found
  kubectl delete deployment --all -n "${NAMESPACE}" --ignore-not-found
  kubectl delete daemonset --all -n "${NAMESPACE}" --ignore-not-found
  kubectl delete replicaset --all -n "${NAMESPACE}" --ignore-not-found
}

delete_remaining_pods() {
  echo "Deleting remaining observability pods..."
  kubectl delete pod --all -n "${NAMESPACE}" --ignore-not-found --timeout="${RESET_TIMEOUT}" || true
  kubectl wait --for=delete pod --all -n "${NAMESPACE}" --timeout="${RESET_TIMEOUT}" 2>/dev/null || true
}

delete_observability_pvcs() {
  local PVC_NAMES=""

  PVC_NAMES="$(kubectl get pvc -n "${NAMESPACE}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
  if [ -z "${PVC_NAMES}" ]; then
    echo "No observability PVCs found to delete."
    return
  fi

  echo "Deleting observability PVCs:"
  echo "${PVC_NAMES}"
  kubectl delete pvc -n "${NAMESPACE}" ${PVC_NAMES}

  while read -r PVC; do
    if [ -n "${PVC}" ]; then
      kubectl wait --for=delete pvc/"${PVC}" -n "${NAMESPACE}" --timeout="${RESET_TIMEOUT}" 2>/dev/null || true
    fi
  done <<< "${PVC_NAMES}"
}

show_status() {
  echo ""
  echo "Observability pods:"
  kubectl get pods -n "${NAMESPACE}" -o wide || true
  echo ""
  echo "Observability PVCs:"
  kubectl get pvc -n "${NAMESPACE}" || true
}

configure_kubectl "${CLUSTER_NAME}" "${REGION}"

if namespace_exists; then
  uninstall_observability_releases
  delete_leftover_controllers
  delete_remaining_pods
  delete_observability_pvcs
else
  echo "Namespace '${NAMESPACE}' does not exist; deploy will recreate it."
fi

echo "Redeploying observability stack with fresh PVCs..."
"${SCRIPT_DIR}/k8s-deploy-observability.sh" "${ENVIRONMENT}"
show_status
