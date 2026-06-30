#! /usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
SERVICE_FILTER="${2:-}"
REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source common functions for service discovery
source "${SCRIPT_DIR}/k8s-common.sh"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 [staging|prod] [optional-service]"
  echo " - With a service name, only that service is fetched."
  echo " - Special case: profile-service uses HTTP-filtered logs."
  exit 1
fi

NAMESPACE="ml-ops-${ENVIRONMENT}"
CLUSTER_NAME="ml-ops-${ENVIRONMENT}"

# Discover services from repo structure. macOS still ships Bash 3.2, which
# does not include mapfile/readarray.
SERVICES=()
while IFS= read -r service; do
  SERVICES+=("${service}")
done < <(get_services_list "$PROJECT_ROOT")

ensure_eks_context() {
  echo "Using EKS cluster: ${CLUSTER_NAME} in ${REGION}" >&2
  
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${REGION}" --profile "${AWS_PROFILE}" >/dev/null
  else
    aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${REGION}" >/dev/null
  fi

  if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "ERROR: kubectl cannot reach cluster '${CLUSTER_NAME}'." >&2
    exit 1
  fi
}

fetch_service_logs() {
  SVC="$1"

  PODS=$(kubectl get pods -n "${NAMESPACE}" \
    -l "app=${SVC}" \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)

  if [ -z "${PODS}" ]; then
    PODS=$(kubectl get pods -n "${NAMESPACE}" --no-headers 2>/dev/null \
      | awk -v p="$SVC" '$1 ~ "^"p {print $1}' || true)
  fi

  if [ -z "${PODS}" ]; then
    echo "${SVC}: no pod found (skipping)"
    return 0
  fi

  FIRST_POD=$(echo "${PODS}" | awk '{print $1}')

  echo
  echo "${SVC} -> ${FIRST_POD}"
  kubectl logs -n "${NAMESPACE}" "${FIRST_POD}" || echo "[WARN] Failed to fetch logs"
}


ensure_eks_context

echo "Environment: ${ENVIRONMENT}"
echo "Namespace:   ${NAMESPACE}"
echo
if [ -n "${SERVICE_FILTER}" ]; then
  if [ "${SERVICE_FILTER}" = "profile-service" ]; then
    # profile-service has noisy OTEL lines; filter them with the dedicated helper
    NS="${NAMESPACE}" LABEL="app=${SERVICE_FILTER}" "${SCRIPT_DIR}/k8s-filter-logs.sh"
  else
    fetch_service_logs "${SERVICE_FILTER}"
  fi
else
  for SVC in "${SERVICES[@]}"; do
    fetch_service_logs "${SVC}"
  done
fi
