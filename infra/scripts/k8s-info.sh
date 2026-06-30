#! /usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source common functions for service discovery
source "${SCRIPT_DIR}/k8s-common.sh"

# Discover services from repo structure
mapfile -t SERVICES < <(get_services_list "$PROJECT_ROOT")
# Add api-gateway which is not a *_service directory
SERVICES+=("api-gateway")

check_helm_release() {
  RELEASE="$1"
  if ! helm status "${RELEASE}" >/dev/null 2>&1; then
    echo "${RELEASE} deployment failed."
    return 1
  fi
}

check_deployment() {
  helm list -A

  echo "Getting deployed pod images:"
  for P in "${SERVICES[@]}"; do
    printf '%s -> ' "$P"
    POD=$(kubectl get pods --no-headers 2>/dev/null | awk -v p="$P" '$1 ~ "^"p {print $1; exit}')
    if [ -n "$POD" ]; then
      kubectl get pod "$POD" -o=jsonpath='{.spec.containers[0].image}{"\n"}'
    else
      echo "(no pod found)"
    fi
  done

  for SVC in "${SERVICES[@]}"; do
    check_helm_release "${SVC}.0.0.1"
  done

  check_helm_release "postgres"

  if ! kubectl get deployment redis >/dev/null 2>&1; then
    echo "Redis deployment failed."
    exit 1
  fi

  echo "All services deployed successfully."
  kubectl get svc
  kubectl get pods -o wide
}

check_kafka() {
  kubectl get statefulset kafka
  kubectl get pods -l app=kafka
  kubectl get pvc | grep kafka || true
  kubectl get storageclass
}


check_deployment
check_kafka
