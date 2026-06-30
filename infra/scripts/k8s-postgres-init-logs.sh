#! /usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
JOB_NAME="${2:-ml-ops-infra-ml-ops-services-postgres-bootstrap}"
REGION="${AWS_REGION:-us-east-1}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 [staging|prod] [job-name-optional]"
  exit 1
fi

NAMESPACE="ml-ops-${ENVIRONMENT}"
CLUSTER_NAME="ml-ops-${ENVIRONMENT}"

configure_kubectl() {
  local CLUSTER="$1"
  local REGION_ARG="$2"

  echo "Configuring kubectl for EKS cluster: ${CLUSTER} (${REGION_ARG})"
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}" --profile "${AWS_PROFILE}" >/dev/null
  else
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}" >/dev/null
  fi
}

fetch_job_pods() {
  local TARGET_NAMESPACE="$1"
  local TARGET_JOB="$2"

  kubectl get pods -n "${TARGET_NAMESPACE}" \
    --selector="job-name=${TARGET_JOB}" \
    -o jsonpath='{.items[*].metadata.name}'
}

stream_job_logs() {
  local TARGET_NAMESPACE="$1"
  local TARGET_JOB="$2"

  local PODS=$(fetch_job_pods "$TARGET_NAMESPACE" "$TARGET_JOB")

  if [ -z "$PODS" ]; then
    echo "No pods found for job '${TARGET_JOB}' in namespace '${TARGET_NAMESPACE}'."
    echo "Note: This is expected if the job completed successfully - Helm hooks are cleaned up after success."
    echo "To see historical logs, check CloudWatch or run the job manually."
    exit 0
  fi

  for POD in $PODS; do
    echo "Logs for pod: ${POD} (job: ${TARGET_JOB})"

    if ! kubectl logs -n "${TARGET_NAMESPACE}" "${POD}"; then
      echo "[normal logs failed, trying --previous]"
      kubectl logs -n "${TARGET_NAMESPACE}" "${POD}" --previous || true
    fi

    echo
  done
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
echo "Using namespace: ${NAMESPACE}"
echo "Target job: ${JOB_NAME}"
stream_job_logs "$NAMESPACE" "$JOB_NAME"
