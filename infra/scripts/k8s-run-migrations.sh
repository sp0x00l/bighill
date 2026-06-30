#!/usr/bin/env bash
set -euo pipefail

ENV="${1:-staging}"
NAMESPACE="ml-ops-${ENV}"
RELEASE_NAME="ml-ops-infra"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="ml-ops-${ENV}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALUES_BASE="$PROJECT_ROOT/infra/helm/platform/helm-chart/values.yaml"
VALUES_ENV="$PROJECT_ROOT/infra/helm/platform/helm-chart/${ENV}-values.yaml"
HELM_CHART_DIR="$PROJECT_ROOT/infra/helm/platform/helm-chart"

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

determine_migrations_image() {
  local IMAGE_OVERRIDE="$1"
  local VALUES_BASE_FILE="$2"
  local VALUES_ENV_FILE="$3"

  if [ -n "$IMAGE_OVERRIDE" ]; then
    echo "$IMAGE_OVERRIDE"
    return
  fi

  if ! command -v yq >/dev/null 2>&1; then
    echo "Error: yq is required to parse Helm values for migrations image." >&2
    exit 1
  fi

  local REPO=""
  local TAG=""

  if [ -f "$VALUES_ENV_FILE" ]; then
    REPO=$(yq e '.migrations.image.repository' "$VALUES_ENV_FILE")
    TAG=$(yq e '.migrations.image.tag' "$VALUES_ENV_FILE")
  fi

  if [ -z "${REPO:-}" ] || [ "${REPO:-}" = "null" ]; then
    REPO=$(yq e '.migrations.image.repository' "$VALUES_BASE_FILE")
  fi
  if [ -z "${TAG:-}" ] || [ "${TAG:-}" = "null" ]; then
    TAG=$(yq e '.migrations.image.tag' "$VALUES_BASE_FILE")
  fi

  if [ -z "$REPO" ] || [ -z "$TAG" ]; then
    echo "Error: Could not determine migrations image (repo/tag missing)." >&2
    exit 1
  fi

  echo "${REPO}:${TAG}"
}

verify_migrations_image_exists() {
  local IMAGE="$1"
  local REGION_ARG="$2"

  echo "Verifying migrations image exists: ${IMAGE}"
  local REGISTRY=$(echo "$IMAGE" | cut -d/ -f1)
  local REPO=$(echo "$IMAGE" | cut -d: -f1 | cut -d/ -f2-)
  local TAG=$(echo "$IMAGE" | cut -d: -f2)
  
  if [[ "$REGISTRY" == *.amazonaws.com ]]; then
    if aws ecr describe-images --repository-name "$REPO" --image-ids imageTag="$TAG" --region "$REGION_ARG" >/dev/null 2>&1; then
      echo "Image verified in ECR: ${IMAGE}"
    else
      echo "Warning: Could not verify image in ECR. K8s will attempt to pull it."
    fi
  fi
}

run_migrations() {
  local RELEASE="$1"
  local CHART_DIR="$2"
  local NAMESPACE_ARG="$3"
  local VALUES_BASE_FILE="$4"
  local VALUES_ENV_FILE="$5"

  echo "Running database migrations in namespace '${NAMESPACE_ARG}'..."
  echo "Triggering migration via helm upgrade..."
  if helm upgrade "$RELEASE" "$CHART_DIR" \
    -n "$NAMESPACE_ARG" \
    -f "$VALUES_BASE_FILE" \
    -f "$VALUES_ENV_FILE" \
    --reuse-values \
    --wait \
    --timeout 5m; then
    echo "Database migrations completed successfully"
  else
    echo "Database migrations failed"
    echo "Check helm status and events:"
    echo "  kubectl get events -n $NAMESPACE_ARG --sort-by='.lastTimestamp' | tail -20"
    exit 1
  fi
}

configure_kubectl "$CLUSTER_NAME" "$REGION"
MIGRATIONS_IMAGE=$(determine_migrations_image "${MIGRATIONS_IMAGE:-}" "$VALUES_BASE" "$VALUES_ENV")
verify_migrations_image_exists "$MIGRATIONS_IMAGE" "$REGION"
run_migrations "$RELEASE_NAME" "$HELM_CHART_DIR" "$NAMESPACE" "$VALUES_BASE" "$VALUES_ENV"
