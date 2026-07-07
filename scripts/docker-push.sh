#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

ensure_local_image() {
  local IMAGE="$1"

  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "ERROR: Local image '$IMAGE' not found. Did you run docker-build-services.sh?"
    return 1
  fi
  return 0
}

ensure_ecr_repository() {
  local AWS_REGION="$1"
  local ECR_REPO="$2"

  if aws ecr describe-repositories \
      --repository-names "$ECR_REPO" \
      --region "$AWS_REGION" >/dev/null 2>&1; then
    return 0
  fi

  echo "Creating ECR repository: ${ECR_REPO}"
  aws ecr create-repository \
    --repository-name "$ECR_REPO" \
    --region "$AWS_REGION" >/dev/null
}

ensure_aws_writable() {
  local AWS_REGION="$1"
  local ECR_REPO="$2"

  ensure_ecr_repository "$AWS_REGION" "$ECR_REPO"
  aws ecr put-image-tag-mutability \
    --repository-name "$ECR_REPO" \
    --image-tag-mutability MUTABLE \
    --region "$AWS_REGION"
}

login_ecr() {
  local AWS_REGION="$1"
  local AWS_ACCOUNT_ID="$2"

  aws ecr get-login-password --region "$AWS_REGION" \
    | docker login \
        --username AWS \
        --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
}

push_service_image() {
  local SERVICE_NAME="$1"
  local SERVICE_VERSION="$2"
  local ENVIRONMENT="$3"
  local ECR_URI="$4"

  local LOCAL_IMAGE="${SERVICE_NAME}:${SERVICE_VERSION}"
  local TAG_SERVICE_NAME
  TAG_SERVICE_NAME=$(echo "$SERVICE_NAME" | sed 's/_/-/g')
  local REMOTE_TAG="${TAG_SERVICE_NAME}-${SERVICE_VERSION}-${ENVIRONMENT}"
  local REMOTE_IMAGE="${ECR_URI}:${REMOTE_TAG}"

  echo "Pushing ${LOCAL_IMAGE} -> ${REMOTE_IMAGE}"
  ensure_local_image "$LOCAL_IMAGE" || return 1
  docker tag "$LOCAL_IMAGE" "$REMOTE_IMAGE"
  if [ ! $? -eq 0 ]; then
    echo "ERROR: Failed to tag image '$LOCAL_IMAGE' as '$REMOTE_IMAGE'"
    return 1
  fi

  docker push "$REMOTE_IMAGE"
  if [ ! $? -eq 0 ]; then
    echo "ERROR: Failed to push image '$REMOTE_IMAGE'"
    return 1
  fi

  echo "Pushed $REMOTE_IMAGE"
}

push_migrations_image() {
  local MIGRATIONS_SERVICE_BUILD_VERSION="$1"
  local ENVIRONMENT="$2"
  local ECR_URI="$3"
  local SERVICE_NAME="migrations"
  local SERVICE_VERSION="$MIGRATIONS_SERVICE_BUILD_VERSION"

  local LOCAL_IMAGE="${SERVICE_NAME}:${SERVICE_VERSION}"
  local REMOTE_TAG="${SERVICE_NAME}-${SERVICE_VERSION}-${ENVIRONMENT}"
  local REMOTE_IMAGE="${ECR_URI}:${REMOTE_TAG}"

  echo "Pushing ${LOCAL_IMAGE} -> ${REMOTE_IMAGE}"
  ensure_local_image "$LOCAL_IMAGE" || return 1

  docker tag "$LOCAL_IMAGE" "$REMOTE_IMAGE"
  if [ ! $? -eq 0 ]; then
    echo "ERROR: Failed to tag image '$LOCAL_IMAGE' as '$REMOTE_IMAGE'"
    return 1
  fi

  docker push "$REMOTE_IMAGE"
  if [ ! $? -eq 0 ]; then
    echo "ERROR: Failed to push image '$REMOTE_IMAGE'"
    return 1
  fi

  echo "Pushed $REMOTE_IMAGE"
}

push_discovered_services() {
  local PROJECT_ROOT="$1"
  local ENVIRONMENT="$2"
  local ECR_URI="$3"
  local EXCLUDE="${4:-}"

  # Convert comma-separated exclude list to array
  EXCLUDE_ARRAY=()
  if [ -n "$EXCLUDE" ]; then
    IFS=',' read -ra EXCLUDE_ARRAY <<< "$EXCLUDE"
  fi

  is_excluded() {
    local SERVICE="$1"
    if [ ${#EXCLUDE_ARRAY[@]} -eq 0 ]; then
      return 1
    fi
    for EXCLUDED in "${EXCLUDE_ARRAY[@]}"; do
      local NORMALIZED_EXCLUDED="${EXCLUDED//-/_}"
      local NORMALIZED_SERVICE="${SERVICE//-/_}"
      if [[ "$NORMALIZED_SERVICE" == "$EXCLUDED" ]] || [[ "$NORMALIZED_SERVICE" == "$NORMALIZED_EXCLUDED" ]] || [[ "$NORMALIZED_SERVICE" == "${NORMALIZED_EXCLUDED}_service" ]] || [[ "${NORMALIZED_SERVICE}_service" == "${NORMALIZED_EXCLUDED}_service" ]]; then
        return 0
      fi
    done
    return 1
  }

  for SERVICE_DIR in $(discover_services "$PROJECT_ROOT"); do
    local SERVICE_PREFIX=""
    local SERVICE_NAME_VAR=""
    local SERVICE_VERSION_VAR=""
    local SERVICE_NAME=""
    local SERVICE_VERSION=""

    source_service_config "$SERVICE_DIR" "$ENVIRONMENT" "$PROJECT_ROOT"

    SERVICE_PREFIX="$(service_dir_to_env_prefix "$SERVICE_DIR")"
    SERVICE_NAME_VAR="${SERVICE_PREFIX}_NAME"
    SERVICE_VERSION_VAR="${SERVICE_PREFIX}_BUILD_VERSION"
    SERVICE_NAME="${!SERVICE_NAME_VAR:-}"
    SERVICE_VERSION="${!SERVICE_VERSION_VAR:-}"

    if [ -z "$SERVICE_NAME" ] || [ -z "$SERVICE_VERSION" ]; then
      echo "Warning: Missing ${SERVICE_NAME_VAR} or ${SERVICE_VERSION_VAR} for ${SERVICE_DIR}, skipping..."
      continue
    fi

    if is_excluded "$SERVICE_DIR" || is_excluded "$SERVICE_NAME"; then
      echo "Skipping ${SERVICE_NAME} (excluded)"
      continue
    fi

    push_service_image "$SERVICE_NAME" "$SERVICE_VERSION" "$ENVIRONMENT" "$ECR_URI" || return 1
  done
}

push() {
  local ENVIRONMENT="${1:-}"
  local TARGETARCH="${2:-}"
  local EXCLUDE="${3:-${EXCLUDE_SERVICES:-${CI_EXCLUDE_SERVICES:-}}}"
  local MIGRATIONS_SERVICE_BUILD_VERSION="0.0.1"
  local AWS_REGION="${AWS_REGION:-eu-west-1}"
  local AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
  local ECR_REPO="bighill/mlops"
  local ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"

  if [ -z "$ENVIRONMENT" ]; then
    echo "Error: No environment provided."
    exit 1
  fi

  if [ -z "$TARGETARCH" ]; then
    echo "Error: No target architecture provided."
    exit 1
  fi

  echo "Using ECR repository: ${ECR_URI}"
  if ! login_ecr "$AWS_REGION" "$AWS_ACCOUNT_ID"; then
    echo "Failed to log in to ECR."
    exit 1
  fi

  ensure_aws_writable "$AWS_REGION" "$ECR_REPO" || exit 1
  push_discovered_services "$PROJECT_ROOT" "$ENVIRONMENT" "$ECR_URI" "$EXCLUDE" || exit 1
  push_migrations_image "$MIGRATIONS_SERVICE_BUILD_VERSION" "$ENVIRONMENT" "$ECR_URI" || exit 1
}

push "$@"
