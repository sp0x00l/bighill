#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

API_GATEWAY_BUILD_VERSION="0.0.1"
MIGRATIONS_SERVICE_BUILD_VERSION="0.0.1"

validate_args() {
  local ENVIRONMENT="$1"
  local TARGETARCH="$2"
  local BUILD_MODE="${3:-full}"
  local EXCLUDE="${4:-}"

  if [ -z "$ENVIRONMENT" ]; then
    echo "Error: No environment provided."
    echo "Usage: './docker-build.sh [local-dev|cicd|staging|prod] [amd64|arm64] [full|prebuilt] [exclude_list]'"
    exit 1
  fi
  echo "Building for $ENVIRONMENT environment"

  if [ -z "$TARGETARCH" ]; then
    echo "Error: No target architecture provided."
    echo "Usage: './docker-build.sh [local-dev|cicd|staging|prod] [amd64|arm64] [full|prebuilt] [exclude_list]'"
    exit 1
  fi
  echo "Building for $TARGETARCH"

  if [[ "$BUILD_MODE" != "full" && "$BUILD_MODE" != "prebuilt" ]]; then
    echo "Error: Invalid build mode. Must be 'full' or 'prebuilt'."
    echo "Usage: './docker-build.sh [local-dev|cicd|staging|prod] [amd64|arm64] [full|prebuilt] [exclude_list]'"
    exit 1
  fi
  echo "Build mode: $BUILD_MODE"

  if [ -n "$EXCLUDE" ]; then
    echo "Excluding services: $EXCLUDE"
  fi
}

CACHE_FROM_DIR=${DOCKER_CACHE_FROM:-/tmp/.buildx-cache}
CACHE_TO_DIR=${DOCKER_CACHE_TO:-/tmp/.buildx-cache-new}

prepare_cache_dirs() {
  mkdir -p "$CACHE_FROM_DIR"
  rm -rf "$CACHE_TO_DIR" 2>/dev/null || true
}

build_protobuffers() {
  cd "${PROJECT_ROOT}/data_contracts"
  # Ensure data_contracts is installed before building
  if [ ! -f "build/protobufs/go.mod" ]; then
    ./scripts/install.sh
  fi
  ./scripts/build.sh
}

build_service() {
  local SERVICE_NAME="$1"
  local SERVICE_VERSION="$2"
  local TARGETARCH="$3"
  local BUILD_MODE="${4:-full}"
  local FILE_NAME=$(echo "$SERVICE_NAME" | sed 's/_/-/g').Dockerfile
  
  rm "${PROJECT_ROOT}/${SERVICE_NAME}/go.mod" 2>/dev/null || true
  echo "Building ${SERVICE_NAME}:${SERVICE_VERSION} with ${FILE_NAME} (mode: ${BUILD_MODE})"
  cd "$PROJECT_ROOT"

  prepare_cache_dirs
  docker buildx build --load --platform "linux/${TARGETARCH}" \
    --build-arg TARGETARCH="${TARGETARCH}" \
    --build-arg BUILD_VERSION_REQUIRED="${SERVICE_VERSION}" \
    --build-arg BUILD_MODE="${BUILD_MODE}" \
    --cache-from type=local,src="$CACHE_FROM_DIR" \
    --cache-to type=local,dest="$CACHE_TO_DIR",mode=max \
    -t "${SERVICE_NAME}:${SERVICE_VERSION}" -f "$FILE_NAME" .
  rm -rf "$CACHE_FROM_DIR" 2>/dev/null || true
  mv "$CACHE_TO_DIR" "$CACHE_FROM_DIR" 2>/dev/null || true
}

build_api_gateway() {
  local TARGETARCH="$1"
  local FILE_NAME="api-gateway.Dockerfile"
  
  rm "${PROJECT_ROOT}/api_gateway/go.mod" 2>/dev/null || true
  echo "Building api-gateway:${API_GATEWAY_BUILD_VERSION} with ${FILE_NAME}"
  cd "$PROJECT_ROOT"

  prepare_cache_dirs
  docker buildx build --load --platform "linux/${TARGETARCH}" \
    --build-arg TARGETARCH="${TARGETARCH}" \
    --build-arg BUILD_VERSION_REQUIRED="${API_GATEWAY_BUILD_VERSION}" \
    --cache-from type=local,src="$CACHE_FROM_DIR" \
    --cache-to type=local,dest="$CACHE_TO_DIR",mode=max \
    -t "api-gateway:${API_GATEWAY_BUILD_VERSION}" -f "$FILE_NAME" .
  rm -rf "$CACHE_FROM_DIR" 2>/dev/null || true
  mv "$CACHE_TO_DIR" "$CACHE_FROM_DIR" 2>/dev/null || true
}

build_db_migrations() {
  local TARGETARCH="$1"
  local FILE_NAME="migrations.Dockerfile"
  
  echo "Building migrations:${MIGRATIONS_SERVICE_BUILD_VERSION} with ${FILE_NAME}"
  cd "$PROJECT_ROOT"

  # Always rebuild migrations without cache to ensure fresh migration files
  docker buildx build --load --platform "linux/${TARGETARCH}" \
    --build-arg TARGETARCH="${TARGETARCH}" \
    --build-arg BUILD_VERSION_REQUIRED="${MIGRATIONS_SERVICE_BUILD_VERSION}" \
    --no-cache \
    -t "migrations:${MIGRATIONS_SERVICE_BUILD_VERSION}" -f "$FILE_NAME" .
}

build_all_docker_images() {
  if [ -z "${1:-}" ]; then
    echo "Error: No environment provided."
    echo "Usage: './docker-build.sh [local-dev|cicd|staging|prod] [arm64|amd64] [full|prebuilt] [exclude_list]'"
    exit 1
  fi
  
  if [ -z "${2:-}" ]; then
    echo "Error: No target architecture provided."
    echo "Usage: './docker-build.sh [local-dev|cicd|staging|prod] [arm64|amd64] [full|prebuilt] [exclude_list]'"
    exit 1
  fi

  local ENVIRONMENT="$1"
  local TARGETARCH="$2"
  local BUILD_MODE="${3:-full}"
  local EXCLUDE="${4:-}"

  validate_args "$ENVIRONMENT" "$TARGETARCH" "$BUILD_MODE" "$EXCLUDE"
  if ! check_docker; then
    exit 1
  fi

  # Source all service configs
  source_all_configs "$ENVIRONMENT" "$PROJECT_ROOT"

  build_protobuffers

  # Convert comma-separated exclude list to array
  EXCLUDE_ARRAY=()
  if [ -n "$EXCLUDE" ]; then
    IFS=',' read -ra EXCLUDE_ARRAY <<< "$EXCLUDE"
  fi

  # Helper function to check if service should be excluded
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

  # Build api-gateway first - it's lightweight and doesn't depend on other services
  if ! is_excluded "api-gateway"; then
    build_api_gateway "$TARGETARCH"
  else
    echo "Skipping api-gateway (excluded)"
  fi

  # Build all discovered services
  for SERVICE_DIR in $(discover_services "$PROJECT_ROOT"); do
    local SERVICE_NAME_VAR=$(echo "${SERVICE_DIR}" | tr '[:lower:]' '[:upper:]')_NAME
    local SERVICE_VERSION_VAR=$(echo "${SERVICE_DIR}" | tr '[:lower:]' '[:upper:]')_BUILD_VERSION
    
    local SERVICE_NAME="${!SERVICE_NAME_VAR:-}"
    local SERVICE_VERSION="${!SERVICE_VERSION_VAR:-0.0.1}"
    
    if [ -n "$SERVICE_NAME" ]; then
      local SERVICE_DOCKERFILE
      SERVICE_DOCKERFILE="$(echo "$SERVICE_NAME" | sed 's/_/-/g').Dockerfile"
      if is_excluded "$SERVICE_DIR" || is_excluded "$SERVICE_NAME"; then
        echo "Skipping ${SERVICE_NAME} (excluded)"
      elif [ ! -f "$PROJECT_ROOT/$SERVICE_DOCKERFILE" ]; then
        echo "Skipping ${SERVICE_NAME} (no ${SERVICE_DOCKERFILE})"
      else
        build_service "$SERVICE_NAME" "$SERVICE_VERSION" "$TARGETARCH" "$BUILD_MODE"
      fi
    else
      echo "Warning: Could not find SERVICE_NAME for ${SERVICE_DIR}, skipping..."
    fi
  done

  if ! is_excluded "migrations"; then
    build_db_migrations "$TARGETARCH"
  else
    echo "Skipping migrations (excluded)"
  fi
}

build_all_docker_images "$1" "$2" "${3:-full}" "${4:-}"
