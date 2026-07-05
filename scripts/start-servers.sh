#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

# CI_TEST_EXCLUDE_SERVICES or EXCLUDE_SERVICES can be comma or space separated
EXCLUDE_SERVICES="${CI_TEST_EXCLUDE_SERVICES:-${EXCLUDE_SERVICES:-}}"

resolve_service_dirs() {
  local PROJECT_ROOT="$1"
  shift
  local EXCLUDE="$1"
  shift
  local REQUESTED=("$@")
  local RESOLVED=()

  # Normalize comma-separated to space-separated for matching
  local EXCLUDE_LIST
  EXCLUDE_LIST="$(echo "$EXCLUDE" | tr ',' ' ')"

  if [ "${#REQUESTED[@]}" -eq 0 ]; then
    for SERVICE in $(discover_services "$PROJECT_ROOT"); do
      local SKIP=false
      for EX in $EXCLUDE_LIST; do
        # Normalize exclude pattern
        local EX_NORM="${EX//-/_}"
        [[ "$EX_NORM" != *_service ]] && EX_NORM="${EX_NORM}_service"
        if [ "$SERVICE" = "$EX_NORM" ]; then
          SKIP=true
          break
        fi
      done
      if [ "$SKIP" = "false" ]; then
        RESOLVED+=("$SERVICE")
      fi
    done
    echo "${RESOLVED[@]}"
    return
  fi

  for NAME in "${REQUESTED[@]}"; do
    local CANDIDATE="${NAME//-/_}"
    if [[ "$CANDIDATE" != *_service ]]; then
      CANDIDATE="${CANDIDATE}_service"
    fi
    if [ -d "${PROJECT_ROOT}/${CANDIDATE}" ]; then
      RESOLVED+=("$CANDIDATE")
    else
      echo "Error: Unknown service '${NAME}' (expected ${CANDIDATE})"
      exit 1
    fi
  done

  echo "${RESOLVED[@]}"
}

build_all_services() {
  local ENV="$1"
  shift
  local SERVICE_DIRS=("$@")
  
  echo "Building protobuffers..."
  cd "${PROJECT_ROOT}/data_contracts"
  . ./scripts/build.sh

  echo "Building services..."
  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    cd "${PROJECT_ROOT}/${SERVICE_DIR}"
    . ./scripts/install.sh
    . ./scripts/build.sh "$ENV"
  done
  
  cd "$PROJECT_ROOT"
}

should_skip_build() {
  local SERVICE_DIRS=("$@")

  if [ -z "${CI_USE_PREBUILT_BINARIES:-}" ]; then
    return 1
  fi

  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    local SERVICE_NAME
    SERVICE_NAME="$(service_dir_to_binary "$SERVICE_DIR")"
    local BINARY="${PROJECT_ROOT}/${SERVICE_DIR}/build/${SERVICE_NAME}"
    if [ ! -f "$BINARY" ]; then
      echo "Prebuilt binary missing for ${SERVICE_DIR} at ${BINARY}"
      return 1
    fi
  done

  echo "Using prebuilt service binaries."
  return 0
}

start_all_services() {
  local ENV="$1"
  shift
  local SERVICE_DIRS=("$@")
  
  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    local SERVICE_NAME=$(service_dir_to_binary "$SERVICE_DIR")
    local BINARY="${PROJECT_ROOT}/${SERVICE_DIR}/build/${SERVICE_NAME}"
    
    if [ ! -f "$BINARY" ]; then
      echo "Error: Binary not found for ${SERVICE_DIR} at ${BINARY}"
      exit 1
    fi
    
    echo "Starting ${SERVICE_NAME}..."
    cd "${PROJECT_ROOT}/${SERVICE_DIR}"
    source_service_config "$SERVICE_DIR" "$ENV" "$PROJECT_ROOT"
    "$BINARY" &
  done
  
  cd "$PROJECT_ROOT"
}

wait_for_service_ports() {
  local ENV="$1"
  local SERVICE_DIR="$2"

  while IFS='|' read -r PORT LABEL; do
    if [ -n "$PORT" ]; then
      wait_for_port "$PORT" "$LABEL"
    fi
  done < <(get_service_ports "$ENV" "$PROJECT_ROOT" "$SERVICE_DIR")
}

has_service_dir() {
  local TARGET="$1"
  shift
  for ITEM in "$@"; do
    if [ "$ITEM" = "$TARGET" ]; then
      return 0
    fi
  done
  return 1
}

build_and_start_servers() {
  local BUILD_PARAM="${1:-run}"
  local ENV="${2:-}"
  shift 2 || true
  local REQUESTED_SERVICES=()
  if [ $# -gt 0 ]; then
    REQUESTED_SERVICES=("$@")
  fi

  if [ -z "$ENV" ]; then
    echo "Error: No environment provided."
    exit 1
  fi

  . "${PROJECT_ROOT}/shared_lib/scripts/config.sh" "$ENV"
  . "${PROJECT_ROOT}/database/scripts/config.sh" "$ENV"

  local SERVICE_DIRS
  SERVICE_DIRS=($(resolve_service_dirs "$PROJECT_ROOT" "$EXCLUDE_SERVICES" "${REQUESTED_SERVICES[@]+"${REQUESTED_SERVICES[@]}"}"))

  if [ -n "$EXCLUDE_SERVICES" ]; then
    echo "Excluding services: ${EXCLUDE_SERVICES}"
  fi

  if [ "$BUILD_PARAM" = "build" ]; then
    if ! should_skip_build "${SERVICE_DIRS[@]}"; then
      build_all_services "$ENV" "${SERVICE_DIRS[@]}"
    fi
  fi

  start_all_services "$ENV" "${SERVICE_DIRS[@]}"

  cd "$PROJECT_ROOT"
  
  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    wait_for_service_ports "$ENV" "$SERVICE_DIR"
  done

}

build_and_start_servers "$@"
