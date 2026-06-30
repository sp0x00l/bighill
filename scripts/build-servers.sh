#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

resolve_service_dirs() {
  local PROJECT_ROOT="$1"
  shift
  local EXCLUDED="$1"
  shift
  local REQUESTED=("$@")
  local RESOLVED=()

  if [ "${#REQUESTED[@]}" -eq 0 ]; then
    for SERVICE in $(discover_services "$PROJECT_ROOT"); do
      if [ -n "$EXCLUDED" ] && [[ "$SERVICE" == *"$EXCLUDED"* ]]; then
        continue
      fi
      RESOLVED+=("$SERVICE")
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

build_protobuffers() {
  local PROJECT_ROOT="$1"

  echo "Building protobuffers..."
  cd "${PROJECT_ROOT}/data_contracts"
  . ./scripts/build.sh
  cd "$PROJECT_ROOT"
}

install_service() {
  local PROJECT_ROOT="$1"
  local SERVICE_DIR="$2"

  echo "Installing ${SERVICE_DIR}..."
  cd "${PROJECT_ROOT}/${SERVICE_DIR}"
  . ./scripts/install.sh
  cd "$PROJECT_ROOT"
}

build_service() {
  local PROJECT_ROOT="$1"
  local SERVICE_DIR="$2"
  local ENV="$3"

  cd "${PROJECT_ROOT}/${SERVICE_DIR}"
  . ./scripts/build.sh "$ENV"
  cd "$PROJECT_ROOT"
}

build_all_services() {
  local ENV="$1"
  local PROJECT_ROOT="$2"
  shift 2
  local SERVICE_DIRS=("$@")

  build_protobuffers "$PROJECT_ROOT"

  echo "Building services..."
  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    install_service "$PROJECT_ROOT" "$SERVICE_DIR"
    build_service "$PROJECT_ROOT" "$SERVICE_DIR" "$ENV"
  done
}

main() {
  local ENV="${1:-local-dev}"
  local EXCLUDE="${2:-}"
  shift 2 || shift || true
  local REQUESTED_SERVICES=()
  if [ $# -gt 0 ]; then
    REQUESTED_SERVICES=("$@")
  fi

  local SERVICE_DIRS
  SERVICE_DIRS=($(resolve_service_dirs "$PROJECT_ROOT" "$EXCLUDE" "${REQUESTED_SERVICES[@]+"${REQUESTED_SERVICES[@]}"}"))

  if [ "${#SERVICE_DIRS[@]}" -eq 0 ]; then
    echo "No services found to build."
    exit 1
  fi

  build_all_services "$ENV" "$PROJECT_ROOT" "${SERVICE_DIRS[@]}"
}

main "$@"
