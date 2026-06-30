#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

discover_services() {
  local PROJECT_ROOT="${1:-$PROJECT_ROOT}"
  local SERVICES=()
  
  for DIR in "${PROJECT_ROOT}"/*_service; do
    if [ -d "$DIR" ]; then
      local SERVICE_DIR_NAME=$(basename "$DIR")
      local SERVICE_NAME="${SERVICE_DIR_NAME//_/-}"
      SERVICES+=("$SERVICE_NAME")
    fi
  done
  
  echo "${SERVICES[@]}"
}

get_services_list() {
  local PROJECT_ROOT="${1:-$PROJECT_ROOT}"
  discover_services "$PROJECT_ROOT" | tr ' ' '\n'
}

get_services_string() {
  local PROJECT_ROOT="${1:-$PROJECT_ROOT}"
  discover_services "$PROJECT_ROOT"
}

service_exists() {
  local SERVICE_NAME="$1"
  local PROJECT_ROOT="${2:-$PROJECT_ROOT}"
  local SERVICES=$(discover_services "$PROJECT_ROOT")
  
  for SVC in $SERVICES; do
    if [ "$SVC" = "$SERVICE_NAME" ]; then
      return 0
    fi
  done
  return 1
}

INFRA_SERVICES="kafka redis"

export -f discover_services
export -f get_services_list
export -f get_services_string
export -f service_exists
