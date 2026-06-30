#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"
EXCLUDE_SERVICES="${CI_TEST_EXCLUDE_SERVICES:-${EXCLUDE_SERVICES:-}}"

stop_service() {
  local SERVICE_DIR="$1"
  local SERVICE_PATH="${PROJECT_ROOT}/${SERVICE_DIR}"
  local PREFIX
  PREFIX="$(service_dir_to_env_prefix "$SERVICE_DIR")"
  local BASE_PREFIX="${PREFIX%_SERVICE}"

  cd "$SERVICE_PATH"

  local CONFIG_FILE="./scripts/config.sh"
  local SERVICE_PORTS=()
  local PORT

  # Collect all ports the service might be using
  PORT=$(awk -F'=' -v prefix="$PREFIX" -v base="$BASE_PREFIX" '
    $1 ~ ("^export[[:space:]]*" prefix "_API_HTTP_PORT$") || $1 == (prefix "_API_HTTP_PORT") ||
    $1 ~ ("^export[[:space:]]*" base "_API_HTTP_PORT$") || $1 == (base "_API_HTTP_PORT") ||
    $1 ~ ("^export[[:space:]]*" prefix "_HTTP_PORT$") || $1 == (prefix "_HTTP_PORT") ||
    $1 ~ ("^export[[:space:]]*" base "_HTTP_PORT$") || $1 == (base "_HTTP_PORT") {print $2}' "$CONFIG_FILE" | head -1)
  [ -n "$PORT" ] && SERVICE_PORTS+=("$PORT")

  PORT=$(awk -F'=' -v prefix="$PREFIX" -v base="$BASE_PREFIX" '
    $1 ~ ("^export[[:space:]]*" prefix "_API_GRPC_PORT$") || $1 == (prefix "_API_GRPC_PORT") ||
    $1 ~ ("^export[[:space:]]*" base "_API_GRPC_PORT$") || $1 == (base "_API_GRPC_PORT") ||
    $1 ~ ("^export[[:space:]]*" prefix "_GRPC_PORT$") || $1 == (prefix "_GRPC_PORT") ||
    $1 ~ ("^export[[:space:]]*" base "_GRPC_PORT$") || $1 == (base "_GRPC_PORT") {print $2}' "$CONFIG_FILE" | head -1)
  [ -n "$PORT" ] && SERVICE_PORTS+=("$PORT")

  PORT=$(awk -F'=' -v prefix="$PREFIX" -v base="$BASE_PREFIX" '
    $1 ~ ("^export[[:space:]]*" prefix "_WEBSOCKET_PORT$") || $1 == (prefix "_WEBSOCKET_PORT") ||
    $1 ~ ("^export[[:space:]]*" base "_WEBSOCKET_PORT$") || $1 == (base "_WEBSOCKET_PORT") {print $2}' "$CONFIG_FILE" | head -1)
  [ -n "$PORT" ] && SERVICE_PORTS+=("$PORT")

  PORT=$(awk -F'=' -v prefix="$PREFIX" -v base="$BASE_PREFIX" '
    $1 ~ ("^export[[:space:]]*" prefix "_HEALTHCHECK_PORT$") || $1 == (prefix "_HEALTHCHECK_PORT") ||
    $1 ~ ("^export[[:space:]]*" base "_HEALTHCHECK_PORT$") || $1 == (base "_HEALTHCHECK_PORT") {print $2}' "$CONFIG_FILE" | head -1)
  [ -n "$PORT" ] && SERVICE_PORTS+=("$PORT")

  if [ "${#SERVICE_PORTS[@]}" -eq 0 ]; then
    echo "Warning: Could not determine any ports for ${SERVICE_DIR}, skipping..."
    return 0
  fi

  echo "Service port: ${SERVICE_PORTS[0]}"
  cd "$PROJECT_ROOT"

  # Kill processes on all service ports
  for SERVICE_PORT in "${SERVICE_PORTS[@]}"; do
    lsof -ti:"$SERVICE_PORT" | xargs kill 2>/dev/null || true
  done

  # Wait for primary port to be released
  local PRIMARY_PORT="${SERVICE_PORTS[0]}"
  local RETRIES=5
  while lsof -ti:"$PRIMARY_PORT" >/dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -eq 0 ]; then
      echo "Failed to stop ${SERVICE_DIR}, giving up"
      return 1
    fi

    echo "Waiting for ${SERVICE_DIR} to stop, ${RETRIES} remaining attempts..."
    sleep 2

    if [ "$RETRIES" -eq 1 ]; then
      echo "Force stopping ${SERVICE_DIR}..."
      for SERVICE_PORT in "${SERVICE_PORTS[@]}"; do
        lsof -ti:"$SERVICE_PORT" | xargs kill -9 2>/dev/null || true
      done
    else
      for SERVICE_PORT in "${SERVICE_PORTS[@]}"; do
        lsof -ti:"$SERVICE_PORT" | xargs kill 2>/dev/null || true
      done
    fi
  done
  
  echo "${SERVICE_DIR} stopped"
}

stop_all_services() {
  cd "$PROJECT_ROOT"

  local SERVICE_DIRS
  SERVICE_DIRS=($(resolve_service_dirs "$PROJECT_ROOT" "$EXCLUDE_SERVICES"))

  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    stop_service "$SERVICE_DIR" || true
  done
}

resolve_service_dirs() {
  local PROJECT_ROOT="$1"
  shift
  local EXCLUDE="$1"
  shift
  local REQUESTED=("$@")
  local RESOLVED=()

  if [ "${#REQUESTED[@]}" -eq 0 ]; then
    local EXCLUDE_LIST
    EXCLUDE_LIST="$(echo "$EXCLUDE" | tr ',' ' ')"

    for SERVICE in $(discover_services "$PROJECT_ROOT"); do
      local SKIP=false
      for EX in $EXCLUDE_LIST; do
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

stop_requested_services() {
  local REQUESTED=("$@")

  if [ "${#REQUESTED[@]}" -eq 0 ]; then
    stop_all_services
    return
  fi

  local SERVICE_DIRS
  SERVICE_DIRS=($(resolve_service_dirs "$PROJECT_ROOT" "$EXCLUDE_SERVICES" "${REQUESTED[@]}"))

  cd "$PROJECT_ROOT"
  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    stop_service "$SERVICE_DIR" || true
  done
}

stop_requested_services "$@"
