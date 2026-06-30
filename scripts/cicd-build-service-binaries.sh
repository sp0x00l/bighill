#!/usr/bin/env bash
set -euo pipefail

build_binaries() {
  local SCRIPT_DIR
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/common.sh"
  local PROJECT_ROOT
  PROJECT_ROOT="$(get_project_root)"

  local ENVIRONMENT
  ENVIRONMENT="${1:-cicd}"
  local EXCLUDE="${2:-}"
  local JOBS
  if [ -n "${JOBS:-}" ]; then
    JOBS="${JOBS}"
  elif command -v nproc &>/dev/null; then
    JOBS="$(nproc)"
  else
    JOBS=4
  fi

  # Convert comma-separated exclude list to array
  local EXCLUDE_ARRAY=()
  if [ -n "$EXCLUDE" ]; then
    IFS=',' read -ra EXCLUDE_ARRAY <<< "$EXCLUDE"
    echo "Excluding services: ${EXCLUDE}"
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

  echo "Building data contracts (protobufs)..."
  cd "${PROJECT_ROOT}/data_contracts"
  make install
  . ./scripts/build.sh

  echo "Running install-all to set up all dependencies..."
  cd "${PROJECT_ROOT}"
  ./scripts/install-all.sh

  echo "Building service binaries with ${JOBS} parallel jobs..."
  local SERVICE_DIRS
  SERVICE_DIRS=($(discover_services "$PROJECT_ROOT"))
  local PIDS=()
  local SERVICE_NAMES=()
  local RUNNING=0
  local FAILED=0

  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    if is_excluded "$SERVICE_DIR"; then
      echo "Skipping ${SERVICE_DIR} (excluded)"
      continue
    fi
    (
      echo "Building ${SERVICE_DIR}..."
      cd "${PROJECT_ROOT}/${SERVICE_DIR}"
      ./scripts/build.sh "${ENVIRONMENT}"
    ) &
    PIDS+=("$!")
    SERVICE_NAMES+=("$SERVICE_DIR")
    RUNNING=$((RUNNING + 1))

    if [ "$RUNNING" -ge "$JOBS" ]; then
      if ! wait "${PIDS[0]}"; then
        echo "ERROR: Build failed for ${SERVICE_NAMES[0]}"
        FAILED=1
      fi
      PIDS=("${PIDS[@]:1}")
      SERVICE_NAMES=("${SERVICE_NAMES[@]:1}")
      RUNNING=$((RUNNING - 1))
    fi
  done

  for i in "${!PIDS[@]}"; do
    if ! wait "${PIDS[$i]}"; then
      echo "ERROR: Build failed for ${SERVICE_NAMES[$i]}"
      FAILED=1
    fi
  done

  if [ "$FAILED" -ne 0 ]; then
    echo "One or more service builds failed"
    exit 1
  fi
}

build_binaries "$@"
