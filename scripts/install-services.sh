#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

is_excluded() {
  local SERVICE="$1"
  local EXCLUDE_CSV="${EXCLUDE_SERVICES:-${CI_TEST_EXCLUDE_SERVICES:-${CI_EXCLUDE_SERVICES:-}}}"

  if [ -z "$EXCLUDE_CSV" ]; then
    return 1
  fi

  local EXCLUDED
  IFS=',' read -ra EXCLUDE_ARRAY <<< "$EXCLUDE_CSV"
  for EXCLUDED in "${EXCLUDE_ARRAY[@]}"; do
    local NORMALIZED_EXCLUDED
    local NORMALIZED_SERVICE
    NORMALIZED_EXCLUDED="$(echo "${EXCLUDED//-/_}" | xargs)"
    NORMALIZED_SERVICE="${SERVICE//-/_}"
    if [ "$NORMALIZED_SERVICE" = "$NORMALIZED_EXCLUDED" ] ||
      [ "$NORMALIZED_SERVICE" = "${NORMALIZED_EXCLUDED}_service" ] ||
      [ "${NORMALIZED_SERVICE}_service" = "${NORMALIZED_EXCLUDED}_service" ]; then
      return 0
    fi
  done
  return 1
}

install_services() {
  local SERVICES_TO_INSTALL=("$@")

  if [ ${#SERVICES_TO_INSTALL[@]} -eq 0 ]; then
    while IFS= read -r SERVICE; do
      SERVICES_TO_INSTALL+=("$SERVICE")
    done < <(get_services_list "$PROJECT_ROOT")
  fi

  echo "Installing services: ${SERVICES_TO_INSTALL[*]}"

  local SERVICE_DIR
  for SERVICE_DIR in "${SERVICES_TO_INSTALL[@]}"; do
    if is_excluded "$SERVICE_DIR"; then
      echo "Skipping ${SERVICE_DIR} (excluded)"
      continue
    fi

    local INSTALL_SCRIPT="${PROJECT_ROOT}/${SERVICE_DIR}/scripts/install.sh"
    if [ -f "$INSTALL_SCRIPT" ]; then
      echo "Installing ${SERVICE_DIR}..."
      bash "$INSTALL_SCRIPT"
    else
      echo "Warning: No install script found for ${SERVICE_DIR}" >&2
    fi
  done

  echo "Service installation complete."
}

install_services "$@"
