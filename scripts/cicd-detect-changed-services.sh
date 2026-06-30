#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

get_base_ref() {
  local BASE_REF="$1"

  if [ -n "${BASE_REF}" ]; then
    echo "${BASE_REF}"
    return 0
  fi

  if [ -n "${CI_BASE_SHA:-}" ]; then
    echo "${CI_BASE_SHA}"
    return 0
  fi

  if [ -n "${GITHUB_BASE_SHA:-}" ]; then
    echo "${GITHUB_BASE_SHA}"
    return 0
  fi

  if [ -n "${GITHUB_BASE_REF:-}" ]; then
    echo "origin/${GITHUB_BASE_REF}"
    return 0
  fi

  if git rev-parse --verify origin/main >/dev/null 2>&1; then
    git merge-base origin/main HEAD
    return 0
  fi

  if git rev-parse --verify main >/dev/null 2>&1; then
    git merge-base main HEAD
    return 0
  fi

  git rev-parse HEAD~1
}

list_changed_files() {
  local BASE_REF="$1"
  local HEAD_REF="$2"

  git diff --name-only "${BASE_REF}..${HEAD_REF}"
}

has_global_change() {
  local -a CHANGED_FILES=("$@")

  for FILE in "${CHANGED_FILES[@]}"; do
    if [[ "${FILE}" == shared_lib/* || "${FILE}" == data_contracts/* ]]; then
      return 0
    fi
  done

  return 1
}

detect_changed_services() {
  local PROJECT_ROOT="$1"
  local EXCLUDE="${2:-}"
  shift 2
  local -a CHANGED_FILES=("$@")
  local INCLUDE_ALL="${INCLUDE_ALL_SERVICES:-false}"
  local -a EXCLUDE_ARRAY=()
  if [ -n "${EXCLUDE}" ]; then
    IFS=',' read -ra EXCLUDE_ARRAY <<< "${EXCLUDE}"
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
  local -a CHANGED_SERVICES=()

  for SERVICE_DIR in $(discover_services "${PROJECT_ROOT}"); do
    if is_excluded "${SERVICE_DIR}"; then
      continue
    fi
    if [ "${INCLUDE_ALL}" = "true" ]; then
      CHANGED_SERVICES+=("${SERVICE_DIR}")
      continue
    fi
    local SERVICE_NAME
    SERVICE_NAME="$(service_dir_to_name "${SERVICE_DIR}")"
    local DOCKERFILE="${SERVICE_NAME}.Dockerfile"

    for FILE in "${CHANGED_FILES[@]}"; do
      if [[ "${FILE}" == "${SERVICE_DIR}/"* || "${FILE}" == "${DOCKERFILE}" ]]; then
        CHANGED_SERVICES+=("${SERVICE_DIR}")
        break
      fi
    done
  done

  printf '%s ' "${CHANGED_SERVICES[@]}" | sed 's/ $//'
}

detect_changes() {
  local BASE_REF_INPUT="${1:-}"
  local HEAD_REF="${2:-HEAD}"
  local EXCLUDE="${3:-}"
  local PROJECT_ROOT
  PROJECT_ROOT="$(get_project_root)"

  cd "${PROJECT_ROOT}"

  local BASE_REF
  BASE_REF="$(get_base_ref "${BASE_REF_INPUT}")"

  local -a CHANGED_FILES=()
  mapfile -t CHANGED_FILES < <(list_changed_files "${BASE_REF}" "${HEAD_REF}")
  if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
    return 0
  fi

  if has_global_change "${CHANGED_FILES[@]}"; then
    local SERVICES
    SERVICES="$(INCLUDE_ALL_SERVICES=true detect_changed_services "${PROJECT_ROOT}" "${EXCLUDE}" "${CHANGED_FILES[@]}")"
    printf '%s\n' "${SERVICES}"
    return 0
  fi

  detect_changed_services "${PROJECT_ROOT}" "${EXCLUDE}" "${CHANGED_FILES[@]}"
}

detect_changes "$@"
