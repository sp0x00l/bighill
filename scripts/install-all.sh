#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"

PROJECT_ROOT="$(get_project_root)"

run_install() {
  local DIR="$1"
  local INSTALL_SCRIPT="${PROJECT_ROOT}/${DIR}/scripts/install.sh"
  if [ -f "$INSTALL_SCRIPT" ]; then
    "$INSTALL_SCRIPT"
  else
    echo "Skipping ${DIR}: missing scripts/install.sh"
  fi
}

# Core libs first.
run_install "data_contracts"
"${PROJECT_ROOT}/data_contracts/scripts/build.sh"
run_install "shared_lib"

# All discovered *_service directories with config.sh.
for SERVICE_DIR in $(discover_services "$PROJECT_ROOT"); do
  run_install "$SERVICE_DIR"
done

# api_gateway is not a *_service directory, so handle explicitly.
run_install "api_gateway"
