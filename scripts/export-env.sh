#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

if [ -z "${1:-}" ]; then
  echo "Usage: $0 [local-dev|cicd|staging|prod]"
  exit 1
fi

export ENVIRONMENT="$1"

export_env_configs "$ENVIRONMENT" "$PROJECT_ROOT"
