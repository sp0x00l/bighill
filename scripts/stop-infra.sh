#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CURRENT_DIR=$(pwd)
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

if [ -z "${1:-}" ]; then
    echo "Error: No environment provided."
    echo "Usage: './stop-infra.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

ENVIRONMENT="$1"

# Export environment variables from config scripts
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/export-env.sh" "$ENVIRONMENT"

local_stop_polaris() {
    if check_docker; then
        echo "Stopping Polaris catalog infra..."
        cd "$PROJECT_ROOT"
        docker compose -f docker-compose-services.yml stop polaris-catalog polaris-bucket-setup polaris-object-store >/dev/null 2>&1 || true
        docker compose -f docker-compose-services.yml rm -f -v polaris-catalog polaris-bucket-setup polaris-object-store >/dev/null 2>&1 || true
        cd "$CURRENT_DIR"
    fi
}

local_stop_services() {
    local_stop_polaris

    echo "stopping database"
    cd "$PROJECT_ROOT/database"
    . ./scripts/db-stop.sh

    cd "$CURRENT_DIR"

    "$PROJECT_ROOT/scripts/stop-data-sources.sh"

    if command -v brew >/dev/null 2>&1; then
        brew services stop redis || true
        brew services stop kafka || true
    else
        if command -v redis-cli >/dev/null 2>&1; then
            redis-cli shutdown >/dev/null 2>&1 || true
        fi
    fi
}

cicd_stop_services() {
    if check_docker; then
        echo "Stopping docker-compose infra..."
        cd "$PROJECT_ROOT"
        env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f docker-compose-services.yml down -v --remove-orphans
        cd "$CURRENT_DIR"
    else
        echo "Docker is not available; nothing to stop for infra."
    fi
}

case "$ENVIRONMENT" in
  local-dev) local_stop_services ;;
  cicd)      cicd_stop_services ;;
  *)
    echo "Error: stop-infra only manages local-dev and cicd infra."
    exit 1
    ;;
esac
