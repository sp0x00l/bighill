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

local_stop_temporal() {
    local TEMPORAL_PID_FILE="$PROJECT_ROOT/tmp/temporal/temporal.pid"

    if command -v brew >/dev/null 2>&1; then
        brew services stop temporal >/dev/null 2>&1 || true
    fi

    if [ -f "$TEMPORAL_PID_FILE" ]; then
        echo "Stopping Temporal dev server..."
        kill "$(cat "$TEMPORAL_PID_FILE")" >/dev/null 2>&1 || true
        rm -f "$TEMPORAL_PID_FILE"
        return
    fi

    lsof -ti:7233 | xargs kill 2>/dev/null || true
    lsof -ti:8233 | xargs kill 2>/dev/null || true
}

local_stop_tei() {
    local TEI_PID_FILE="$PROJECT_ROOT/tmp/tei-embeddings/tei-embeddings.pid"
    local TEI_SCRIPT="$PROJECT_ROOT/scripts/docker/services/tei_stub.py"

    if [ -f "$TEI_PID_FILE" ]; then
        echo "Stopping local TEI-compatible embedding endpoint..."
        kill "$(cat "$TEI_PID_FILE")" >/dev/null 2>&1 || true
        rm -f "$TEI_PID_FILE"
        return
    fi

    if command -v pgrep >/dev/null 2>&1; then
        pgrep -f "$TEI_SCRIPT" | xargs kill 2>/dev/null || true
    fi
}

local_stop_services() {
    local_stop_tei
    local_stop_polaris
    local_stop_temporal

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
    local_stop_tei

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
