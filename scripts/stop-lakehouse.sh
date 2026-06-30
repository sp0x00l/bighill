#!/usr/bin/env bash
set -euo pipefail

stop_lakehouse()
{
    local SCRIPT_DIR
    local PROJECT_ROOT
    local CURRENT_DIR
    local COMPOSE_FILE

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/common.sh"
    PROJECT_ROOT="$(get_project_root)"
    CURRENT_DIR="$(pwd)"
    COMPOSE_FILE="$PROJECT_ROOT/docker-compose-lakehouse.yml"

    if ! check_docker; then
        exit 1
    fi

    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    cd "$CURRENT_DIR"
}

stop_lakehouse
