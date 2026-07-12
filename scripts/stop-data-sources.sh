#!/usr/bin/env bash
set -euo pipefail

stop_data_sources()
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
    COMPOSE_FILE="$PROJECT_ROOT/docker-compose-data.yml"

    if ! check_docker; then
        echo "Docker is not available; nothing to stop for data sources."
        return 0
    fi

    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" down -v
    cd "$CURRENT_DIR"
}

stop_data_sources
