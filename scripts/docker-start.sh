#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

start_docker() 
{
    local ENVIRONMENT=$1
    local TARGETARCH=$2
    if [ -z "$ENVIRONMENT" ]; then
        echo "Error: No environment provided."
        echo "Usage: './docker-start.sh [local-dev|cicd|staging|prod]'"
        exit 1
    fi
    if [ -z "$TARGETARCH" ]; then
        echo "Error: No target architecture provided."
        echo "Usage: './docker-start.sh [local-dev|cicd|staging|prod][amd64|arm64]'"
        exit 1
    fi

    local CURRENT_DIR="$(pwd)"
    cd "$PROJECT_ROOT"

    echo "exporting environment $ENVIRONMENT"
    export_env_configs "$ENVIRONMENT" "$PROJECT_ROOT"

    # Keep service and data-source stacks together for connector/e2e tests.
    env ENVIRONMENT=${ENVIRONMENT} TARGETARCH=${TARGETARCH} PROJECT_ROOT=${PROJECT_ROOT} \
        docker compose -f docker-compose-services.yml -f docker-compose-data.yml up
    
    cd "$CURRENT_DIR"
}

start_docker "$@"
