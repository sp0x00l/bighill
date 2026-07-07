#!/usr/bin/env bash

set -euo pipefail

# IMPORTANT NOTE ON ENVIRONMENT CONFIG
# Service configuration is sourced through config.sh, then rendered into a local
# SAM template for the API Gateway.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
API_GATEWAY_DIR="${PROJECT_ROOT}/api_gateway"
ORIGINAL_DIR="$(pwd)"

restore_cwd() {
    cd "${ORIGINAL_DIR}" || true
}

trap restore_cwd EXIT INT TERM

set_aws_credentials()
{
    : "${AWS_ACCESS_KEY_ID:=test}"
    : "${AWS_SECRET_ACCESS_KEY:=test}"
    : "${AWS_SESSION_TOKEN:=test}"
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
}

check_bootstrap_binary()
{
    local API_BOOTSTRAP_PATH="${API_GATEWAY_DIR}/build/api_binary/bootstrap"
    local AUTH_BOOTSTRAP_PATH="${API_GATEWAY_DIR}/build/auth_binary/bootstrap"
    if [ ! -f "${API_BOOTSTRAP_PATH}" ]; then
        echo "ERROR: Bootstrap binary not found at ${API_BOOTSTRAP_PATH}"
        echo "Run 'make build' in api_gateway directory first"
        exit 1
    fi
    if [ ! -f "${AUTH_BOOTSTRAP_PATH}" ]; then
        echo "ERROR: Bootstrap binary not found at ${AUTH_BOOTSTRAP_PATH}"
        echo "Run 'make build' in api_gateway directory first"
        exit 1
    fi
    echo "Bootstrap binaries found"
}

generate_local_template()
{
    "${API_GATEWAY_DIR}/scripts/local-template.sh"
}

load_local_gateway_config()
{
    # shellcheck disable=SC1090
    . "${API_GATEWAY_DIR}/scripts/config.sh" "${ENVIRONMENT:-local-dev}"
    SAM_REDIS_HOST="${REDIS_ADDRESS%:*}"
    SAM_REDIS_PORT="${REDIS_ADDRESS##*:}"
}

start_sam_local()
{
    cd "${API_GATEWAY_DIR}"

    local DOCKER_NETWORK_FLAG=""
    if [[ "$(uname -s)" == "Linux" ]] && [[ -z "${CI:-}" ]] && [[ -z "${GITHUB_ACTIONS:-}" ]]; then
        DOCKER_NETWORK_FLAG="--docker-network host"
    fi

    sam local start-api -t template.local.generated.yml ${DOCKER_NETWORK_FLAG} \
        --parameter-overrides "ParameterKey=RedisHost,ParameterValue=${SAM_REDIS_HOST}" "ParameterKey=RedisPort,ParameterValue=${SAM_REDIS_PORT}" \
        --warm-containers EAGER --log-file /dev/stdout --debug > api-debug.log 2>&1 &
}

wait_for_gateway()
{
    local MAX_WAIT=120
    local WAITED=0
    local PORT=3000

    echo "Waiting for API Gateway to start on port ${PORT}..."
    while ! curl -s "http://127.0.0.1:${PORT}" >/dev/null 2>&1; do
        sleep 2
        WAITED=$((WAITED + 2))
        if [ "${WAITED}" -ge "${MAX_WAIT}" ]; then
            echo "ERROR: API Gateway failed to start within ${MAX_WAIT}s"
            cat "${API_GATEWAY_DIR}/api-debug.log" 2>/dev/null || echo "No log file found"
            exit 1
        fi
        echo "  waited ${WAITED}s..."
    done
    echo "API Gateway is ready"
}

set_aws_credentials
check_bootstrap_binary
generate_local_template
load_local_gateway_config
start_sam_local
wait_for_gateway

trap - EXIT INT TERM
restore_cwd
