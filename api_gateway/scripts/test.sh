#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
source "${PROJECT_ROOT}/scripts/common.sh"

run_api_gateway_tests()
{
    if [ -z "${1:-}" ]; then
        echo "Error: No environment provided."
        echo "Usage: './test.sh [local-dev|cicd|staging|prod]'"
        exit 1
    fi

    local CURRENT_DIR
    local GATEWAY_ROOT
    local BASE_URL
    CURRENT_DIR="$(pwd)"
    GATEWAY_ROOT="$PROJECT_ROOT/api_gateway"
    BASE_URL="${API_GATEWAY_URL:-http://127.0.0.1:3000}"

    wait_for_api_gateway_ready 120 2 "$BASE_URL"

    cd "$GATEWAY_ROOT"
    if [ "${API_GATEWAY_RUN_CORE_TESTS:-true}" = "true" ]; then
        ginkgo -timeout=1800s -r -v --output-dir=../test_results/api_gateway_tests -procs=1 --label-filter='!real-huggingface && !external-datasource'
    fi

    if [ "${API_GATEWAY_RUN_DATASOURCE_TESTS:-true}" = "true" ]; then
        "$PROJECT_ROOT/scripts/start-data-sources.sh"
        ginkgo -timeout=1800s -r -v --output-dir=../test_results/api_gateway_tests -procs=1 --label-filter='external-datasource'
    fi

    cd "$CURRENT_DIR"
}

run_api_gateway_tests "$1"
