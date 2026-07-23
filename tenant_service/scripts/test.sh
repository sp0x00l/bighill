#!/usr/bin/env bash
set -euo pipefail

setup_cgo() {
    export CGO_ENABLED=1
    if [ -z "${CC:-}" ] && command -v gcc &>/dev/null; then
        export CC=gcc
    fi
}

run_tenant_service_tests() {
    echo "tenant service test"

    local ENV="$1"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT
    PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local OUTPUT_DIR="${PROJECT_ROOT}/test_results/tenant_service"
    . "$PROJECT_ROOT/scripts/common.sh"

    setup_cgo

    if [ -e "$OUTPUT_DIR" ]; then
        rm -rf "$OUTPUT_DIR"
    fi
    mkdir -p "$OUTPUT_DIR"

    . "$PROJECT_ROOT/shared_lib/scripts/config.sh" "$ENV"
    cd "$PROJECT_ROOT/tenant_service"
    . ./scripts/config.sh "$ENV"
    stop_service "tenant_service"

    ginkgo -timeout=300s -r -v -procs=1 -race

    cd "$CURRENT_DIR"
    echo "tenant service test complete"
}

run_tenant_service_tests "$1"
