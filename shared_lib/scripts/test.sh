#!/usr/bin/env bash
set -euo pipefail

setup_cgo() {
    export CGO_ENABLED=1
    if [ -z "${CC:-}" ] && command -v gcc &>/dev/null; then
        export CC=gcc
    fi
}

run_shared_lib_tests() {
    echo "shared lib test"

    local ENV="${1:-}"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT
    PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local OUTPUT_DIR="${PROJECT_ROOT}/test_results/shared_lib"

    setup_cgo

    bash "${SCRIPT_DIR}/install_cwd_test.sh"

    if [ -e "$OUTPUT_DIR" ]; then
        rm -rf "$OUTPUT_DIR"
    fi
    mkdir -p "$OUTPUT_DIR"

    cd "$PROJECT_ROOT/shared_lib"

    GOCOVERDIR="$OUTPUT_DIR" ginkgo -timeout=10m -r -v -procs=1 -race

    cd "$CURRENT_DIR"
    echo "shared lib test complete"
}

run_shared_lib_tests "${1:-}"
