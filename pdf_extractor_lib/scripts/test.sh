#!/usr/bin/env bash
set -euo pipefail

setup_cgo() {
    export CGO_ENABLED=1
    if [ -z "${CC:-}" ] && command -v gcc &>/dev/null; then
        export CC=gcc
    fi
}

test_pdf_extractor_lib() {
    echo "pdf extractor lib test"

    local ENV="$1"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT
    PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local OUTPUT_DIR="${PROJECT_ROOT}/test_results/pdf_extractor_lib"

    setup_cgo

    if [ ! -f "$PROJECT_ROOT/pdf_extractor_lib/cpp/build/bin/libgo_pdf_extractor_lib.a" ]; then
        "$PROJECT_ROOT/pdf_extractor_lib/scripts/build_cpp.sh"
    fi

    if [ -e "$OUTPUT_DIR" ]; then
        rm -rf "$OUTPUT_DIR"
    fi
    mkdir -p "$OUTPUT_DIR"

    . "$PROJECT_ROOT/shared_lib/scripts/config.sh" "$ENV"

    cd "$PROJECT_ROOT/pdf_extractor_lib"
    ginkgo -timeout=5m -r -v -procs=1 -race

    cd "$CURRENT_DIR"
    echo "pdf extractor lib test complete"
}

test_pdf_extractor_lib "$1"

