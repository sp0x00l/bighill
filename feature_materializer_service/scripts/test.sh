#!/usr/bin/env bash
set -euo pipefail

setup_cgo() {
    export CGO_ENABLED=1
    if [ -z "${CC:-}" ] && command -v gcc &>/dev/null; then
        export CC=gcc
    fi
}

test()
{
    echo "feature materializer service test"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"

    setup_cgo

    if [ ! -f "$BIGHILL_ROOT/pdf_extractor_lib/cpp/build/bin/libgo_pdf_extractor_lib.a" ]; then
        "$BIGHILL_ROOT/pdf_extractor_lib/scripts/build_cpp.sh"
    fi

    rm -rf $BIGHILL_ROOT/test_results/feature_materializer_service
    mkdir -p $BIGHILL_ROOT/test_results/feature_materializer_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/feature_materializer_service
    . ./scripts/config.sh $1
    ginkgo -timeout=120s -r -v --output-dir=../test_results/feature_materializer_service -procs=1 -race

    echo "feature materializer service test complete"
    cd $CURRENT_DIR
}

test $1
