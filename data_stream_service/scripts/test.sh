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
    echo "data stream service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"
    . "$BIGHILL_ROOT/scripts/common.sh"

    setup_cgo

    rm -rf $BIGHILL_ROOT/test_results/data_stream_service
    mkdir -p $BIGHILL_ROOT/test_results/data_stream_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/data_stream_service
    . ./scripts/config.sh $1
    stop_service "data_stream_service"
    if [ "${DATA_STREAM_RUN_CORE_TESTS:-true}" = "true" ]; then
        ginkgo -timeout=120s -r -v --output-dir=../test_results/data_stream_service -procs=1 -race --label-filter='!external-data-source'
    fi

    if [ "${DATA_STREAM_RUN_DATASOURCE_TESTS:-true}" = "true" ]; then
        "$BIGHILL_ROOT/scripts/start-data-sources.sh"
        ginkgo -timeout=120s -r -v --output-dir=../test_results/data_stream_service -procs=1 -race --label-filter='external-data-source'
    fi

    echo "data stream service test complete"
    cd $CURRENT_DIR
}

test $1
