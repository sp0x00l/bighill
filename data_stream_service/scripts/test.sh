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

    setup_cgo

    rm -rf $BIGHILL_ROOT/test_results/data_stream_service
    mkdir -p $BIGHILL_ROOT/test_results/data_stream_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/data_stream_service
    . ./scripts/config.sh $1
    ginkgo -timeout=120s -r -v --output-dir=../test_results/data_stream_service -procs=1 -race --label-filter='!external-data-source'

    echo "data stream service test complete"
    cd $CURRENT_DIR
}

test $1
