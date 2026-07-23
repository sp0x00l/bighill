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
    echo "data registry service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"
    . "$BIGHILL_ROOT/scripts/common.sh"

    setup_cgo

    rm -rf $BIGHILL_ROOT/test_results/data_registry_service
    mkdir -p $BIGHILL_ROOT/test_results/data_registry_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/data_registry_service
    . ./scripts/config.sh $1
    stop_service "data_registry_service"
    ginkgo -timeout=120s -r -v --output-dir=../test_results/data_registry_service -procs=1 -race

    cd $CURRENT_DIR
    echo "data registry service test complete"
}

test $1
