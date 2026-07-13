#!/usr/bin/env bash
set -euo pipefail

test()
{
    echo "socket service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"

    rm -rf $BIGHILL_ROOT/test_results/socket_service
    mkdir -p $BIGHILL_ROOT/test_results/socket_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/socket_service
    . ./scripts/config.sh $1
    ginkgo -timeout=120s -r -v --output-dir=../test_results/socket_service -procs=1 -race

    echo "socket service test complete"
    cd $CURRENT_DIR
}

test $1
