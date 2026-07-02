#!/usr/bin/env bash
set -euo pipefail

test()
{
    echo "model serving service test"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"

    rm -rf $BIGHILL_ROOT/test_results/model_serving_service
    mkdir -p $BIGHILL_ROOT/test_results/model_serving_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/model_serving_service
    . ./scripts/config.sh $1
    ginkgo -timeout=120s -r -v --output-dir=../test_results/model_serving_service -procs=1 -race

    echo "model serving service test complete"
    cd $CURRENT_DIR
}

test $1
