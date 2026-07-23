#!/usr/bin/env bash
set -euo pipefail

test()
{
    echo "training service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"
    . "$BIGHILL_ROOT/scripts/common.sh"

    rm -rf $BIGHILL_ROOT/test_results/training_service
    mkdir -p $BIGHILL_ROOT/test_results/training_service

    . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
    cd $BIGHILL_ROOT/training_service
    . ./scripts/config.sh $1
    stop_service "training_service"
    ginkgo -timeout=120s -r -v --output-dir=../test_results/training_service -procs=1 -race
    PYTHONPATH="$BIGHILL_ROOT/shared_py:$BIGHILL_ROOT/training_service/training_jobs" python3 -m unittest discover -s "$BIGHILL_ROOT/training_service/test/training_jobs/tests" -p '*_test.py'

    echo "training service test complete"
    cd $CURRENT_DIR
}

test $1
