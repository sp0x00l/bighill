#!/usr/bin/env bash
set -euo pipefail

test()
{
    echo "agent registry service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"
    . "$BIGHILL_ROOT/scripts/common.sh"

    rm -rf "$BIGHILL_ROOT/test_results/agent_registry_service"
    mkdir -p "$BIGHILL_ROOT/test_results/agent_registry_service"

    . "$BIGHILL_ROOT/shared_lib/scripts/config.sh" "$1"
    cd "$BIGHILL_ROOT/agent_registry_service"
    . ./scripts/config.sh "$1"
    stop_service_binary_for_tests "agent_registry_service" "$BIGHILL_ROOT"
    ginkgo -timeout=120s -r -v --output-dir=../test_results/agent_registry_service -procs=1 -race --skip-package=heuristic --label-filter='!heuristic'
    ginkgo -timeout=120s -v --output-dir=../test_results/agent_registry_service/heuristic -procs=1 -race --label-filter='heuristic' ./test/heuristic

    echo "agent registry service test complete"
    cd "$CURRENT_DIR"
}

test "$1"
