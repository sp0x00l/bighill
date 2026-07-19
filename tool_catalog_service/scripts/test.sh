#!/usr/bin/env bash
set -euo pipefail

test()
{
    echo "tool catalog service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"

    rm -rf "$BIGHILL_ROOT/test_results/tool_catalog_service"
    mkdir -p "$BIGHILL_ROOT/test_results/tool_catalog_service"

    . "$BIGHILL_ROOT/shared_lib/scripts/config.sh" "$1"
    cd "$BIGHILL_ROOT/tool_catalog_service"
    . ./scripts/config.sh "$1"
    ginkgo -timeout=120s -r -v --output-dir=../test_results/tool_catalog_service -procs=1 -race

    echo "tool catalog service test complete"
    cd "$CURRENT_DIR"
}

test "$1"
