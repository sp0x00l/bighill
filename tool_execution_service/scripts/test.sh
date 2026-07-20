#!/usr/bin/env bash
set -euo pipefail

test()
{
    echo "tool execution service test"

    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"
    . "$BIGHILL_ROOT/scripts/common.sh"

    . "$BIGHILL_ROOT/shared_lib/scripts/config.sh" "$1"
    cd "$BIGHILL_ROOT/tool_execution_service"
    . ./scripts/config.sh "$1"
    stop_service_binary_for_tests "tool_execution_service" "$BIGHILL_ROOT"
    go test ./...

    echo "tool execution service test complete"
    cd "$CURRENT_DIR"
}

test "$1"
