#!/usr/bin/env bash

set -euo pipefail

install()
{
    local CURRENT_DIR="$(pwd)"
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

    if [ ! -f "$PROJECT_ROOT/shared_lib/go.mod" ]; then
        cd "$PROJECT_ROOT/shared_lib"
        bash scripts/install.sh
    fi

    if [ ! -f "$PROJECT_ROOT/data_contracts/build/protobufs/go.mod" ]; then
        "$PROJECT_ROOT/data_contracts/scripts/install.sh"
    fi

    cd "$PROJECT_ROOT/data_stream_service"
    rm -f go.mod go.sum

    go mod init data_stream_service
    go mod edit -replace lib/shared_lib=../shared_lib
    go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

install
