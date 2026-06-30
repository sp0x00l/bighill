#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

install_lambda()
{
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"

    cd "$PROJECT_ROOT/api_gateway/lambda/$1"
    rm -f go.mod go.sum
    go mod init "$1"
    go mod edit -replace lib/shared_lib=../../../shared_lib
    go mod edit -replace lib/data_contracts_lib=../../../data_contracts/build/protobufs
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

install_test()
{
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"

    cd "$PROJECT_ROOT/api_gateway/test"
    rm -f go.mod go.sum
    go mod init test
    go mod edit -replace lib/shared_lib=../../shared_lib
    go mod edit -replace lib/data_contracts_lib=../../data_contracts/build/protobufs
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

if [ ! -f "$PROJECT_ROOT/shared_lib/go.mod" ]; then
    cd "$PROJECT_ROOT/shared_lib"
    bash scripts/install.sh
fi

if [ ! -f "$PROJECT_ROOT/data_contracts/build/protobufs/go.mod" ]; then
    cd "$PROJECT_ROOT/data_contracts"
    make install
fi

install_lambda api
install_lambda auth
install_test
