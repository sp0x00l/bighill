#!/usr/bin/env bash

set -euo pipefail

install()
{
    echo "model registry service install"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT
    PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

    if [ ! -f "$PROJECT_ROOT/shared_lib/go.mod" ]; then
        cd "$PROJECT_ROOT/shared_lib"
        bash scripts/install.sh
    fi

    if [ ! -f "$PROJECT_ROOT/data_contracts/build/protobufs/go.mod" ]; then
        cd "$PROJECT_ROOT/data_contracts"
        make build
    fi

    cd "$PROJECT_ROOT/model_registry_service"
    rm -f go.mod go.sum

    go mod init model_registry_service
    go mod edit -go=1.25.0
    go mod edit -replace lib/shared_lib=../shared_lib
    go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
    go mod edit -require k8s.io/apimachinery@v0.35.0
    go mod edit -require k8s.io/client-go@v0.35.0
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

install
