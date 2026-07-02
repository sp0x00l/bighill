#!/usr/bin/env bash
set -euo pipefail

install()
{
    echo "model serving service install"
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

    cd "$PROJECT_ROOT/model_serving_service"
    rm -f go.mod go.sum

    go mod init model_serving_service
    go mod edit -go=1.25.0
    go mod edit -replace lib/shared_lib=../shared_lib
    go get k8s.io/apimachinery@v0.35.0
    go get k8s.io/client-go@v0.35.0
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

install
