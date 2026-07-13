#!/usr/bin/env bash

set -euo pipefail

install()
{
    echo "socket service install"
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

    cd "$PROJECT_ROOT/socket_service"
    rm -f go.mod go.sum

    go mod init socket_service
    go mod edit -go=1.26.4
    go mod edit -replace lib/shared_lib=../shared_lib
    go mod edit -require github.com/go-playground/validator/v10@v10.28.0
    go mod edit -require github.com/gorilla/websocket@v1.4.1
    go mod edit -require github.com/onsi/ginkgo/v2@v2.32.0
    go mod edit -require github.com/onsi/gomega@v1.42.1
    go mod edit -require github.com/redis/rueidis@v1.0.76
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

install
