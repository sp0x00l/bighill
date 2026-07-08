#! /usr/bin/env bash

set -euo pipefail

install()
{
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

    cd "$PROJECT_ROOT/pdf_extractor_lib"
    rm -f go.mod go.sum

    go mod init lib/pdf_extractor_lib
    go mod edit -replace lib/shared_lib=../shared_lib

    go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo@latest
    go mod tidy
    go mod download

    cd "$CURRENT_DIR"
}

install
