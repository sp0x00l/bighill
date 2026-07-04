#! /usr/bin/env bash

set -euo pipefail

install() {
    local CURRENT_DIR=$(pwd)
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local DATA_CONTRACT_ROOT=$PROJECT_ROOT/data_contracts

    mkdir -p "$DATA_CONTRACT_ROOT/build/protobufs"
    cd "$DATA_CONTRACT_ROOT/build/protobufs"


    cat > go.mod <<'EOF'
module lib/data_contracts_lib

go 1.24

require (
    google.golang.org/grpc v1.77.0
    google.golang.org/protobuf v1.36.11
)
EOF

    # Download dependencies (avoid tidy here to keep required modules even before proto generation)
    GOFLAGS= go mod download

    cd "$CURRENT_DIR"
}

install
