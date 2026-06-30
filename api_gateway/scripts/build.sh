#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

build_lambda()
{
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local BUILD_DIR="$PROJECT_ROOT/api_gateway/build"
    local BINARY_NAME="${1}_binary"
    local LAMBDA_DIR="$PROJECT_ROOT/api_gateway/lambda/$1"

    if [ ! -f "$LAMBDA_DIR/go.mod" ]; then
        echo "ERROR: missing $LAMBDA_DIR/go.mod. Run 'make install' in api_gateway first." >&2
        exit 1
    fi

    echo "building api gateway lambda binary for $1"
    mkdir -p "$BUILD_DIR/$BINARY_NAME"
    rm -rf "$BUILD_DIR/$BINARY_NAME/"* 2>/dev/null || true

    cd "$LAMBDA_DIR"
    CGO_ENABLED=0 GOARCH=arm64 GOOS=linux go build -ldflags="-X 'main.Version=${API_GATEWAY_BUILD_VERSION:-}'" -v -o "$BUILD_DIR/$BINARY_NAME/bootstrap" main.go

    if [ ! -f "$BUILD_DIR/$BINARY_NAME/bootstrap" ]; then
        echo "ERROR: Lambda bootstrap not found at $BUILD_DIR/$BINARY_NAME/bootstrap" >&2
        exit 1
    fi

    cd "$CURRENT_DIR"
}

export_zip()
{
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"

    mkdir -p "$PROJECT_ROOT/api_gateway/build/dist"

    echo "exporting api gateway lambda zip for $1"
    cd "$PROJECT_ROOT/api_gateway/build/${1}_binary"
    zip -r9 "../dist/${1}.zip" .
    if [ ! -f "../dist/${1}.zip" ]; then
        echo "ERROR: Lambda zip not created at $PROJECT_ROOT/api_gateway/build/dist/${1}.zip" >&2
        exit 1
    fi
    cd "$CURRENT_DIR"
}

build_lambda "$1"
export_zip "$1"
