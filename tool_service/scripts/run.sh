#! /usr/bin/env sh
set -e

. ./scripts/config.sh $1

mkdir -p build
go build -ldflags="-X 'main.Version=${TOOL_SERVICE_BUILD_VERSION}'" -v -o "build/$TOOL_SERVICE_NAME" -tags debug
./build/$TOOL_SERVICE_NAME
