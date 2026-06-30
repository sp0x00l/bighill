#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${DATA_STREAM_SERVICE_BUILD_VERSION}'" -v -o "build/$DATA_STREAM_SERVICE_NAME" -tags debug
./build/$DATA_STREAM_SERVICE_NAME
