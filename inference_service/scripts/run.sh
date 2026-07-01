#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${INFERENCE_SERVICE_BUILD_VERSION}'" -v -o "build/$INFERENCE_SERVICE_NAME" -tags debug
./build/$INFERENCE_SERVICE_NAME
