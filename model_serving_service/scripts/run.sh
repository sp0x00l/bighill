#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${MODEL_SERVING_SERVICE_BUILD_VERSION}'" -v -o "build/$MODEL_SERVING_SERVICE_NAME" -tags debug
./build/$MODEL_SERVING_SERVICE_NAME
