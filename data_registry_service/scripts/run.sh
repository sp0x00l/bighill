#! /usr/bin/env sh

. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${DATA_REGISTRY_SERVICE_BUILD_VERSION}' -X 'main.ServiceName=${DATA_REGISTRY_SERVICE_NAME}'" -v -o "build/$DATA_REGISTRY_SERVICE_NAME" -tags debug
./build/$DATA_REGISTRY_SERVICE_NAME
