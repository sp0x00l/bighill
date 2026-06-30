#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${FEATURE_MATERIALIZER_SERVICE_BUILD_VERSION}'" -v -o "build/$FEATURE_MATERIALIZER_SERVICE_NAME" -tags debug
./build/$FEATURE_MATERIALIZER_SERVICE_NAME
