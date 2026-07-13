#! /usr/bin/env sh

. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${SOCKET_SERVICE_BUILD_VERSION}'" -v -o "build/$SOCKET_SERVICE_NAME" -tags debug
./build/$SOCKET_SERVICE_NAME
