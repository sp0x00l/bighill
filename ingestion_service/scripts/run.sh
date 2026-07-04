#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${INGESTION_SERVICE_BUILD_VERSION}'" -v -o "build/$INGESTION_SERVICE_NAME" -tags debug
./build/$INGESTION_SERVICE_NAME
