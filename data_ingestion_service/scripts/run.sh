#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${DATA_INGESTION_SERVICE_BUILD_VERSION}'" -v -o "build/$DATA_INGESTION_SERVICE_NAME" -tags debug
./build/$DATA_INGESTION_SERVICE_NAME
