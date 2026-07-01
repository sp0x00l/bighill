#! /usr/bin/env sh
. ./scripts/config.sh $1

go build -ldflags="-X 'main.Version=${TRAINING_SERVICE_BUILD_VERSION}'" -v -o "build/$TRAINING_SERVICE_NAME" -tags debug
./build/$TRAINING_SERVICE_NAME
