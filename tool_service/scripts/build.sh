#! /usr/bin/env sh
set -e

. ./scripts/config.sh $1

if [ ! -d "build" ]; then
  mkdir -p build
else
  rm -rf build/*
fi

go build -ldflags="-X 'main.Version=${TOOL_SERVICE_BUILD_VERSION}'" -v -o "build/$TOOL_SERVICE_NAME"
