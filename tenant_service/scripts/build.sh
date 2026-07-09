#! /usr/bin/env bash

build()
{
  local CURRENT_DIR=$(pwd)
  local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  cd "$PROJECT_ROOT"

  if [ -e "$PROJECT_ROOT/tenant_service/build" ]; then
    rm -rf "$PROJECT_ROOT/tenant_service/build"
  fi
  mkdir -p "$PROJECT_ROOT/tenant_service/build"

  . $PROJECT_ROOT/shared_lib/scripts/config.sh $1
  cd $PROJECT_ROOT/tenant_service
  . scripts/config.sh $1
  go build -tags=kafka -ldflags="-X 'main.Version=${TENANT_SERVICE_BUILD_VERSION}'" -v -o "$PROJECT_ROOT/tenant_service/build/$TENANT_SERVICE_NAME"
  BUILD_EXIT_CODE=$?

  if [ $BUILD_EXIT_CODE -ne 0 ] || [ ! -f "$PROJECT_ROOT/tenant_service/build/$TENANT_SERVICE_NAME" ]; then
    echo "Build failed, no binary found"
    exit 1
  fi
  echo "Binary built at: $PROJECT_ROOT/tenant_service/build/$TENANT_SERVICE_NAME"

  cd $CURRENT_DIR
}

build $1
