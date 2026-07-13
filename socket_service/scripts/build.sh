#! /usr/bin/env sh
set -e

build()
{
  echo "socket service build"
  local CURRENT_DIR=$(pwd)
  local BIGHILL_ROOT=$(git rev-parse --show-toplevel)

  cd $BIGHILL_ROOT/socket_service
  if [ ! -d "build" ]; then
    mkdir -p $BIGHILL_ROOT/socket_service/build
  else
    rm -rf $BIGHILL_ROOT/socket_service/build/*
  fi

  . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
  . ./scripts/config.sh $1
  go build -ldflags="-X 'main.Version=${SOCKET_SERVICE_BUILD_VERSION}'" -v -o "build/$SOCKET_SERVICE_NAME"

  cd $CURRENT_DIR
  echo "socket service build complete"
}

build $1
