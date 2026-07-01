#! /usr/bin/env sh
set -e

build()
{
  echo "inference service build"
  local CURRENT_DIR=$(pwd)
  local BIGHILL_ROOT=$(git rev-parse --show-toplevel)

  cd $BIGHILL_ROOT/inference_service
  if [ ! -d "build" ]; then
    mkdir -p $BIGHILL_ROOT/inference_service/build
  else
    rm -rf $BIGHILL_ROOT/inference_service/build/*
  fi

  . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
  . ./scripts/config.sh $1
  go build -ldflags="-X 'main.Version=${INFERENCE_SERVICE_BUILD_VERSION}'" -v -o "build/$INFERENCE_SERVICE_NAME"

  cd $CURRENT_DIR
  echo "inference service build complete"
}

build $1
