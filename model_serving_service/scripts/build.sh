#! /usr/bin/env sh
set -e

build()
{
  echo "model serving service build"
  local CURRENT_DIR=$(pwd)
  local BIGHILL_ROOT=$(git rev-parse --show-toplevel)

  cd $BIGHILL_ROOT/model_serving_service

  if [ ! -d "build" ]; then
    mkdir -p $BIGHILL_ROOT/model_serving_service/build
  else
    rm -rf $BIGHILL_ROOT/model_serving_service/build/*
  fi

  . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
  . ./scripts/config.sh $1
  go build -ldflags="-X 'main.Version=${MODEL_SERVING_SERVICE_BUILD_VERSION}'" -v -o "build/$MODEL_SERVING_SERVICE_NAME"

  cd $CURRENT_DIR
  echo "model serving service build complete"
}

build $1
