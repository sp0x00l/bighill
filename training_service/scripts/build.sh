#! /usr/bin/env sh
set -e

build()
{
  echo "training service build"
  local CURRENT_DIR=$(pwd)
  local BIGHILL_ROOT=$(git rev-parse --show-toplevel)

  cd $BIGHILL_ROOT/training_service
  if [ ! -d "build" ]; then
    mkdir -p $BIGHILL_ROOT/training_service/build
  else
    rm -rf $BIGHILL_ROOT/training_service/build/*
  fi

  . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
  . ./scripts/config.sh $1
  go build -ldflags="-X 'main.Version=${TRAINING_SERVICE_BUILD_VERSION}'" -v -o "build/$TRAINING_SERVICE_NAME"

  cd $CURRENT_DIR
  echo "training service build complete"
}

build $1
