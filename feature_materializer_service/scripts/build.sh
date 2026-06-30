#! /usr/bin/env sh
set -e

build()
{
  echo "feature materializer service build"
  local CURRENT_DIR=$(pwd)
  local BIGHILL_ROOT=$(git rev-parse --show-toplevel)

  cd $BIGHILL_ROOT/feature_materializer_service
  if [ ! -d "build" ]; then
    mkdir -p $BIGHILL_ROOT/feature_materializer_service/build
  else
    rm -rf $BIGHILL_ROOT/feature_materializer_service/build/*
  fi

  . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
  . ./scripts/config.sh $1
  go build -ldflags="-X 'main.Version=${FEATURE_MATERIALIZER_SERVICE_BUILD_VERSION}'" -v -o "build/$FEATURE_MATERIALIZER_SERVICE_NAME"

  cd $CURRENT_DIR
  echo "feature materializer service build complete"
}

build $1
