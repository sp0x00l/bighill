#! /usr/bin/env sh
set -e

build()
{
  echo "ingestion service build"
  local CURRENT_DIR=$(pwd)
  local BIGHILL_ROOT=$(git rev-parse --show-toplevel)

  cd $BIGHILL_ROOT/ingestion_service
  if [ ! -d "build" ]; then
    mkdir -p $BIGHILL_ROOT/ingestion_service/build
  else
    rm -rf $BIGHILL_ROOT/ingestion_service/build/*
  fi

  . $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
  . ./scripts/config.sh $1
  go build -ldflags="-X 'main.Version=${INGESTION_SERVICE_BUILD_VERSION}'" -v -o "build/$INGESTION_SERVICE_NAME"

  cd $CURRENT_DIR
  echo "ingestion service build complete"
}

build $1
