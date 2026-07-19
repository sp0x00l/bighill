#!/usr/bin/env bash
set -euo pipefail

build()
{
  echo "agent registry service build"
  local CURRENT_DIR
  CURRENT_DIR="$(pwd)"
  local BIGHILL_ROOT
  BIGHILL_ROOT="$(git rev-parse --show-toplevel)"

  cd "$BIGHILL_ROOT/agent_registry_service"
  rm -rf build
  mkdir -p build

  . "$BIGHILL_ROOT/shared_lib/scripts/config.sh" "$1"
  . ./scripts/config.sh "$1"
  go build -ldflags="-X 'main.Version=${AGENT_REGISTRY_SERVICE_BUILD_VERSION}'" -v -o "build/$AGENT_REGISTRY_SERVICE_NAME"

  cd "$CURRENT_DIR"
  echo "agent registry service build complete"
}

build "$1"
