#! /usr/bin/env sh
set -e

TARGETARCH=$1
ENVIRONMENT=${2:-local-dev}
BUILD_MODE=${3:-full}
EXCLUDE=${4:-}

if [ -z "$TARGETARCH" ]; then
  echo "Error: No target architecture provided."
  echo "Usage: './docker-build-services.sh [amd64|arm64] [local-dev|cicd|staging|prod] [full|prebuilt] [exclude_list]'"
  exit 1
fi

BIGHILL_ROOT=$(git rev-parse --show-toplevel)

"$BIGHILL_ROOT/scripts/docker-build.sh" "$ENVIRONMENT" "$TARGETARCH" "$BUILD_MODE" "$EXCLUDE"
