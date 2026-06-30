#! /usr/bin/env bash

run()
{
    local ENV=$1
    local CURRENT_DIR=$(pwd)
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    cd "$PROJECT_ROOT/profile_service"

    . ./scripts/install.sh
    . ./scripts/config.sh $ENV
    . ./scripts/build.sh $ENV

    ./build/$PROFILE_SERVICE_NAME &

    cd $CURRENT_DIR
}

run $1
