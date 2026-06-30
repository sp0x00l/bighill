#! /usr/bin/env bash

kill_proc()
{
    local ENV=$1
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    . $PROJECT_ROOT/profile_service/scripts/config.sh $ENV

    ps aux | grep "/build/$PROFILE_SERVICE_NAME" | grep -v grep | awk '{print $2}' | xargs kill -9
}

kill_proc $1
