#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS=localhost,127.0.0.1
    export TOOL_SERVICE_ALLOWED_ORG_IDS=11111111-1111-1111-1111-111111111111
elif [ "$1" = "staging" ]; then
    export TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS=
    export TOOL_SERVICE_ALLOWED_ORG_IDS=
elif [ "$1" = "prod" ]; then
    export TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS=
    export TOOL_SERVICE_ALLOWED_ORG_IDS=
else
    echo "Error: Invalid environment provided to tool_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export TOOL_SERVICE_NAME=tool-service
export TOOL_SERVICE_GRPC_PORT=7084
export TOOL_SERVICE_HEALTHCHECK_PORT=5065
export TOOL_SERVICE_HTTP_TOOL_TIMEOUT_MS=1500
export TOOL_SERVICE_HTTP_TOOL_MAX_RESPONSE_BYTES=65536
export TOOL_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export TOOL_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export TOOL_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export TOOL_SERVICE_BUILD_VERSION=0.0.1
