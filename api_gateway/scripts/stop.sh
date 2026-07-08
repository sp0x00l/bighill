#! /usr/bin/env sh

stop_sam_local()
{
    PIDS=$(ps aux | grep 'sam local start-api' | grep -v grep | awk '{print $2}')

    if [ -z "$PIDS" ]; then
        return 0
    fi

    kill $PIDS >/dev/null 2>&1 || true
    sleep 1

    for PID in $PIDS; do
        if kill -0 "$PID" >/dev/null 2>&1; then
            kill -9 "$PID" >/dev/null 2>&1 || true
        fi
    done
}

remove_bighill_sam_containers()
{
    if ! command -v docker >/dev/null 2>&1; then
        return 0
    fi

    if ! docker info >/dev/null 2>&1; then
        return 0
    fi

    for FUNCTION_NAME in BighillApiFunction BighillAuthFunction; do
        CONTAINERS=$(docker ps -aq \
            --filter "label=sam.cli.container.type=lambda" \
            --filter "label=sam.cli.function.name=${FUNCTION_NAME}")

        if [ -n "$CONTAINERS" ]; then
            docker rm -f $CONTAINERS >/dev/null 2>&1 || true
        fi
    done
}

stop_sam_local
remove_bighill_sam_containers
