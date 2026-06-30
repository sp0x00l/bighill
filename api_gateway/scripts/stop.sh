#! /usr/bin/env sh

stop_sam_local()
{
    PIDS=$(ps aux | grep 'sam local start-api' | grep -v grep | awk '{print $2}')

    if [ -z "$PIDS" ]; then
        return 0
    fi

    kill $PIDS
}

stop_sam_local
