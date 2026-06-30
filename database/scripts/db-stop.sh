#!/usr/bin/env bash

ENVIRONMENT="local-dev"
. "${BASH_SOURCE%/*}/config.sh" "$ENVIRONMENT"

stop_database() {
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local DATA_ROOT="${PROJECT_ROOT}/database/$POSTGRES_DATA"
    if [ ! -d "$DATA_ROOT" ]; then
        mkdir -p "$DATA_ROOT"
    fi

    local FILE="$DATA_ROOT/postmaster.pid"
    echo "Database location: $FILE"
    if test -f "$FILE"; then
        if pg_ctl -D "$DATA_ROOT" status >/dev/null 2>&1; then
            echo "Stopping all databases"
            if ! pg_ctl -D "$DATA_ROOT" stop 2>&1; then
                echo "Warning: pg_ctl stop failed; removing stale postmaster.pid"
                rm -f "$FILE"
            fi
        else
            echo "Database was not running; removing stale postmaster.pid"
            rm -f "$FILE"
        fi
    else
        echo "Databases were not started"
    fi
}

stop_database

echo "Databases stopped"
