#!/usr/bin/env bash

ENVIRONMENT="local-dev"
. "${BASH_SOURCE%/*}/config.sh" "$ENVIRONMENT"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

stop_brew_service()
{
    if brew services list | grep -q "postgresql@$POSTGRES_VERSION" | grep -q "started"; then
        brew services stop postgresql@14
        echo "brew Postgres service stopped. Postgres will be started with 'pg_ctl' command with a handle the data directory."
        sleep 3
    fi
}

start_db()
{
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local DATA_ROOT="${PROJECT_ROOT}/database/$POSTGRES_DATA"

    if [ ! -d "$DATA_ROOT" ]; then
        mkdir -p "$DATA_ROOT"
        echo "$DATA_ROOT"
        # chown "$USER:$(id -gn)" "$DATA_ROOT"
        # chmod 700 $DATA_ROOT
    fi

    if [ ! -f "$DATA_ROOT/PG_VERSION" ]; then
        echo "$POSTGRES_VERSION" > "$DATA_ROOT/PG_VERSION"
    fi

    local FILE="$DATA_ROOT/postmaster.pid"
    if [ ! -f "$FILE" ]; then
        pg_ctl start -l logfile -D "$DATA_ROOT" -o "-c config_file=$PROJECT_ROOT/database/conf/postgresql.conf"
        sleep 5
    fi
}

start_db 
. "${PROJECT_ROOT}/database/scripts/setup/db-common-init.sh"
