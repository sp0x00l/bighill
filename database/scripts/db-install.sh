#!/usr/bin/env bash

ENVIRONMENT="local-dev"
. "${BASH_SOURCE%/*}/config.sh" "$ENVIRONMENT"

install() {
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local DATABASE_ROOT="${PROJECT_ROOT}/database"
    local DATA_ROOT="$DATABASE_ROOT/$POSTGRES_DATA"
    
    mkdir -p "$DATABASE_ROOT/tmp"
    echo "$BIGHILL_DB_ADMIN_PASSWORD" > "$DATABASE_ROOT/tmp/pgpass.txt"

    initdb \
    --username="$BIGHILL_DB_ADMIN" \
    --pwfile="$DATABASE_ROOT/tmp/pgpass.txt" \
    --auth-host=scram-sha-256 \
    --auth-local=scram-sha-256 \
    -D "$DATA_ROOT"
    
    if [ -e "$DATABASE_ROOT/tmp" ]; then
        rm -rf "$DATABASE_ROOT/tmp"
    fi
}

install
