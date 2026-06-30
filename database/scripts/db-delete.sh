#!/usr/bin/env bash

echo "Deleting databases..."

. "${BASH_SOURCE%/*}/config.sh" local-dev

delete_database()
{
    local SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    if [ -z "$POSTGRES_DATA" ]; then
        echo "POSTGRES_DATA is not set. Please set it in the config file."
        exit 1
    fi
    local DATA_ROOT="${PROJECT_ROOT}/database/$POSTGRES_DATA"
    echo "Deleting databases, ${BIGHILL_DB_NAMES}, in $DATA_ROOT"

    rm  -rf "$DATA_ROOT"
    echo "the database data folder location is $POSTGRES_DATA."
    echo "databases, ${BIGHILL_DB_NAMES}, were deleted"
}
 
delete_database 
