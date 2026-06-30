#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENVIRONMENT="local-dev"
SERVICE_NAME="${1:-}"

if [ -z "$SERVICE_NAME" ]; then
    echo "Error: Missing required arguments." >&2
    echo "Usage: './db-name.sh [service_dir]'" >&2
    exit 1
fi
# shellcheck disable=SC1090
. "$PROJECT_ROOT/database/scripts/config.sh" "$ENVIRONMENT"

DB_NAME=$(basename "$SERVICE_NAME" | sed 's/_service//')
DB_NAME="bighill_${DB_NAME}_db"
DB_NAME=$(echo ${BIGHILL_DB_NAMES[@]} | grep -o "$DB_NAME")

if [ -n "$DB_NAME" ]; then
    echo "$DB_NAME"
fi
