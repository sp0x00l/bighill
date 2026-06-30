#!/usr/bin/env bash

set -eu

CURRENT_DIR=$(pwd)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$PROJECT_ROOT"

ENVIRONMENT="local-dev"
SERVICE="${1:-}"

# shellcheck disable=SC1090
. "$PROJECT_ROOT/database/scripts/config.sh" "$ENVIRONMENT"

echo "This is a dev env database migration script"
echo "To run the database migrations in the Docker or K8s environment: "
echo "in the root, use the migrate.Dockerfile and/or K8s infra/helm/platform/helm-chart/templates/migrations"

migrate_service_db()
{
    local PROJECT_ROOT="$PROJECT_ROOT"
    service_dir="$1"
    db_name="$2"

    echo "Migrating local dev database, $db_name, for $service_dir service"
    for f in "$PROJECT_ROOT/$service_dir/db/migrations/"*up*.sql; do
        echo "Running migrations: $f"
        pwd
        ls -l "$f"
        PGUSER="$BIGHILL_DB_ADMIN" PGPASSWORD="$BIGHILL_DB_ADMIN_PASSWORD" \
          psql -d "$db_name" -f "$f"
    done
}

migrate_all_service_dbs()
{
    local PROJECT_ROOT="$PROJECT_ROOT"
    SERVICE_DIRS=$(find . -maxdepth 1 -type d -name "*service*")

    for SERVICE_DIR in $SERVICE_DIRS; do
        SERVICE_DIR=${SERVICE_DIR#./}   # strip leading ./

        DB_NAME="$("$PROJECT_ROOT/database/scripts/db-name.sh" "$SERVICE_DIR" | awk '{print $1}')"

        if [ -z "$DB_NAME" ]; then
            echo "The service $SERVICE_DIR is not registered for migrations in config.sh BIGHILL_DB_NAMES."
            echo "$SERVICE_DIR will not be migrated."
        else
            if PGUSER="$BIGHILL_DB_ADMIN" PGPASSWORD="$BIGHILL_DB_ADMIN_PASSWORD" \
               psql -d "$DB_NAME" -c '\q' >/dev/null 2>&1; then
                migrate_service_db "$SERVICE_DIR" "$DB_NAME"
                echo "database $DB_NAME migration complete."
            else
                echo "Database $DB_NAME does not exist. Try running 'make install'."
            fi
        fi
    done
}

if [ -z "$SERVICE" ]; then
    migrate_all_service_dbs
else
    DB_NAME="$("$PROJECT_ROOT/database/scripts/db-name.sh" "$SERVICE" | awk '{print $1}')"

    if PGUSER="$BIGHILL_DB_ADMIN" PGPASSWORD="$BIGHILL_DB_ADMIN_PASSWORD" \
       psql -d "$DB_NAME" -c '\q' >/dev/null 2>&1; then
        migrate_service_db "$SERVICE" "$DB_NAME"
        echo "database $DB_NAME migration complete."
    else 
        echo "Database $DB_NAME does not exist. Try running 'make install'"
    fi
fi

cd "$CURRENT_DIR"
