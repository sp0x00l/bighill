#!/bin/sh
set -e

# URL-encode a string (handles special characters in passwords)
# Uses printf to convert each byte to hex - works in any POSIX shell
urlencode() {
    _string="$1"
    _encoded=""
    _len=${#_string}
    _i=0
    
    while [ $_i -lt $_len ]; do
        _c=$(printf '%s' "$_string" | cut -c$((_i+1)))
        case "$_c" in
            [a-zA-Z0-9.~_-]) _encoded="${_encoded}${_c}" ;;
            *) _encoded="${_encoded}$(printf '%%%02X' "'$_c")" ;;
        esac
        _i=$((_i+1))
    done
    printf '%s' "$_encoded"
}

# Run migrations for a single database
migrate_db()
{
    DB_NAME="$1"
    DB_USER="$2"

    echo "Waiting for $PGHOST $DB_NAME database"
    RETRIES=30

    READY_USER="${BIGHILL_DB_ADMIN}"
    READY_PASSWORD="${BIGHILL_DB_ADMIN_PASSWORD}"
    if [ -z "${READY_USER:-}" ]; then
        READY_USER="$DB_USER"
    fi
    if [ -z "${READY_PASSWORD:-}" ]; then
        READY_PASSWORD="${BIGHILL_DB_PASSWORD:-}"
    fi

    # Wait for the target database to exist and accept TCP connections.
    until PGPASSWORD="$READY_PASSWORD" \
        psql -h "$PGHOST" -U "$READY_USER" -d postgres -tAc \
        "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" 2>/dev/null | grep -q 1; do
        RETRIES=$(( RETRIES - 1 ))
        if [ "$RETRIES" -le 0 ] ; then
            echo "Failed waiting for database $DB_NAME with user $READY_USER, bye!"
            exit 1
        fi

        echo "Waiting for postgres $DB_NAME to start, ${RETRIES} remaining attempts..."
        sleep 5
    done

    echo "Migrating $DB_NAME database"
    ls -la "/app/migrations/$DB_NAME"

    echo "Running migrations for $DB_NAME..."

    # URL-encode the password to handle special characters
    ENCODED_PASSWORD=$(urlencode "$BIGHILL_DB_ADMIN_PASSWORD")

    if ! migrate \
            -path "/app/migrations/$DB_NAME" \
            -database "postgres://${BIGHILL_DB_ADMIN}:${ENCODED_PASSWORD}@${PGHOST}:${PGPORT}/${DB_NAME}?sslmode=${PGSSLMODE}&search_path=${DB_NAME},public" \
            up; then
        echo "ERROR: Migration of $DB_NAME database failed!"
        exit 1
    fi

    echo "Migration of $DB_NAME database completed successfully"
}

echo "Migrating databases for services in $BIGHILL_DB_NAMES"

# Turn BIGHILL_DB_NAMES into positional parameters
set -- $BIGHILL_DB_NAMES

for db in "$@"; do
    # append _user to the db name to get the db_user name
    # - the db_user name is configured at <project_root><service_name>/scripts/config.sh
    # - the db_user name created in the database/scripts/setup/common/0003-db-create-user.sh
    user="${db}_user"
    migrate_db "$db" "$user"
done

echo "All migrations completed successfully"
