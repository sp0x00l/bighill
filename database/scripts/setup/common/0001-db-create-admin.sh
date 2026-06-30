#! /usr/bin/env sh
set +ue

create_admin_role()
{
    local DEFAULT_ADMIN_USER="$1"
    local DEFAULT_ADMIN_PASSWORD="$2"
    echo "Creating admin user ${BIGHILL_DB_ADMIN}"

    ADMIN_USER_EXISTS=$(psql -d "$PGDATABASE" -tAc "SELECT 1 FROM pg_roles WHERE rolname='${BIGHILL_DB_ADMIN}'" > /dev/null 2>&1)
    if [ "$ADMIN_USER_EXISTS" != "1" ]; then
        echo "creating admin ${BIGHILL_DB_ADMIN}"
        PGUSER="${DEFAULT_ADMIN_USER}" PGPASSWORD="${DEFAULT_ADMIN_PASSWORD}" \
          psql --set ON_ERROR_STOP=1 -c "CREATE ROLE ${BIGHILL_DB_ADMIN} WITH LOGIN SUPERUSER PASSWORD '${BIGHILL_DB_ADMIN_PASSWORD}';" > /dev/null 2>&1
    else
        echo "altering admin ${BIGHILL_DB_ADMIN}"
        PGUSER="${DEFAULT_ADMIN_USER}" PGPASSWORD="${DEFAULT_ADMIN_PASSWORD}" \
          psql --set ON_ERROR_STOP=1 -c "ALTER ROLE ${BIGHILL_DB_ADMIN} WITH SUPERUSER;" > /dev/null 2>&1
        PGUSER="${DEFAULT_ADMIN_USER}" PGPASSWORD="${DEFAULT_ADMIN_PASSWORD}" \
          psql --set ON_ERROR_STOP=1 -c "ALTER ROLE ${BIGHILL_DB_ADMIN} WITH PASSWORD '${BIGHILL_DB_ADMIN_PASSWORD}';" > /dev/null 2>&1
    fi

    if [ "$DEFAULT_ADMIN_USER" != "$BIGHILL_DB_ADMIN" ]; then
        DEFAULT_ADMIN_USER_EXISTS=$(psql -d "$PGDATABASE" -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DEFAULT_ADMIN_USER}'" > /dev/null 2>&1)
        if [ "$DEFAULT_ADMIN_USER_EXISTS" = "1" ]; then 
            echo "dropping old admin role '${DEFAULT_ADMIN_USER}' because it is no longer used"
            PGUSER="${DEFAULT_ADMIN_USER}" PGPASSWORD="${DEFAULT_ADMIN_PASSWORD}" \
              psql --set ON_ERROR_STOP=1 -c "DROP ROLE ${DEFAULT_ADMIN_USER};" > /dev/null 2>&1
        fi
    fi
}

if [ -z "$BIGHILL_DB_ADMIN" ]; then
    echo "Error BIGHILL_DB_ADMIN is not set"
    exit 0
fi

if [ -z "$BIGHILL_DB_ADMIN_PASSWORD" ]; then
    echo "Error BIGHILL_DB_ADMIN_PASSWORD is not set"
    exit 0
fi

case "$OSTYPE" in
    darwin*) 
        echo "local dev environment has admin role created with initdb"
        ;;
    *)
        # Linux / containers: official postgres / alpine path
        if [ -n "${POSTGRES_USERNAME:-}" ]; then
            DEFAULT_ADMIN_USER="$POSTGRES_USERNAME"
        elif [ -n "${POSTGRES_USER:-}" ]; then
            DEFAULT_ADMIN_USER="$POSTGRES_USER"
        else
            DEFAULT_ADMIN_USER="postgres"
        fi

        DEFAULT_ADMIN_PASSWORD="${BIGHILL_DB_ADMIN_PASSWORD:-${POSTGRES_PASSWORD:-${PGPASSWORD:-}}}"
        export PGUSER="$DEFAULT_ADMIN_USER"
        if [ -n "$DEFAULT_ADMIN_PASSWORD" ]; then
            export PGPASSWORD="$DEFAULT_ADMIN_PASSWORD"
        fi

        create_admin_role "$DEFAULT_ADMIN_USER" "$DEFAULT_ADMIN_PASSWORD"
        ;;
esac
