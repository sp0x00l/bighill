
#! /usr/bin/env sh

if [ -z "$1" ]; then
    set -- $BIGHILL_DB_NAMES
fi

for db_name in "$@"; do
    # CASCADE is used to remove all data from related tables but not used - it is not necessary. Here for completeness.
    echo "removing all data from tables in $db_name database"
    tables=$(PGPASSWORD=${BIGHILL_DB_ADMIN_PASSWORD} psql -d $db_name -U${BIGHILL_DB_ADMIN} -t -c "SELECT tablename FROM pg_tables WHERE schemaname='$db_name';")

    for table_name in $tables; do
        PGPASSWORD=${BIGHILL_DB_ADMIN_PASSWORD} psql -d $db_name -U${BIGHILL_DB_ADMIN} -c "TRUNCATE $db_name.$table_name CASCADE;"
    done

    # functions have been removed, recreate them
    . ./scripts/setup/common/0007-db-create-functions.sh $db_name
done

