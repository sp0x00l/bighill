#! /usr/bin/env sh

migrate_db()
{
    echo "Waiting for $PGHOST $1 database"
    RETRIES=5

    IS_DB_READY="pg_isready -h $PGHOST -U $2 -d $1"

    eval $IS_DB_READY > /dev/null 2>&1
    until [ $? -eq 0 ];
    do
        RETRIES=$(( RETRIES - 1 ))
        if [ $RETRIES -eq 0 ] ; then
            echo "Failed to find database $1, bye!"
            exit 1
        fi

        echo "Waiting for postgres $1 to start, ${RETRIES} remaining attempts..."
        sleep 5
        $IS_DB_READY > /dev/null 2>&1
    done

    echo "Migrating $1 database"
    ls -ll /app/migrations/$1
    migrate -path /app/migrations/$1 -database postgres://$BIGHILL_DB_ADMIN:$BIGHILL_DB_ADMIN_PASSWORD@$PGHOST:$PGPORT/$1?sslmode=$PGSSLMODE up

    echo "Migration of $1 database completed successfully"
}

echo "Migrating databases for services in $BIGHILL_DB_NAMES"
set -- $BIGHILL_DB_NAMES
for db in $@; do
    # append _user to the db name 
    # - it is also set in the <project_root><service_name>/scripts/config.sh
    # - it is also set in the database/scripts/setup/common/0003-db-create-user.sh
    user="${db}_user"
    migrate_db $db $user
done

