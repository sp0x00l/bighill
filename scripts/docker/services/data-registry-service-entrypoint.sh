#! /usr/bin/env sh
echo "Waiting for ${DATA_REGISTRY_SERVICE_DB_NAME} database"
RETRIES=5

IS_DB_READY="pg_isready -h ${PGHOST} -U ${DATA_REGISTRY_SERVICE_DB_USER} -d ${DATA_REGISTRY_SERVICE_DB_NAME}"

eval $IS_DB_READY > /dev/null 2>&1
until [ $? -eq 0 ];
do
    RETRIES=$(( RETRIES - 1 ))
    if [ $RETRIES -eq 0 ] ; then
        echo "Failed to find database ${DATA_REGISTRY_SERVICE_DB_NAME}, bye!"
        exit
    fi

    echo "Waiting for postgres server to start, ${RETRIES} remaining attempts..."
    sleep 5s
    $IS_DB_READY > /dev/null 2>&1
done

/go/bin/data_registry_service
