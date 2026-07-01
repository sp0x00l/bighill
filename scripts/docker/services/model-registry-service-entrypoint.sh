#! /usr/bin/env sh
echo "Waiting for ${MODEL_REGISTRY_DB_NAME} database"
RETRIES=5

IS_DB_READY="pg_isready -h ${PGHOST} -U ${MODEL_REGISTRY_DB_USER} -d ${MODEL_REGISTRY_DB_NAME}"

eval $IS_DB_READY > /dev/null 2>&1
until [ $? -eq 0 ];
do
    RETRIES=$(( RETRIES - 1 ))
    if [ $RETRIES -eq 0 ] ; then
        echo "Failed to find database ${MODEL_REGISTRY_DB_NAME}, bye!"
        exit
    fi

    echo "Waiting for postgres server to start, ${RETRIES} remaining attempts..."
    sleep 5s
    $IS_DB_READY > /dev/null 2>&1
done

/go/bin/model_registry_service
