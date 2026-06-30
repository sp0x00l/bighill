#! /usr/bin/env sh

TARGETARCH=$1

if [ -z "$TARGETARCH" ]; then
  echo "Error: No target architecture provided."
  echo "Usage: './docker-build-services.sh [amd64|arm64]'"
  exit 1
else
  echo "Building for $TARGETARCH"
fi


BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/database/scripts/db-dev-config.sh

API_GATEWAY_BUILD_VERSION=0.0.1
MIGRATIONS_BUILD_VERSION=0.0.1
POSTGRES_DB_VERSION=0.0.1
DATA_INGESTION_SERVICE_BUILD_VERSION=0.0.1
DATA_REGISTRY_SERVICE_BUILD_VERSION=0.0.1


gather_db_migrations()
{    
    echo "Gathering db migrations"
    local SERVICE_DIRS=($(find . -maxdepth 1 -type d -name "*service*"))
    for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
        local DB_NAME=$(basename $SERVICE_DIR | sed 's/_service//')
        if [ -z "$DB_NAME" ]; then
            echo "Error: Service $1 not with no suffix '_service'."
            exit 1
        fi
        SERVICE_DIR=${SERVICE_DIR:2}
        local DB_NAME=$($BIGHILL_ROOT/database/scripts/db-name.sh $SERVICE_DIR)
        if [ -z "$DB_NAME" ]; then
            echo "Service database migrations in $SERVICE_DIR were not referenced in db-dev-config.sh"
            echo "$SERVICE_DIR not found in BIGHILL_DB_NAMES so it will not be migrated." 
        else
            echo "Migrating $DB_NAME"
            mkdir -p $BIGHILL_ROOT/build/tmp/db-migrations/$DB_NAME
            cp -r $SERVICE_DIR/db/migrations/* $BIGHILL_ROOT/build/tmp/db-migrations/$DB_NAME
        fi
    done
}

build_service()
{
    local SERVICE_NAME=$1
    local SERVICE_VERSION=$2
    local FILE_NAME=$(echo $SERVICE_NAME | sed 's/_/-/g').Dockerfile
    echo "====================================== $FILE_NAME START ======================================"
    rm $BIGHILL_ROOT/$SERVICE_NAME/go.mod 2> /dev/null
    echo "Building $SERVICE_NAME:$SERVICE_VERSION with $FILE_NAME"
    cd $BIGHILL_ROOT
    docker build --no-cache --platform linux/${TARGETARCH} --build-arg TARGETARCH=${TARGETARCH} --build-arg BUILD_VERSION_REQUIRED=$SERVICE_VERSION -t $SERVICE_NAME:$SERVICE_VERSION -f $FILE_NAME .
    echo "====================================== $FILE_NAME END ======================================"
}

build_db()
{
    mkdir -p $BIGHILL_ROOT/build/tmp/db-migrations
    gather_db_migrations
    cd $BIGHILL_ROOT
    docker build --no-cache -t migrations:$MIGRATIONS_BUILD_VERSION -f migrations.Dockerfile .
}

build_protobuffers()
{
    cd $BIGHILL_ROOT/data_contracts
    . ./scripts/build.sh
    cd $BIGHILL_ROOT
}

build_api_gateway()
{
    rm $BIGHILL_ROOT/api_gateway/lambda/api/go.mod 2> /dev/null
    rm $BIGHILL_ROOT/api_gateway/lambda/auth/go.mod 2> /dev/null
    docker build --build-arg BUILD_VERSION_REQUIRED=$API_GATEWAY_BUILD_VERSION -t api-gateway:$API_GATEWAY_BUILD_VERSION -f api-gateway.Dockerfile .
}


echo "building protobuffers"
build_protobuffers

echo "building db migrations"
build_db

echo "building all services"
export DOCKER_BUILDKIT=1
build_service data_ingestion_service $DATA_INGESTION_SERVICE_BUILD_VERSION
build_service data_registry_service $DATA_REGISTRY_SERVICE_BUILD_VERSION


echo building api-gateway
build_api_gateway
