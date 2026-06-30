#! /usr/bin/env sh

if [[ $1 != *_service ]]; then
    echo "Invalid service name"
    exit 1
fi

db_name=$(./scripts/db-name.sh $1)

CURRENT_DIR=$(pwd)

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
cd $BIGHILL_ROOT/database
. ./scripts/db-stop.sh
. ./scripts/db-delete.sh
. ./scripts/db-start.sh
. ./scripts/db-setup.sh $db_name
. ./scripts/db-migrate.sh $1 $db_name

cd $CURRENT_DIR
