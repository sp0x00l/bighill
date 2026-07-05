#! /usr/bin/env sh

BIGHILL_ROOT=$(pwd)

. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1
. $BIGHILL_ROOT/database/scripts/config.sh $1
. $BIGHILL_ROOT/api_gateway/scripts/config.sh

cd $BIGHILL_ROOT/ingestion_service/
. ./scripts/config.sh $1
. ./scripts/test.sh


cd $BIGHILL_ROOT/data_registry_service/
. ./scripts/config.sh $1
. ./scripts/test.sh

echo "starting servers for api gateway test"
cd $BIGHILL_ROOT
. ./scripts/start-servers.sh

cd $BIGHILL_ROOT/api_gateway
. ./scripts/config.sh
. ./scripts/test.sh
