#! /usr/bin/env sh

export BIGHILL_DB_NAMES="bighill_data_registry_db bighill_ingestion_db bighill_feature_materializer_db bighill_inference_db bighill_model_registry_db bighill_tenant_db bighill_tool_db"


# if [ "$1" = "local-dev" ]; then

# elif [ "$1" = "staging" ]; then

# elif [ "$1" = "prod" ]; then

# else 
#     echo "Error: invalid environment param in database config"
#     echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
#     exit 1
# fi

export BIGHILL_DB_ADMIN=bighill_admin
export BIGHILL_DB_ADMIN_PASSWORD=${BIGHILL_DB_ADMIN_PASSWORD:-LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw}
export BIGHILL_DB_MIGRATIONS_USER=bighill_user
export BIGHILL_DB_PASSWORD=${BIGHILL_DB_PASSWORD:-LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw}

export PGSSLMODE=disable
export POSTGRES_DEBUG=true
export POSTGRES_VERSION=17
export POSTGRES_DATA=data # official postgres docker: default data env is: /var/lib/postgresql/data
export PGDATABASE=postgres # default database
export PGPORT=5432
export PGHOST="127.0.0.1"
export POSTGRES_HOST="${PGHOST}"
export POSTGRES_PORT="${PGPORT}"
