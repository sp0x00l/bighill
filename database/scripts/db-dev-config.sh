#! /usr/bin/env sh

export BIGHILL_DB_NAMES="bighill_data_registry_db bighill_data_ingestion_db"

export BIGHILL_DB_MIGRATIONS_USER=bighill_user
export BIGHILL_DB_PASSWORD=LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw
export BIGHILL_DB_ADMIN=bighill_admin
export BIGHILL_DB_ADMIN_PASSWORD=LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw

export PGSSLMODE=disable
export POSTGRES_DEBUG=true
export POSTGRES_DATA=data # official postgres docker: default data env is: /var/lib/postgresql/data
export PGDATABASE=postgres # default database
export PGPORT=5432
export PGHOST="0.0.0.0"
