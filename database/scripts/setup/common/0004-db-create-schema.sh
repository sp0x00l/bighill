#! /usr/bin/env sh
set -e

# Determine databases to operate on
if [ "$#" -gt 0 ]; then
  DB_NAMES="$*"
elif [ -n "${BIGHILL_DB_NAMES:-}" ]; then
  DB_NAMES="$BIGHILL_DB_NAMES"
else
  echo "No database names provided (args or BIGHILL_DB_NAMES)" >&2
  exit 1
fi

# Admin identity (macOS, docker, k8s)
DEFAULT_ADMIN_USER="${BIGHILL_DB_ADMIN:-${POSTGRES_USERNAME:-${POSTGRES_USER:-postgres}}}"
DEFAULT_ADMIN_PASSWORD="${BIGHILL_DB_ADMIN_PASSWORD:-${POSTGRES_PASSWORD:-${PGPASSWORD:-}}}"

MIGRATIONS_USER="${BIGHILL_DB_MIGRATIONS_USER:-bighill_user}"

psql_admin() {
  PGUSER="$DEFAULT_ADMIN_USER" PGPASSWORD="$DEFAULT_ADMIN_PASSWORD" \
    psql --set ON_ERROR_STOP=1 "$@"
}

clean_schema() {
  local db="$1"
  local schema="$1"  # you originally used DB name as schema name

  echo "cleaning schema '$schema' in database '$db' (if exists)"
  if psql_admin -d "$db" -tAc "SELECT 1 FROM pg_namespace WHERE nspname = '${schema}'" | grep -q 1; then
    psql_admin -d "$db" -c "DROP SCHEMA IF EXISTS \"$schema\" CASCADE;"
  fi
}

create_schema() {
  local db="$1"
  local owner="$2"
  local schema="$1"  # same as DB name

  echo "creating schema '$schema' in database '$db' owned by '$owner'"
  psql_admin -d "$db" -c "CREATE SCHEMA IF NOT EXISTS \"$schema\" AUTHORIZATION \"$owner\";"
}

for db in $DB_NAMES; do
  clean_schema "$db"
  create_schema "$db" "$MIGRATIONS_USER"
done
