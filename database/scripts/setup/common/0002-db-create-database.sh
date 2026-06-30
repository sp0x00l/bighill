#! /usr/bin/env sh
set -e

# Determine which databases to create
if [ "$#" -gt 0 ]; then
  DB_NAMES="$*"
elif [ -n "${BIGHILL_DB_NAMES:-}" ]; then
  DB_NAMES="$BIGHILL_DB_NAMES"
else
  echo "No database names provided (args or BIGHILL_DB_NAMES)" >&2
  exit 1
fi

# Admin identity (docker/k8s/mac)
# 1. BIGHILL_DB_ADMIN          – your logical admin
# 2. POSTGRES_USERNAME          – if you set it
# 3. POSTGRES_USER              – official image default
# 4. postgres                   – hard default
DEFAULT_ADMIN_USER="${BIGHILL_DB_ADMIN:-${POSTGRES_USERNAME:-${POSTGRES_USER:-postgres}}}"

# Password:
# 1. BIGHILL_DB_ADMIN_PASSWORD – logical admin password
# 2. POSTGRES_PASSWORD          – official image default
# 3. PGPASSWORD                 – fallback
DEFAULT_ADMIN_PASSWORD="${BIGHILL_DB_ADMIN_PASSWORD:-${POSTGRES_PASSWORD:-${PGPASSWORD:-}}}"

psql_admin() {
  PGUSER="$DEFAULT_ADMIN_USER" PGPASSWORD="$DEFAULT_ADMIN_PASSWORD" \
    psql --set ON_ERROR_STOP=1 "$@"
}

createdb_admin() {
  PGUSER="$DEFAULT_ADMIN_USER" PGPASSWORD="$DEFAULT_ADMIN_PASSWORD" \
    createdb "$@"
}

for db in $DB_NAMES; do
  echo "Creating database '$db' (if not exists)"

  if psql_admin -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '${db}'" | grep -q 1; then
    echo "Database '$db' already exists, skipping"
  else
    createdb_admin "$db"
  fi
done
