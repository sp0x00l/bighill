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

# Admin identity (works for mac/dev, docker, k8s)
# 1. BIGHILL_DB_ADMIN
# 2. POSTGRES_USERNAME
# 3. POSTGRES_USER
# 4. postgres
DEFAULT_ADMIN_USER="${BIGHILL_DB_ADMIN:-${POSTGRES_USERNAME:-${POSTGRES_USER:-postgres}}}"

# Password:
# 1. BIGHILL_DB_ADMIN_PASSWORD
# 2. POSTGRES_PASSWORD
# 3. PGPASSWORD
DEFAULT_ADMIN_PASSWORD="${BIGHILL_DB_ADMIN_PASSWORD:-${POSTGRES_PASSWORD:-${PGPASSWORD:-}}}"

MIGRATIONS_USER="${BIGHILL_DB_MIGRATIONS_USER:-bighill_user}"
MIGRATIONS_PASSWORD="${BIGHILL_DB_MIGRATIONS_PASSWORD:-${BIGHILL_DB_PASSWORD:-}}"

ADMIN_DB="${PGDATABASE:-postgres}"

psql_admin() {
  PGUSER="$DEFAULT_ADMIN_USER" PGPASSWORD="$DEFAULT_ADMIN_PASSWORD" \
    psql --set ON_ERROR_STOP=1 "$@"
}

role_exists() {
  local role="$1"
  psql_admin -d "$ADMIN_DB" -tAc "SELECT 1 FROM pg_roles WHERE rolname='${role}'" | grep -q 1
}

create_role_if_needed() {
  local role="$1"
  local password="$2"

  if role_exists "$role"; then
    echo "role '$role' already exists, skipping"
    return
  fi

  echo "creating role '$role'"
  if [ -n "$password" ]; then
    psql_admin -d "$ADMIN_DB" -c "CREATE ROLE \"$role\" LOGIN PASSWORD '${password}';"
  else
    psql_admin -d "$ADMIN_DB" -c "CREATE ROLE \"$role\" LOGIN;"
  fi
  # Grant admin membership so we can SET ROLE (required for Aurora RDS, harmless on standard PostgreSQL)
  psql_admin -d "$ADMIN_DB" -c "GRANT \"$role\" TO \"$DEFAULT_ADMIN_USER\" WITH ADMIN OPTION;"
}

grant_migration_access_to_db() {
  local db="$1"
  echo "granting all access to db '$db' for '$MIGRATIONS_USER'"
  psql_admin -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE \"$db\" TO \"$MIGRATIONS_USER\";"
}

grant_user_access_to_public_schema() {
  local db="$1"
  local user="$2"
  echo "granting public usage access on db '$db' to '$user'"
  psql_admin -d "$db" -c "GRANT USAGE ON SCHEMA public TO \"$user\";"
}

grant_migration_create_on_public() {
  local db="$1"
  echo "granting CREATE, USAGE on public schema in db '$db' to '$MIGRATIONS_USER'"
  psql_admin -d "$db" -c "GRANT CREATE, USAGE ON SCHEMA public TO \"$MIGRATIONS_USER\";"
}

# 1. Global migrations role
create_role_if_needed "$MIGRATIONS_USER" "$MIGRATIONS_PASSWORD"

# 2. Per-database users & grants
for db in $DB_NAMES; do
  app_user="${db}_user"

  create_role_if_needed "$app_user" "${BIGHILL_DB_PASSWORD:-}"
  grant_user_access_to_public_schema "$db" "$app_user"
  grant_migration_access_to_db "$db"
  grant_migration_create_on_public "$db"
done
