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

psql_admin() {
  PGUSER="$DEFAULT_ADMIN_USER" PGPASSWORD="$DEFAULT_ADMIN_PASSWORD" \
    psql --set ON_ERROR_STOP=1 "$@"
}

create_permissions() {
  local db="$1"
  local user="$2"

  echo "creating read/write permissions in schema '$db' for user '$user'"
  psql_admin -d "$db" -c "GRANT CONNECT ON DATABASE \"$db\" TO \"$user\";"
  psql_admin -d "$db" -c "GRANT CREATE, USAGE ON SCHEMA \"$db\" TO \"$user\";"
  psql_admin -d "$db" -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"$db\" TO \"$user\";"
  psql_admin -d "$db" -c "ALTER DEFAULT PRIVILEGES IN SCHEMA \"$db\" GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"$user\";"
  psql_admin -d "$db" -c "GRANT USAGE ON ALL SEQUENCES IN SCHEMA \"$db\" TO \"$user\";"
  psql_admin -d "$db" -c "ALTER DEFAULT PRIVILEGES IN SCHEMA \"$db\" GRANT USAGE ON SEQUENCES TO \"$user\";"
  psql_admin -d "$db" -c "ALTER DEFAULT PRIVILEGES GRANT ALL ON FUNCTIONS TO \"$user\";"

  # Lock DB down from PUBLIC, like your original script
  psql_admin -d "$db" -c "REVOKE CONNECT ON DATABASE \"$db\" FROM PUBLIC;"
}

remove_permissions() {
  local db="$1"
  local user="$2"

  echo "removing CONNECT on '$db' for '$user'"
  psql_admin -d "$db" -c "REVOKE CONNECT ON DATABASE \"$db\" FROM \"$user\";"
}

for db in $DB_NAMES; do
  user="${db}_user"
  create_permissions "$db" "$user"

  # Revoke access for other DB users to this DB
  for other_db in $DB_NAMES; do
    if [ "$db" != "$other_db" ]; then
      other_user="${other_db}_user"
      remove_permissions "$db" "$other_user"
    fi
  done
done
