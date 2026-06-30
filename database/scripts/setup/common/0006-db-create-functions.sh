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

create_ext() {
  local db="$1"
  echo "creating extensions and functions for database '$db'"

  psql_admin -d "$db" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" WITH SCHEMA public;"
  psql_admin -d "$db" -c "CREATE EXTENSION IF NOT EXISTS \"citext\" WITH SCHEMA public;"
  psql_admin -d "$db" -c "SET search_path TO \"$db\",public;"

  local DATE_TRIGGER=$(cat <<'EOF'
create or replace function updated_at_column()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language 'plpgsql';
EOF
)

  psql_admin -d "$db" -c "$DATE_TRIGGER"
}

for db in $DB_NAMES; do
  create_ext "$db"
done
