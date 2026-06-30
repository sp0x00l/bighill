#! /usr/bin/env sh
set +ue

DB_ROOT_DIR=$(pwd)
cd "$DB_ROOT_DIR/scripts/setup/common"

DB_NAME="${1:-}"   # safe even if set -u is ever turned on elsewhere

. ./0001-db-create-admin.sh

if [ -n "$DB_NAME" ]; then
  # Single database mode
  . ./0002-db-create-database.sh "$DB_NAME"
  . ./0003-db-create-user.sh "$DB_NAME"
  . ./0004-db-create-schema.sh "$DB_NAME"
  . ./0005-db-create-permissions.sh "$DB_NAME"
  . ./0006-db-create-functions.sh "$DB_NAME"
else
  # Multi-database mode: scripts read BIGHILL_DB_NAMES
  . ./0002-db-create-database.sh
  . ./0003-db-create-user.sh
  . ./0004-db-create-schema.sh
  . ./0005-db-create-permissions.sh
  . ./0006-db-create-functions.sh
fi

cd "$DB_ROOT_DIR"
