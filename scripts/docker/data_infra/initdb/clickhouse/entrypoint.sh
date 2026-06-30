#! /usr/bin/env sh

echo "Setting up Wikipedia movies sample database. https://raw.githubusercontent.com/prust/wikipedia-movie-data/master/movies.json"

CLICKHOUSE_USERNAME="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DB:-mlops}"

clickhouse-client --user "$CLICKHOUSE_USERNAME" --password "$CLICKHOUSE_PASSWORD" --query "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DATABASE"

clickhouse-client --user "$CLICKHOUSE_USERNAME" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DATABASE" --query "DROP TABLE IF EXISTS movies"
clickhouse-client --user "$CLICKHOUSE_USERNAME" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DATABASE" --query "
CREATE TABLE movies
(
    title String,
    release_year UInt16,
    cast_members Array(String),
    genres Array(String),
    href Nullable(String),
    extract Nullable(String)
)
ENGINE = MergeTree
ORDER BY (release_year, title)
"

clickhouse-client --user "$CLICKHOUSE_USERNAME" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DATABASE" --query "INSERT INTO movies FORMAT JSONEachRow" < /tmp/movies/movies.jsonl

echo "Sample data imported."
