#!/usr/bin/env bash
set -euo pipefail

start_data_sources()
{
    local SCRIPT_DIR
    local PROJECT_ROOT
    local CURRENT_DIR
    local COMPOSE_FILE
    local SERVICES

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/common.sh"
    PROJECT_ROOT="$(get_project_root)"
    CURRENT_DIR="$(pwd)"
    COMPOSE_FILE="$PROJECT_ROOT/docker-compose-data.yml"
    SERVICES=("$@")

    if [ "${#SERVICES[@]}" -eq 0 ]; then
        SERVICES=(postgres-data-source mysql-data-source mongodb-data-source clickhouse-data-source)
    fi

    if ! check_docker; then
        exit 1
    fi

    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" up -d "${SERVICES[@]}"

    for SERVICE in "${SERVICES[@]}"; do
        case "$SERVICE" in
            postgres-data-source) wait_for_port 5435 "Postgres data source" 60 2 ;;
            mysql-data-source)    wait_for_port 3306 "MySQL data source" 60 2 ;;
            mongodb-data-source)  wait_for_port 27017 "MongoDB data source" 60 2 ;;
            clickhouse-data-source)
                wait_for_port 19000 "ClickHouse data source" 60 2
                wait_for_clickhouse_data_source
                ;;
            oracle-data-source)   wait_for_port 1521 "Oracle data source" 120 5 ;;
            minio-data-source)
                wait_for_port 9000 "MinIO data source API" 60 2
                wait_for_port 9001 "MinIO data source console" 60 2
                ;;
            nessie-data-source)   wait_for_port 19120 "Nessie data source" 60 2 ;;
        esac
    done

    cd "$CURRENT_DIR"
}

wait_for_clickhouse_data_source()
{
    local RETRIES
    local DELAY

    RETRIES=60
    DELAY=2

    until docker exec clickhouse-data-source clickhouse-client --user user --password password --database mlops --query "SELECT count() FROM movies" >/dev/null 2>&1; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout waiting for ClickHouse fixture data"
            return 1
        fi
        echo "Waiting for ClickHouse fixture data... (${RETRIES} retries left)"
        sleep "$DELAY"
    done

    echo "ClickHouse fixture data is ready"
}

start_data_sources "$@"
