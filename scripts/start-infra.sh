#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
export PROJECT_ROOT="$(get_project_root)"

set -eu

CURRENT_DIR=$(pwd)

if [ -z "${1:-}" ]; then
    echo "Error: No environment provided."
    echo "Usage: './start-infra.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export ENVIRONMENT="$1"

# Export environment variables from config scripts
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/export-env.sh" "$ENVIRONMENT"

local_restart_db() {
    local DB_SCRIPTS_DIR="$PROJECT_ROOT/database/scripts"
    
    echo "Starting database..."
    cd "$PROJECT_ROOT/database"
    . "$DB_SCRIPTS_DIR/db-stop.sh"
    . "$DB_SCRIPTS_DIR/db-delete.sh"
    . "$DB_SCRIPTS_DIR/db-install.sh"
    . "$DB_SCRIPTS_DIR/db-start.sh"
    (cd "$PROJECT_ROOT/database" && . "$DB_SCRIPTS_DIR/setup/db-common-init.sh")
    . "$DB_SCRIPTS_DIR/db-migrate.sh"
}

local_restart_redis() {
    echo "Starting Redis..."
    if command -v brew >/dev/null 2>&1; then
        brew services stop redis >/dev/null 2>&1 || true
        brew services start redis
    elif command -v redis-server >/dev/null 2>&1; then
        if ! nc -z localhost 6379 >/dev/null 2>&1; then
            redis-server --daemonize yes
        fi
    else
        echo "Error: Redis is not installed. Install Redis before running local-dev infra."
        exit 1
    fi

    wait_for_port 6379 "Redis"
}

local_start_kafka() {
    local KAFKA_WAIT_TIME=15
    
    if [ -z "${KAFKA_BROKER+x}" ] || [ -z "$KAFKA_BROKER" ]; then
        echo "Error: KAFKA_BROKER is not set"
        exit 1
    fi

    if ! command -v brew >/dev/null 2>&1; then
        echo "Error: local Kafka startup currently expects Homebrew Kafka. Start Kafka manually or use ENV=cicd."
        exit 1
    fi

    if ! brew services list | grep -E "kafka.*started" >/dev/null 2>&1; then
        echo "Starting Kafka..."
        brew services start kafka
        sleep "${KAFKA_WAIT_TIME}s"
    fi

    wait_for_kafka_ready 60 2
    
    # Purge stale messages from previous test runs
    purge_kafka_topics "$KAFKA_BROKER" "$PROJECT_ROOT" "true"
}

local_start_temporal() {
    local TEMPORAL_GRPC_PORT="${TEMPORAL_PORT:-7233}"
    local TEMPORAL_WEB_PORT="${TEMPORAL_UI_PORT:-8233}"
    local TEMPORAL_DB_FILE="$PROJECT_ROOT/tmp/temporal/temporal.db"
    local TEMPORAL_LOG_FILE="$PROJECT_ROOT/tmp/temporal/temporal.log"
    local TEMPORAL_PID_FILE="$PROJECT_ROOT/tmp/temporal/temporal.pid"
    local TEMPORAL_NAMESPACE_NAME="${TEMPORAL_NAMESPACE:-default}"

    if nc -z localhost "$TEMPORAL_GRPC_PORT" >/dev/null 2>&1; then
        echo "Temporal is already available on port ${TEMPORAL_GRPC_PORT}"
        return
    fi

    if ! command -v temporal >/dev/null 2>&1; then
        echo "Error: Temporal CLI is not installed. Run make install-dev before starting local-dev infra."
        exit 1
    fi

    if command -v brew >/dev/null 2>&1 &&
        [ "$TEMPORAL_GRPC_PORT" = "7233" ] &&
        [ "$TEMPORAL_WEB_PORT" = "8233" ] &&
        [ "$TEMPORAL_NAMESPACE_NAME" = "default" ]; then
        echo "Starting Temporal dev server..."
        brew services start temporal
        wait_for_port "$TEMPORAL_GRPC_PORT" "Temporal"
        wait_for_port "$TEMPORAL_WEB_PORT" "Temporal UI"
        return
    fi

    mkdir -p "$PROJECT_ROOT/tmp/temporal"
    echo "Starting Temporal dev server..."
    nohup temporal server start-dev \
        --port "$TEMPORAL_GRPC_PORT" \
        --ui-port "$TEMPORAL_WEB_PORT" \
        --namespace "$TEMPORAL_NAMESPACE_NAME" \
        --db-filename "$TEMPORAL_DB_FILE" \
        > "$TEMPORAL_LOG_FILE" 2>&1 &
    echo $! > "$TEMPORAL_PID_FILE"

    wait_for_port "$TEMPORAL_GRPC_PORT" "Temporal"
    wait_for_port "$TEMPORAL_WEB_PORT" "Temporal UI"
}

compose_wait_for_migrations() {
    local COMPOSE_FILE="$1"
    local RETRIES=60
    local DELAY=2
    
    echo "Waiting for database migrations to complete..."
    while [ "$RETRIES" -gt 0 ]; do
        if env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" \
            docker compose -f "$COMPOSE_FILE" logs migrations 2>/dev/null | \
            grep -q "All migrations completed successfully"; then
            echo "Database migrations completed successfully"
            return 0
        fi
        
        if env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" \
            docker compose -f "$COMPOSE_FILE" ps migrations 2>/dev/null | \
            grep -Eqi 'exited.*\(0\)|exit 0|completed'; then
            echo "Database migrations completed (container exited with success)"
            return 0
        fi
        
        RETRIES=$((RETRIES - 1))
        echo "Waiting for migrations... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    
    echo "Error: Could not confirm migration completion"
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" \
        docker compose -f "$COMPOSE_FILE" ps migrations 2>/dev/null || true
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" \
        docker compose -f "$COMPOSE_FILE" logs --tail=200 migrations 2>/dev/null || true
    return 1
}

compose_wait_for_postgres_ready() {
    local COMPOSE_FILE="$1"
    local RETRIES=30
    local DELAY=2

    echo "Waiting for Postgres query readiness..."
    while [ "$RETRIES" -gt 0 ]; do
        if env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" \
            docker compose -f "$COMPOSE_FILE" exec -T bighilldb \
            pg_isready -U "$BIGHILL_DB_ADMIN" >/dev/null 2>&1; then
            echo "Postgres is query-ready"
            return 0
        fi
        RETRIES=$((RETRIES - 1))
        echo "Waiting for Postgres query readiness... (${RETRIES} retries left)"
        sleep "$DELAY"
    done

    echo "Error: Postgres did not become query-ready in time"
    return 1
}

compose_start_polaris() {
    local COMPOSE_FILE="$1"

    if ! check_docker; then
        echo "Docker is not available; skipping Polaris catalog infra."
        return
    fi

    echo "Starting Polaris catalog infra..."
    cd "$PROJECT_ROOT"
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" up -d polaris-catalog

    wait_for_port 9100 "Polaris object store API" 60 2
    wait_for_port 9101 "Polaris object store console" 60 2
    wait_for_port 8181 "Polaris catalog" 60 2
    wait_for_port 8182 "Polaris health" 60 2
    compose_bootstrap_polaris "$COMPOSE_FILE"
}

compose_bootstrap_polaris() {
    local COMPOSE_FILE="$1"

    if ! check_docker; then
        echo "Docker is not available; skipping Polaris bootstrap."
        return
    fi

    echo "Bootstrapping Polaris catalog..."
    cd "$PROJECT_ROOT"
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" up polaris-bootstrap
}

wait_for_tei_health() {
    local TEI_PORT="$1"
    local RETRIES="${2:-30}"
    local DELAY="${3:-1}"

    until curl -fsS "http://localhost:${TEI_PORT}/health" >/dev/null 2>&1; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout waiting for TEI-compatible embedding endpoint health on port ${TEI_PORT}"
            return 1
        fi
        echo "Waiting for TEI-compatible embedding endpoint health on port ${TEI_PORT}... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    echo "TEI-compatible embedding endpoint health is available on port ${TEI_PORT}"
}

wait_for_ollama_health() {
    local OLLAMA_PORT="$1"
    local RETRIES="${2:-30}"
    local DELAY="${3:-1}"

    until curl -fsS "http://localhost:${OLLAMA_PORT}/api/tags" >/dev/null 2>&1; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout waiting for Ollama-compatible generation endpoint health on port ${OLLAMA_PORT}"
            return 1
        fi
        echo "Waiting for Ollama-compatible generation endpoint health on port ${OLLAMA_PORT}... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    echo "Ollama-compatible generation endpoint health is available on port ${OLLAMA_PORT}"
}

local_start_tei() {
    local TEI_PORT=8080
    local TEI_DIR="$PROJECT_ROOT/tmp/tei-embeddings"
    local TEI_PID_FILE="$TEI_DIR/tei-embeddings.pid"
    local TEI_LOG_FILE="$TEI_DIR/tei-embeddings.log"
    local TEI_SCRIPT="$PROJECT_ROOT/scripts/docker/services/tei_stub.py"
    local PYTHON_BIN="${PYTHON:-python3}"

    if nc -z localhost "$TEI_PORT" >/dev/null 2>&1; then
        echo "TEI-compatible embedding endpoint is already available on port ${TEI_PORT}"
        wait_for_tei_health "$TEI_PORT" 10 1
        return
    fi

    if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
        if command -v python >/dev/null 2>&1; then
            PYTHON_BIN=python
        else
            echo "Error: python3 is required to start the local TEI-compatible embedding endpoint."
            exit 1
        fi
    fi

    mkdir -p "$TEI_DIR"
    rm -f "$TEI_PID_FILE"
    echo "Starting local TEI-compatible embedding endpoint..."
    "$PYTHON_BIN" "$TEI_SCRIPT" --daemonize --pid-file "$TEI_PID_FILE" --log-file "$TEI_LOG_FILE"

    wait_for_port "$TEI_PORT" "TEI-compatible embedding endpoint" 60 2
    wait_for_tei_health "$TEI_PORT" 10 1
}

local_start_ollama() {
    local OLLAMA_PORT=11434
    local OLLAMA_DIR="$PROJECT_ROOT/tmp/ollama-generation"
    local OLLAMA_PID_FILE="$OLLAMA_DIR/ollama-generation.pid"
    local OLLAMA_LOG_FILE="$OLLAMA_DIR/ollama-generation.log"
    local OLLAMA_SCRIPT="$PROJECT_ROOT/scripts/docker/services/ollama_stub.py"
    local PYTHON_BIN="${PYTHON:-python3}"

    if nc -z localhost "$OLLAMA_PORT" >/dev/null 2>&1; then
        echo "Ollama-compatible generation endpoint is already available on port ${OLLAMA_PORT}"
        wait_for_ollama_health "$OLLAMA_PORT" 10 1
        return
    fi

    if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
        if command -v python >/dev/null 2>&1; then
            PYTHON_BIN=python
        else
            echo "Error: python3 is required to start the local Ollama-compatible generation endpoint."
            exit 1
        fi
    fi

    mkdir -p "$OLLAMA_DIR"
    rm -f "$OLLAMA_PID_FILE"
    echo "Starting local Ollama-compatible generation endpoint..."
    "$PYTHON_BIN" "$OLLAMA_SCRIPT" --port "$OLLAMA_PORT" --daemonize --pid-file "$OLLAMA_PID_FILE" --log-file "$OLLAMA_LOG_FILE"

    wait_for_port "$OLLAMA_PORT" "Ollama-compatible generation endpoint" 60 2
    wait_for_ollama_health "$OLLAMA_PORT" 10 1
}

cicd_start_infra() {
    local COMPOSE_FILE="$PROJECT_ROOT/docker-compose-services.yml"
    
    if [ -z "${KAFKA_BROKER+x}" ] || [ -z "$KAFKA_BROKER" ]; then
        echo "Error: KAFKA_BROKER is not set"
        exit 1
    fi

    if ! check_docker; then
        exit 1
    fi

    cd "$PROJECT_ROOT"
    echo "Starting infra using docker compose (Postgres with pgvector, Redis, Kafka, Temporal, Polaris)..."
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" up -d bighilldb redis kafka temporal migrations polaris-catalog

    wait_for_port 5432 "Postgres"
    wait_for_port 6379 "Redis"
    wait_for_port 9092 "Kafka"
    wait_for_port 7233 "Temporal"
    wait_for_port 8233 "Temporal UI"
    wait_for_port 9100 "Polaris object store API"
    wait_for_port 9101 "Polaris object store console"
    wait_for_port 8181 "Polaris catalog"
    wait_for_port 8182 "Polaris health"
    local_start_tei
    local_start_ollama
    compose_bootstrap_polaris "$COMPOSE_FILE"

    compose_wait_for_postgres_ready "$COMPOSE_FILE"
    wait_for_kafka_ready 60 2
    compose_wait_for_migrations "$COMPOSE_FILE"

    # Purge stale messages from previous test runs
    purge_kafka_topics "$KAFKA_BROKER" "$PROJECT_ROOT" "true"
    wait_for_kafka_ready 60 2
}

local_start_infra() {
    local_restart_db
    local_restart_redis
    local_start_kafka
    local_start_temporal
    compose_start_polaris "$PROJECT_ROOT/docker-compose-services.yml"
    local_start_tei
    local_start_ollama
    "$PROJECT_ROOT/scripts/start-data-sources.sh"
    wait_for_tei_health 8080 10 1
    wait_for_ollama_health 11434 10 1
}

case "$ENVIRONMENT" in
    local-dev) local_start_infra ;;
    cicd)      cicd_start_infra ;;
    *)
        echo "Error: start-infra only manages local-dev and cicd infra."
        exit 1
        ;;
esac


cd "$CURRENT_DIR"
sleep 2
