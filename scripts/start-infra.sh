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
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" run --rm --no-deps -T polaris-bootstrap
}

wait_for_tei_health() {
    local TEI_PORT="$1"
    local RETRIES="${2:-30}"
    local DELAY="${3:-1}"

    until curl -fsS --max-time 3 "http://localhost:${TEI_PORT}/health" >/dev/null 2>&1; do
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

resolve_tei_binary() {
    if command -v text-embeddings-router >/dev/null 2>&1; then
        command -v text-embeddings-router
        return
    fi

    if [ -x "$HOME/.cargo/bin/text-embeddings-router" ]; then
        echo "$HOME/.cargo/bin/text-embeddings-router"
        return
    fi

    echo "Error: text-embeddings-router is not installed. Run make install-dev before starting local-dev infra." >&2
    exit 1
}

check_ollama_health() {
    local OLLAMA_PORT="$1"
    local RETRIES="${2:-30}"
    local DELAY="${3:-1}"

    until curl -fsS --max-time 3 "http://localhost:${OLLAMA_PORT}/api/tags" >/dev/null 2>&1 &&
        curl -fsS --max-time 3 "http://localhost:${OLLAMA_PORT}/api/version" >/dev/null 2>&1; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout waiting for Ollama generation endpoint health on port ${OLLAMA_PORT}"
            echo "Start Ollama with: brew services start ollama"
            return 1
        fi
        echo "Waiting for Ollama generation endpoint health on port ${OLLAMA_PORT}... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    echo "Ollama generation endpoint health is available on port ${OLLAMA_PORT}"
}

check_ollama_model_available() {
    local OLLAMA_PORT="$1"
    local MODEL_NAME="$2"
    local RETRIES="${3:-10}"
    local DELAY="${4:-1}"
    local TAGS_PAYLOAD

    if [ -z "$MODEL_NAME" ]; then
        echo "Error: Ollama model name is required"
        return 1
    fi

    until TAGS_PAYLOAD="$(curl -fsS --max-time 3 "http://localhost:${OLLAMA_PORT}/api/tags" 2>/dev/null)" &&
        { printf "%s" "$TAGS_PAYLOAD" | grep -F "\"name\":\"${MODEL_NAME}\"" >/dev/null 2>&1 ||
          printf "%s" "$TAGS_PAYLOAD" | grep -F "\"name\":\"${MODEL_NAME}:latest\"" >/dev/null 2>&1; }; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout waiting for Ollama model ${MODEL_NAME} on port ${OLLAMA_PORT}"
            echo "Provision it with: ollama pull ${MODEL_NAME}"
            return 1
        fi
        echo "Waiting for Ollama model ${MODEL_NAME} on port ${OLLAMA_PORT}... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    echo "Ollama model ${MODEL_NAME} is available on port ${OLLAMA_PORT}"
}

warm_ollama_model() {
    local OLLAMA_PORT="$1"
    local MODEL_NAME="$2"
    local RETRIES="${3:-3}"
    local DELAY="${4:-2}"
    local REQUEST_TIMEOUT="${5:-${FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_REQUEST_TIMEOUT_SECONDS:-120}}"
    local PAYLOAD
    local CURL_ERROR_FILE

    if [ -z "$MODEL_NAME" ]; then
        echo "Error: Ollama model name is required"
        return 1
    fi

    PAYLOAD="$(printf '{"model":"%s","prompt":"Return ok.","stream":false,"options":{"temperature":0,"num_predict":8},"keep_alive":"10m"}' "$MODEL_NAME")"
    CURL_ERROR_FILE="$(mktemp "${TMPDIR:-/tmp}/ollama-warmup.XXXXXX")"
    until curl -4 -fsS --max-time "$REQUEST_TIMEOUT" \
            -H "Content-Type: application/json" \
            -d "$PAYLOAD" \
            "http://127.0.0.1:${OLLAMA_PORT}/api/generate" >/dev/null 2>"$CURL_ERROR_FILE"; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout warming Ollama model ${MODEL_NAME} on port ${OLLAMA_PORT}"
            if [ -s "$CURL_ERROR_FILE" ]; then
                echo "Last Ollama warm-up error:"
                sed 's/^/  /' "$CURL_ERROR_FILE"
            fi
            rm -f "$CURL_ERROR_FILE"
            return 1
        fi
        echo "Waiting for Ollama model ${MODEL_NAME} warm-up on port ${OLLAMA_PORT} with ${REQUEST_TIMEOUT}s timeout... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    rm -f "$CURL_ERROR_FILE"
    echo "Ollama model ${MODEL_NAME} is warmed on port ${OLLAMA_PORT}"
}

check_graph_extraction_model() {
    if [ "${FEATURE_MATERIALIZER_SERVICE_GRAPH_ENABLED:-false}" != "true" ]; then
        return
    fi
    case "${FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_ENDPOINT:-}" in
        http://localhost:11434/*|http://127.0.0.1:11434/*)
            check_ollama_model_available 11434 "$FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL" 10 1
            warm_ollama_model 11434 "$FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL" 3 2 "$FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_REQUEST_TIMEOUT_SECONDS"
            ;;
    esac
}

wait_for_ray_jobs() {
    local RAY_JOBS_URL="$1"
    local RETRIES="${2:-30}"
    local DELAY="${3:-1}"

    until curl -fsS --max-time 3 "${RAY_JOBS_URL%/}/api/version" >/dev/null 2>&1; do
        RETRIES=$((RETRIES - 1))
        if [ "$RETRIES" -le 0 ]; then
            echo "Timeout waiting for Ray Jobs API at ${RAY_JOBS_URL}"
            return 1
        fi
        echo "Waiting for Ray Jobs API at ${RAY_JOBS_URL}... (${RETRIES} retries left)"
        sleep "$DELAY"
    done
    echo "Ray Jobs API is available at ${RAY_JOBS_URL}"
}

ray_jobs_available() {
    local RAY_JOBS_URL="$1"
    curl -fsS --max-time 3 "${RAY_JOBS_URL%/}/api/version" >/dev/null 2>&1
}

resolve_ray_command() {
    if command -v ray >/dev/null 2>&1 && ray --version >/dev/null 2>&1; then
        echo "ray"
        return
    fi

    if command -v pyenv >/dev/null 2>&1; then
        local PYTHON_VERSION="$TRAINING_SERVICE_RAY_PYENV_VERSION"
        if PYENV_VERSION="$PYTHON_VERSION" pyenv exec ray --version >/dev/null 2>&1; then
            echo "pyenv"
            return
        fi
    fi

    echo "Error: Ray is required for local training. Run make install-dev or install ray, then rerun make start-infra." >&2
    exit 1
}

run_ray() {
    local RAY_COMMAND="$1"
    shift

    case "$RAY_COMMAND" in
        ray)
            ray "$@"
            ;;
        pyenv)
            PYENV_VERSION="$TRAINING_SERVICE_RAY_PYENV_VERSION" pyenv exec ray "$@"
            ;;
        *)
            echo "Error: unsupported Ray command runner: $RAY_COMMAND" >&2
            exit 1
            ;;
    esac
}

local_start_ray() {
    if [ "${TRAINING_SERVICE_EXECUTOR_PROVIDER:-}" != "ray" ]; then
        return
    fi

    local RAY_JOBS_URL="$TRAINING_SERVICE_RAY_JOBS_URL"
    local RAY_DASHBOARD_PORT="${RAY_JOBS_URL##*:}"
    RAY_DASHBOARD_PORT="${RAY_DASHBOARD_PORT%%/*}"
    local RAY_HEAD_PORT="$TRAINING_SERVICE_RAY_HEAD_PORT"
    local RAY_COMMAND

    if ray_jobs_available "$RAY_JOBS_URL"; then
        wait_for_ray_jobs "$RAY_JOBS_URL" 10 1
        return
    fi

    case "$RAY_JOBS_URL" in
        http://localhost:*|http://127.0.0.1:*) ;;
        *)
            wait_for_ray_jobs "$RAY_JOBS_URL" 30 1
            return
            ;;
    esac

    RAY_COMMAND="$(resolve_ray_command)"

    if nc -z localhost "$RAY_HEAD_PORT" >/dev/null 2>&1; then
        echo "Ray head port ${RAY_HEAD_PORT} is already in use; checking Jobs API..."
        for _ in 1 2 3; do
            if ray_jobs_available "$RAY_JOBS_URL"; then
                echo "Ray Jobs API is available at ${RAY_JOBS_URL}"
                return
            fi
            sleep 1
        done

        echo "Existing Ray cluster does not expose a healthy Jobs API; restarting local Ray head..."
        run_ray "$RAY_COMMAND" stop --force >/dev/null 2>&1 || true
        sleep 2
    fi

    echo "Starting local Ray head for training jobs..."
    run_ray "$RAY_COMMAND" start --head \
        --port="$RAY_HEAD_PORT" \
        --dashboard-host=127.0.0.1 \
        --dashboard-port="$RAY_DASHBOARD_PORT" \
        --include-dashboard=true \
        --disable-usage-stats >/dev/null

    wait_for_ray_jobs "$RAY_JOBS_URL" 30 1
}

local_start_tei() {
    local TEI_PORT="$FEATURE_MATERIALIZER_SERVICE_EMBEDDING_RUNTIME_PORT"
    local TEI_MODEL_ID="$FEATURE_MATERIALIZER_SERVICE_EMBEDDING_RUNTIME_MODEL_ID"
    local TEI_LOG_FILE="$PROJECT_ROOT/tmp/tei/tei.log"
    local TEI_PID_FILE="$PROJECT_ROOT/tmp/tei/tei.pid"
    local TEI_BINARY

    if [ -z "$TEI_MODEL_ID" ]; then
        echo "Error: FEATURE_MATERIALIZER_SERVICE_EMBEDDING_RUNTIME_MODEL_ID is not set by config."
        exit 1
    fi

    if nc -z localhost "$TEI_PORT" >/dev/null 2>&1; then
        echo "TEI-compatible embedding endpoint is already available on port ${TEI_PORT}"
        wait_for_tei_health "$TEI_PORT" 10 1
        return
    fi

    TEI_BINARY="$(resolve_tei_binary)"

    mkdir -p "$PROJECT_ROOT/tmp/tei"
    echo "Starting local Text Embeddings Inference service..."
    nohup "$TEI_BINARY" \
        --model-id "$TEI_MODEL_ID" \
        --port "$TEI_PORT" \
        > "$TEI_LOG_FILE" 2>&1 &
    echo $! > "$TEI_PID_FILE"

    wait_for_port "$TEI_PORT" "TEI-compatible embedding endpoint" 60 2
    wait_for_tei_health "$TEI_PORT" 60 2
}

compose_start_tei() {
    local COMPOSE_FILE="$1"
    local TEI_PORT="$FEATURE_MATERIALIZER_SERVICE_EMBEDDING_RUNTIME_PORT"

    echo "Starting docker-compose Text Embeddings Inference service..."
    cd "$PROJECT_ROOT"
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" up -d tei-embeddings

    wait_for_port "$TEI_PORT" "TEI-compatible embedding endpoint" 60 2
    wait_for_tei_health "$TEI_PORT" 60 2
}

compose_start_ollama() {
    local COMPOSE_FILE="$1"
    local OLLAMA_PORT="$FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_RUNTIME_PORT"
    local MODEL_NAME="$FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL"

    echo "Starting docker-compose Ollama service..."
    cd "$PROJECT_ROOT"
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" up -d ollama

    wait_for_port "$OLLAMA_PORT" "Ollama"
    check_ollama_health "$OLLAMA_PORT" 60 2

    echo "Pulling docker-compose Ollama graph extraction model ${MODEL_NAME}..."
    env ENVIRONMENT="$ENVIRONMENT" PROJECT_ROOT="$PROJECT_ROOT" docker compose -f "$COMPOSE_FILE" run --rm --no-deps -T ollama-model-pull
    check_ollama_model_available "$OLLAMA_PORT" "$MODEL_NAME" 10 1
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
    local_start_ray
    compose_start_tei "$COMPOSE_FILE"
    compose_start_ollama "$COMPOSE_FILE"
    check_graph_extraction_model
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
    local_start_ray
    local_start_tei
    case "${BIGHILL_START_DATA_SOURCES:-false}" in
        true|1|yes|YES)
            "$PROJECT_ROOT/scripts/start-data-sources.sh"
            ;;
        false|0|no|NO)
            echo "Skipping external datasource fixtures"
            "$PROJECT_ROOT/scripts/stop-data-sources.sh" || true
            ;;
        *)
            echo "Error: BIGHILL_START_DATA_SOURCES must be true or false"
            exit 1
            ;;
    esac
    wait_for_tei_health "$FEATURE_MATERIALIZER_SERVICE_EMBEDDING_RUNTIME_PORT" 10 1
    check_ollama_health 11434 10 1
    check_graph_extraction_model
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
