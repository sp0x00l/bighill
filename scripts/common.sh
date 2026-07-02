#!/usr/bin/env bash

# Common functions for service discovery and configuration
# Source this file from other scripts: source "${SCRIPT_DIR}/common.sh"

get_project_root() {
  # Try git first
  if git rev-parse --show-toplevel 2>/dev/null; then
    return
  fi
  
  # Fallback: use GITHUB_WORKSPACE if set (CI environment)
  if [ -n "${GITHUB_WORKSPACE:-}" ]; then
    echo "$GITHUB_WORKSPACE"
    return
  fi
  
  # Fallback: use PROJECT_ROOT if already set
  if [ -n "${PROJECT_ROOT:-}" ]; then
    echo "$PROJECT_ROOT"
    return
  fi
  
  # Last resort: find directory containing Makefile
  local DIR="$PWD"
  while [ "$DIR" != "/" ]; do
    if [ -f "$DIR/Makefile" ] && [ -d "$DIR/scripts" ]; then
      echo "$DIR"
      return
    fi
    DIR=$(dirname "$DIR")
  done
  
  # Default to scripts directory parent (project root when called from /scripts)
  echo "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
}

discover_services() {
  local PROJECT_ROOT="${1:-$(get_project_root)}"
  local SERVICES=()
  
  for DIR in "${PROJECT_ROOT}"/*_service; do
    if [ -d "$DIR" ] && [ -f "$DIR/scripts/config.sh" ]; then
      local SERVICE_DIR_NAME=$(basename "$DIR")
      SERVICES+=("$SERVICE_DIR_NAME")
    fi
  done
  
  echo "${SERVICES[@]}"
}

get_services_list() {
  local PROJECT_ROOT="${1:-$(get_project_root)}"
  discover_services "$PROJECT_ROOT" | tr ' ' '\n'
}

service_dir_to_name() {
  # Convert service_name_service to service-name-service
  local SERVICE_DIR="$1"
  echo "${SERVICE_DIR//_/-}"
}

service_dir_to_env_prefix() {
  # Convert account_service to ACCOUNT_SERVICE
  local SERVICE_DIR="$1"
  local PREFIX="${SERVICE_DIR//-/_}"
  echo "$PREFIX" | tr '[:lower:]' '[:upper:]'
}

service_dir_to_binary() {
  # Convert account_service to account-service (binary name)
  local SERVICE_DIR="$1"
  echo "${SERVICE_DIR//_/-}"
}

first_env_value() {
  local VAR_NAME
  for VAR_NAME in "$@"; do
    if [ -n "${!VAR_NAME:-}" ]; then
      echo "${!VAR_NAME}"
      return 0
    fi
  done
  return 1
}

source_all_configs() {
  local ENVIRONMENT="$1"
  local PROJECT_ROOT="${2:-$(get_project_root)}"
  
  for SERVICE_DIR in $(discover_services "$PROJECT_ROOT"); do
    local CONFIG_FILE="${PROJECT_ROOT}/${SERVICE_DIR}/scripts/config.sh"
    if [ -f "$CONFIG_FILE" ]; then
      # shellcheck disable=SC1090
      . "$CONFIG_FILE" "$ENVIRONMENT"
    fi
  done
}

source_service_config() {
  local SERVICE_DIR="$1"
  local ENVIRONMENT="$2"
  local PROJECT_ROOT="${3:-$(get_project_root)}"
  
  local CONFIG_FILE="${PROJECT_ROOT}/${SERVICE_DIR}/scripts/config.sh"
  if [ -f "$CONFIG_FILE" ]; then
    # shellcheck disable=SC1090
    . "$CONFIG_FILE" "$ENVIRONMENT"
  else
    echo "Warning: Config file not found: ${CONFIG_FILE}" >&2
  fi
}

get_service_ports() {
  local ENVIRONMENT="$1"
  local PROJECT_ROOT="${2:-$(get_project_root)}"
  shift 2
  local SERVICE_DIRS=("$@")

  for SERVICE_DIR in "${SERVICE_DIRS[@]}"; do
    source_service_config "$SERVICE_DIR" "$ENVIRONMENT" "$PROJECT_ROOT"
    local PREFIX
    PREFIX="$(service_dir_to_env_prefix "$SERVICE_DIR")"
    local BASE_PREFIX="${PREFIX%_SERVICE}"
    local HTTP_PORT
    local GRPC_PORT
    local WS_PORT
    local HEALTH_PORT
    HTTP_PORT="$(first_env_value "${PREFIX}_API_HTTP_PORT" "${BASE_PREFIX}_API_HTTP_PORT" "${PREFIX}_HTTP_PORT" "${BASE_PREFIX}_HTTP_PORT" || true)"
    GRPC_PORT="$(first_env_value "${PREFIX}_API_GRPC_PORT" "${BASE_PREFIX}_API_GRPC_PORT" "${PREFIX}_GRPC_PORT" "${BASE_PREFIX}_GRPC_PORT" || true)"
    WS_PORT="$(first_env_value "${PREFIX}_WEBSOCKET_PORT" "${BASE_PREFIX}_WEBSOCKET_PORT" || true)"
    HEALTH_PORT="$(first_env_value "${PREFIX}_HEALTHCHECK_PORT" "${BASE_PREFIX}_HEALTHCHECK_PORT" || true)"
    local SERVICE_NAME
    SERVICE_NAME="$(service_dir_to_name "$SERVICE_DIR")"

    if [ -z "$HTTP_PORT" ] && [ -z "$GRPC_PORT" ] && [ -z "$WS_PORT" ] && [ -z "$HEALTH_PORT" ]; then
      echo "Warning: No ${PREFIX}_API_HTTP_PORT, ${PREFIX}_API_GRPC_PORT, ${PREFIX}_WEBSOCKET_PORT, or ${PREFIX}_HEALTHCHECK_PORT defined for ${SERVICE_DIR}" >&2
      continue
    fi

    if [ -n "$HTTP_PORT" ]; then
      echo "${HTTP_PORT}|${SERVICE_NAME} HTTP"
    fi
    if [ -n "$GRPC_PORT" ]; then
      echo "${GRPC_PORT}|${SERVICE_NAME} gRPC"
    fi
    if [ -n "$WS_PORT" ]; then
      echo "${WS_PORT}|${SERVICE_NAME} WebSocket"
    fi
    if [ -n "$HEALTH_PORT" ]; then
      echo "${HEALTH_PORT}|${SERVICE_NAME} Healthcheck"
    fi
  done
}

wait_for_port() {
  local PORT="$1"
  local NAME="${2:-service}"
  local RETRIES="${3:-30}"
  local DELAY="${4:-2}"

  until nc -z localhost "$PORT" >/dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -le 0 ]; then
      echo "Timeout waiting for $NAME on port $PORT"
      return 1
    fi
    echo "Waiting for $NAME on port $PORT... (${RETRIES} retries left)"
    sleep "$DELAY"
  done
  echo "$NAME is available on port $PORT"
}

check_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker is not installed"
    return 1
  fi
  
  if ! docker info >/dev/null 2>&1; then
    echo "Error: docker daemon is not running"
    return 1
  fi
  
  return 0
}

is_macos() {
  [ "$(uname)" = "Darwin" ]
}

is_linux() {
  [ "$(uname)" = "Linux" ]
}

export_env_configs() {
  local ENVIRONMENT="$1"
  local PROJECT_ROOT="${2:-$(get_project_root)}"
  local CURRENT_DIR="$PWD"

  . "$PROJECT_ROOT/shared_lib/scripts/config.sh" "$ENVIRONMENT"
  . "$PROJECT_ROOT/database/scripts/config.sh" "$ENVIRONMENT"

  for service_dir in $(discover_services "$PROJECT_ROOT"); do
    source_service_config "$service_dir" "$ENVIRONMENT" "$PROJECT_ROOT"
  done

  . "$PROJECT_ROOT/scripts/kafka/kafka-config.sh"

  cd "$CURRENT_DIR"
}

gather_kafka_topics() {
  local PROJECT_ROOT="${1:-$(get_project_root)}"
  local ENV_NAME="${ENVIRONMENT:-local-dev}"

  if [ -f "$PROJECT_ROOT/shared_lib/scripts/config.sh" ]; then
    # shellcheck disable=SC1091
    . "$PROJECT_ROOT/shared_lib/scripts/config.sh" "$ENV_NAME"
  fi

  expand_kafka_topic_template() {
    local TOPIC_TEMPLATE="$1"
    local SHARD_FAMILIES_CSV="$2"

    if [[ "$TOPIC_TEMPLATE" != *"{market_key}"* ]]; then
      printf '%s\n' "$TOPIC_TEMPLATE"
      return 0
    fi

    if [[ -z "$SHARD_FAMILIES_CSV" ]]; then
      return 0
    fi

    local FAMILY
    IFS=',' read -r -a FAMILIES <<< "$SHARD_FAMILIES_CSV"
    for FAMILY in "${FAMILIES[@]}"; do
      FAMILY="$(echo "$FAMILY" | xargs)"
      [[ -z "$FAMILY" ]] && continue
      local EXPANDED="${TOPIC_TEMPLATE//\{market_key\}/$FAMILY}"
      printf '%s\n' "$EXPANDED"
    done
  }

  collect_service_kafka_topics() {
    local SERVICE_DIR="$1"
    local VAR_NAME

    source_service_config "$SERVICE_DIR" "$ENV_NAME" "$PROJECT_ROOT"
    local SHARD_FAMILIES="${CELL_SHARD_MARKET_KEYS:-}"

    while IFS= read -r VAR_NAME; do
      local RAW_VALUE="${!VAR_NAME:-}"
      [[ -z "$RAW_VALUE" ]] && continue

      local ENTRY
      IFS=',' read -r -a ENTRIES <<< "$RAW_VALUE"
      for ENTRY in "${ENTRIES[@]}"; do
        ENTRY="$(echo "$ENTRY" | xargs)"
        [[ -z "$ENTRY" ]] && continue
        expand_kafka_topic_template "$ENTRY" "$SHARD_FAMILIES"
      done
    done < <(compgen -e | grep -E '(SERVICE_TOPIC|SUBSCRIBER_TOPIC|PUBLISHER_TOPIC|COMMAND_TOPIC)$' || true)
  }

  local SERVICE_DIR
  for SERVICE_DIR in $(discover_services "$PROJECT_ROOT"); do
    ( collect_service_kafka_topics "$SERVICE_DIR" )
  done | sed '/^$/d' | sort -u
}

wait_for_kafka_ready() {
  local RETRIES="${1:-30}"
  local DELAY="${2:-2}"

  echo "Waiting for Kafka to be ready..."
  while [ "$RETRIES" -gt 0 ]; do
    # Check Docker-based Kafka first
    if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -q kafka; then
      if docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && \
         docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Kafka is ready"
        return 0
      fi
    # Check native Kafka (macOS Homebrew)
    elif command -v kafka-broker-api-versions.sh >/dev/null 2>&1; then
      if kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && \
         kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Kafka is ready"
        return 0
      fi
    elif command -v kafka-broker-api-versions >/dev/null 2>&1; then
      if kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && \
         kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Kafka is ready"
        return 0
      fi
    fi
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Kafka... (${RETRIES} retries left)"
    sleep "$DELAY"
  done

  echo "Error: Could not confirm Kafka readiness"
  return 1
}

# Wait for instrument service to complete startup publishing
wait_for_instrument_publishing() {
  local RETRIES="${1:-30}"
  local DELAY="${2:-2}"
  local PORT="${3:-8085}"
  
  echo "Waiting for instrument-service to complete startup publishing..."
  while [ "$RETRIES" -gt 0 ]; do
    # Query the instruments API - if it returns active instruments, publishing completed
    local RESPONSE
    RESPONSE=$(curl -s "http://localhost:${PORT}/public/v1/instruments" 2>/dev/null || echo "")

    # Check if we got a valid JSON response with instrument entries.
    # Current API payloads expose "ticker" fields.
    if echo "$RESPONSE" | grep -q '"ticker"'; then
      echo "Instrument service ready - instruments available"
      return 0
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for instrument publishing... (${RETRIES} retries left)"
    sleep "$DELAY"
  done
  
  echo "Warning: Could not confirm instrument publishing, proceeding anyway"
  return 0
}

wait_for_api_gateway_instrument_baseline() {
  local RETRIES="${1:-90}"
  local DELAY="${2:-2}"
  local BASE_URL="${3:-http://127.0.0.1:3000}"

  echo "Waiting for API gateway instrument baseline..."
  while [ "$RETRIES" -gt 0 ]; do
    local RESPONSE
    RESPONSE="$(curl -fsS "${BASE_URL}/public/v1/instruments" 2>/dev/null || echo "")"

    if [ -n "$RESPONSE" ]; then
      local PARSE_OUTPUT
      PARSE_OUTPUT="$(
        INSTRUMENTS_JSON="$RESPONSE" python3 - <<'PY'
import json
import os
import sys

raw = os.environ.get("INSTRUMENTS_JSON", "")
if not raw:
    print("parse_error")
    sys.exit(0)

try:
    body = json.loads(raw)
except Exception:
    print("parse_error")
    sys.exit(0)

resources = []
if isinstance(body, list):
    resources = body
elif isinstance(body, dict):
    for key in ("instruments", "resources"):
        value = body.get(key)
        if isinstance(value, list):
            resources = value
            break

spot = False
perpetual = False
future = False
future_ticker = ""

for item in resources:
    if not isinstance(item, dict):
        continue
    ticker = str(item.get("ticker", ""))
    quote = str(item.get("quoteAsset", item.get("quote_asset", ""))).upper()
    kind = str(item.get("productType", item.get("product_type", ""))).strip().lower()
    if not kind:
        kind = str(item.get("type", "")).strip().lower()
    if not kind:
        kind = str(item.get("kind", "")).strip().lower()

    active = True
    for key in ("isActive", "active", "enabled", "tradable"):
        if key in item:
            value = item.get(key)
            if isinstance(value, bool):
                active = value
            else:
                active = str(value).strip().lower() in ("true", "1")
            break
    if not active:
        continue

    if not kind:
        upper_ticker = ticker.upper()
        if "PERPETUAL" in upper_ticker:
            kind = "perpetual"
        elif "-" in ticker:
            kind = "future"
        else:
            kind = "spot"

    if quote != "USD":
        continue
    if kind == "spot":
        spot = True
    elif kind == "perpetual":
        perpetual = True
    elif kind == "future":
        future = True
        if not future_ticker:
            future_ticker = ticker

print(f"spot={int(spot)}")
print(f"perpetual={int(perpetual)}")
print(f"future={int(future)}")
print(f"future_ticker={future_ticker}")
print(f"catalog={len(resources)}")
PY
      )"

      if [ "$PARSE_OUTPUT" != "parse_error" ]; then
        local SPOT_OK="" PERP_OK="" FUTURE_OK="" FUTURE_TICKER="" CATALOG_COUNT=""
        while IFS='=' read -r key value; do
          case "$key" in
            spot) SPOT_OK="$value" ;;
            perpetual) PERP_OK="$value" ;;
            future) FUTURE_OK="$value" ;;
            future_ticker) FUTURE_TICKER="$value" ;;
            catalog) CATALOG_COUNT="$value" ;;
          esac
        done <<< "$PARSE_OUTPUT"

        if [ "$SPOT_OK" = "1" ] && [ "$PERP_OK" = "1" ] && [ "$FUTURE_OK" = "1" ]; then
          if [ -n "${FUTURE_TICKER:-}" ]; then
            if curl -fsS "${BASE_URL}/public/v1/markets/${FUTURE_TICKER}" >/dev/null 2>&1; then
              echo "API gateway instrument baseline ready"
              return 0
            fi
            echo "Future ${FUTURE_TICKER} not market-ready yet"
          else
            echo "API gateway instrument baseline ready"
            return 0
          fi
        else
          echo "Waiting for API gateway instrument baseline... (spot=${SPOT_OK} perpetual=${PERP_OK} future=${FUTURE_OK} catalog=${CATALOG_COUNT})"
        fi
      fi
    fi

    RETRIES=$((RETRIES - 1))
    sleep "$DELAY"
  done

  echo "Error: API gateway instrument baseline not ready"
  return 1
}

wait_for_api_gateway_ready() {
  local RETRIES="${1:-60}"
  local DELAY="${2:-2}"
  local BASE_URL="${3:-http://127.0.0.1:3000}"

  echo "Waiting for API gateway..."
  while [ "$RETRIES" -gt 0 ]; do
    if curl -fsS -X OPTIONS "${BASE_URL}/public/v1/profiles" >/dev/null 2>&1; then
      echo "API gateway is ready"
      return 0
    fi

    RETRIES=$((RETRIES - 1))
    echo "Waiting for API gateway... (${RETRIES} retries left)"
    sleep "$DELAY"
  done

  echo "Error: API gateway not ready"
  return 1
}

create_kafka_topics() {
  if [ -z "${1:-}" ]; then
    echo "Error: No Kafka broker provided to create_kafka_topics"
    return 1
  fi
  
  local BROKER="$1"
  local PROJECT_ROOT="${2:-$(get_project_root)}"
  local SKIP_WAIT="${3:-false}"
  
  # Wait for Kafka to be fully ready before creating topics
  if [[ "$SKIP_WAIT" != "true" ]]; then
    wait_for_kafka_ready
  fi
  
  local TOPICS
  TOPICS=$(gather_kafka_topics "$PROJECT_ROOT")

  if [[ -z "$TOPICS" ]]; then
    echo "No Kafka topics found to create."
    return 0
  fi

  echo "Creating Kafka topics on broker '${BROKER}':"
  while IFS= read -r TOPIC; do
    [[ -z "$TOPIC" ]] && continue
    echo " - $TOPIC"
    
    # Try docker first, then native kafka-topics.sh
    if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -q kafka; then
      docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
        --topic "$TOPIC" \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server localhost:9092 2>/dev/null || true
    elif command -v kafka-topics.sh >/dev/null 2>&1; then
      kafka-topics.sh --create \
        --topic "$TOPIC" \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server "$BROKER" 2>/dev/null || true
    elif command -v kafka-topics >/dev/null 2>&1; then
      kafka-topics --create \
        --topic "$TOPIC" \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists \
        --bootstrap-server "$BROKER" 2>/dev/null || true
    else
      echo "Warning: No kafka-topics command found" >&2
      return 1
    fi
  done <<< "$TOPICS"
  
  echo "Kafka topics created successfully."
}

purge_kafka_topics() {
  if [ -z "${1:-}" ]; then
    echo "Error: No Kafka broker provided to purge_kafka_topics"
    return 1
  fi
  
  local BROKER="$1"
  local PROJECT_ROOT="${2:-$(get_project_root)}"
  local SKIP_WAIT="${3:-false}"
  
  # Wait for Kafka to be fully ready before purging topics
  if [[ "$SKIP_WAIT" != "true" ]]; then
    wait_for_kafka_ready
  fi
  
  local TOPICS
  TOPICS=$(gather_kafka_topics "$PROJECT_ROOT")

  if [[ -z "$TOPICS" ]]; then
    echo "No Kafka topics found to purge."
    return 0
  fi

  echo "Purging Kafka topics on broker '${BROKER}':"
  while IFS= read -r TOPIC; do
    [[ -z "$TOPIC" ]] && continue
    echo " - $TOPIC"
    
    # Delete the topic to purge all messages
    if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -q kafka; then
      docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete \
        --topic "$TOPIC" \
        --bootstrap-server localhost:9092 2>/dev/null || true
    elif command -v kafka-topics.sh >/dev/null 2>&1; then
      kafka-topics.sh --delete \
        --topic "$TOPIC" \
        --bootstrap-server "$BROKER" 2>/dev/null || true
    elif command -v kafka-topics >/dev/null 2>&1; then
      kafka-topics --delete \
        --topic "$TOPIC" \
        --bootstrap-server "$BROKER" 2>/dev/null || true
    fi
  done <<< "$TOPICS"
  
  # Wait for deletion to propagate
  sleep 2
  
  # Recreate the topics
  create_kafka_topics "$BROKER" "$PROJECT_ROOT" "true"
  
  echo "Kafka topics purged successfully."
}

export -f get_project_root
export -f discover_services
export -f get_services_list
export -f service_dir_to_name
export -f service_dir_to_env_prefix
export -f service_dir_to_binary
export -f source_all_configs
export -f source_service_config
export -f wait_for_port
export -f check_docker
export -f is_macos
export -f is_linux
export -f export_env_configs
export -f gather_kafka_topics
export -f wait_for_kafka_ready
export -f wait_for_instrument_publishing
export -f create_kafka_topics
export -f purge_kafka_topics
