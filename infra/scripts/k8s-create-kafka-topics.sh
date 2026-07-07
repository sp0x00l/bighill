#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source common functions
# shellcheck disable=SC1091
. "${PROJECT_ROOT}/scripts/common.sh"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/kafka-common.sh"

create_k8s_topics() {
  local NAMESPACE="$1"
  local TOPICS="${2:-}"
  local ENVIRONMENT="${3:-}"

  if [[ -z "$TOPICS" ]]; then
    echo "No Kafka topics found. Nothing to create."
    return 0
  fi

  wait_for_kafka_ready "$NAMESPACE" "$ENVIRONMENT"

  echo "Creating topics in namespace '${NAMESPACE}':"
  while IFS= read -r TOPIC; do
    [[ -z "$TOPIC" ]] && continue
    echo " - $TOPIC"
    local CREATE_OUT CREATE_RC
    CREATE_OUT=$(kubectl -n "$NAMESPACE" exec "$KAFKA_POD" -- sh -c \
      "$(kafka_topics_command "--create \
        --topic '$TOPIC' \
        --partitions 3 \
        --replication-factor 1 \
        --bootstrap-server kafka:9092")" 2>&1) && CREATE_RC=0 || CREATE_RC=$?
    if [[ "$CREATE_RC" -ne 0 ]]; then
      # TopicExistsException is benign (idempotent re-run); anything else is fatal
      if echo "$CREATE_OUT" | grep -q "TopicExistsException"; then
        echo "   (already exists)"
      else
        echo "Error: failed to create topic '${TOPIC}' in namespace '${NAMESPACE}':" >&2
        echo "$CREATE_OUT" >&2
        return 1
      fi
    fi
  done <<< "$TOPICS"

  echo "Verifying topics in namespace '${NAMESPACE}'..."
  local LISTED MISSING ATTEMPT
  for ATTEMPT in $(seq 1 30); do
    LISTED=$(kubectl -n "$NAMESPACE" exec "$KAFKA_POD" -- sh -c \
      "$(kafka_topics_command "--list --bootstrap-server kafka:9092")" 2>&1) || {
      echo "Error: failed to list topics for verification:" >&2
      echo "$LISTED" >&2
      return 1
    }
    MISSING=()
    while IFS= read -r TOPIC; do
      [[ -z "$TOPIC" ]] && continue
      if ! grep -Fxq "$TOPIC" <<< "$LISTED"; then
        MISSING+=("$TOPIC")
      fi
    done <<< "$TOPICS"
    if [[ "${#MISSING[@]}" -eq 0 ]]; then
      echo "All topics present."
      return 0
    fi
    sleep 2
  done

  echo "Error: ${#MISSING[@]} topic(s) missing after create:" >&2
  printf ' - %s\n' "${MISSING[@]}" >&2
  return 1
}

k8s_create_kafka_topics() {
  local ENVIRONMENT="${1:-staging}"
  local NAMESPACE="ml-ops-${ENVIRONMENT}"

  cd "$PROJECT_ROOT"

  local TOPICS
  TOPICS=$(gather_kafka_topics "$PROJECT_ROOT")
  create_k8s_topics "$NAMESPACE" "$TOPICS" "$ENVIRONMENT"
}

k8s_create_kafka_topics "$@"
