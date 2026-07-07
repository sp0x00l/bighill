#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
. "${PROJECT_ROOT}/scripts/common.sh"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/kafka-common.sh"

if [ -z "${1:-}" ]; then
  echo "Error: No environment provided."
  echo "Usage: './scripts/kafka-purge.sh [local-dev|cicd|staging|prod]'"
  exit 1
fi

ENVIRONMENT="$1"
NAMESPACE="ml-ops-${ENVIRONMENT}"

purge_k8s_topics() {
  local namespace="$1"
  local topics="$2"

  if [[ -z "$topics" ]]; then
    echo "No Kafka topics found to purge."
    return 0
  fi

  wait_for_kafka_ready "$namespace" "$ENVIRONMENT"

  echo "Purging Kafka topics in namespace '${namespace}':"
  while IFS= read -r topic; do
    [[ -z "$topic" ]] && continue
    echo " - $topic"
    local DEL_OUT DEL_RC
    DEL_OUT=$(kubectl -n "$namespace" exec "$KAFKA_POD" -- sh -c \
      "$(kafka_topics_command "--delete \
        --topic '$topic' \
        --bootstrap-server kafka:9092")" 2>&1) && DEL_RC=0 || DEL_RC=$?
    if [[ "$DEL_RC" -ne 0 ]]; then
      # UnknownTopicOrPartitionException is benign (already absent)
      if echo "$DEL_OUT" | grep -qE "UnknownTopicOrPartitionException|does not exist"; then
        echo "   (already absent)"
      else
        echo "Error: failed to delete topic '${topic}' in namespace '${namespace}':" >&2
        echo "$DEL_OUT" >&2
        return 1
      fi
    fi
  done <<< "$topics"
}

wait_for_topic_deletions() {
  local namespace="$1"
  local topics="$2"

  while IFS= read -r topic; do
    [[ -z "$topic" ]] && continue
    for _ in $(seq 1 30); do
      if ! kubectl -n "$namespace" exec "$KAFKA_POD" -- sh -c \
        "$(kafka_topics_command "--list --bootstrap-server kafka:9092 | grep -Fx '$topic'")" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
  done <<< "$topics"
}

cd "$PROJECT_ROOT"
TOPICS="$(gather_kafka_topics "$PROJECT_ROOT")"

purge_k8s_topics "$NAMESPACE" "$TOPICS"
wait_for_topic_deletions "$NAMESPACE" "$TOPICS"
"${SCRIPT_DIR}/k8s-create-kafka-topics.sh" "$ENVIRONMENT"

echo "Kafka topics purged successfully in namespace '${NAMESPACE}'."
