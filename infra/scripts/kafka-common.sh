#!/usr/bin/env bash

KAFKA_POD="${KAFKA_POD:-kafka-0}"
KAFKA_PVC="${KAFKA_PVC:-kafka-data-kafka-0}"
KAFKA_READY_TIMEOUT="${KAFKA_READY_TIMEOUT:-120s}"
KAFKA_RESET_TIMEOUT="${KAFKA_RESET_TIMEOUT:-180s}"

kafka_print_diagnostics() {
  local namespace="$1"

  echo "Kafka diagnostics for namespace '${namespace}':" >&2
  kubectl -n "$namespace" get statefulset kafka -o wide >&2 || true
  kubectl -n "$namespace" get pod "$KAFKA_POD" -o wide >&2 || true
  kubectl -n "$namespace" get pvc "$KAFKA_PVC" -o wide >&2 || true
  kubectl -n "$namespace" get endpoints kafka -o wide >&2 || true

  echo "Recent kafka-0 events:" >&2
  kubectl -n "$namespace" get events \
    --field-selector involvedObject.name="$KAFKA_POD" \
    --sort-by=.lastTimestamp >&2 || true

  echo "Recent kafka-0 logs:" >&2
  kubectl -n "$namespace" logs "$KAFKA_POD" --tail=120 >&2 || true

  echo "Previous kafka-0 logs:" >&2
  kubectl -n "$namespace" logs "$KAFKA_POD" --previous --tail=120 >&2 || true
}

kafka_logs_contain_disk_full() {
  local namespace="$1"
  local logs

  logs="$(kubectl -n "$namespace" logs "$KAFKA_POD" --tail=200 2>/dev/null || true)"
  logs="${logs}
$(kubectl -n "$namespace" logs "$KAFKA_POD" --previous --tail=200 2>/dev/null || true)"

  grep -q "No space left on device" <<< "$logs"
}

kafka_reset_storage_on_disk_full() {
  local namespace="$1"
  local environment="$2"

  if [[ "$environment" == "prod" ]]; then
    echo "Error: Kafka is out of disk in prod; refusing to auto-reset storage." >&2
    return 1
  fi

  echo "Kafka is out of disk in non-prod namespace '${namespace}'; auto-resetting PVC '${KAFKA_PVC}'." >&2

  local replicas
  replicas="$(kubectl -n "$namespace" get statefulset kafka -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  if [[ -z "$replicas" || "$replicas" == "0" ]]; then
    replicas="1"
  fi

  echo "Resetting Kafka storage in namespace '${namespace}' by deleting PVC '${KAFKA_PVC}'..."
  kubectl -n "$namespace" scale statefulset/kafka --replicas=0
  kubectl -n "$namespace" wait --for=delete pod/"$KAFKA_POD" --timeout="$KAFKA_RESET_TIMEOUT" || true
  kubectl -n "$namespace" delete pvc "$KAFKA_PVC" --ignore-not-found
  kubectl -n "$namespace" wait --for=delete pvc/"$KAFKA_PVC" --timeout="$KAFKA_RESET_TIMEOUT" || true
  kubectl -n "$namespace" scale statefulset/kafka --replicas="$replicas"
}

kafka_topics_command() {
  local args="$1"
  printf 'if [ -x /opt/bitnami/kafka/bin/kafka-topics.sh ]; then topics_cmd=/opt/bitnami/kafka/bin/kafka-topics.sh; else topics_cmd=/opt/kafka/bin/kafka-topics.sh; fi; "$topics_cmd" %s' "$args"
}

kafka_admin_ready() {
  local namespace="$1"

  kubectl -n "$namespace" exec "$KAFKA_POD" -- sh -c \
    "$(kafka_topics_command "--list --bootstrap-server kafka:9092 >/dev/null")" \
    >/dev/null 2>&1
}

wait_for_kafka_admin_ready() {
  local namespace="$1"
  local attempts="${KAFKA_ADMIN_READY_ATTEMPTS:-20}"

  for _ in $(seq 1 "$attempts"); do
    if kafka_admin_ready "$namespace"; then
      return 0
    fi
    sleep 2
  done

  echo "Error: kafka pod exists but Kafka admin API is not ready." >&2
  kafka_print_diagnostics "$namespace"
  return 1
}

wait_for_kafka_ready() {
  local namespace="$1"
  local environment="${2:-}"

  echo "Waiting for Kafka to be ready in namespace '${namespace}'..."
  if ! kubectl -n "$namespace" get statefulset kafka >/dev/null 2>&1; then
    echo "Error: statefulset kafka not found in namespace '${namespace}'." >&2
    echo "Check that your kubectl context targets the correct cluster:" >&2
    kubectl config current-context >&2 || true
    return 1
  fi

  if kubectl -n "$namespace" rollout status statefulset/kafka --timeout="$KAFKA_READY_TIMEOUT"; then
    if ! kubectl -n "$namespace" get pod "$KAFKA_POD" >/dev/null 2>&1; then
      echo "Error: pod ${KAFKA_POD} not found in namespace '${namespace}' after kafka rollout completed." >&2
      return 1
    fi
    if wait_for_kafka_admin_ready "$namespace"; then
      return 0
    fi
    if [[ -n "$environment" ]] && kafka_logs_contain_disk_full "$namespace"; then
      kafka_reset_storage_on_disk_full "$namespace" "$environment" || return 1
      echo "Waiting for Kafka after storage reset..."
      kubectl -n "$namespace" rollout status statefulset/kafka --timeout="$KAFKA_READY_TIMEOUT" || return 1
      kubectl -n "$namespace" get pod "$KAFKA_POD" >/dev/null || return 1
      wait_for_kafka_admin_ready "$namespace" || return 1
      return 0
    fi
    return 1
  fi

  echo "Error: kafka did not become ready in namespace '${namespace}' within ${KAFKA_READY_TIMEOUT}." >&2
  kafka_print_diagnostics "$namespace"

  if [[ -n "$environment" ]] && kafka_logs_contain_disk_full "$namespace"; then
    kafka_reset_storage_on_disk_full "$namespace" "$environment" || return 1
    echo "Waiting for Kafka after storage reset..."
    kubectl -n "$namespace" rollout status statefulset/kafka --timeout="$KAFKA_READY_TIMEOUT" || return 1
    kubectl -n "$namespace" get pod "$KAFKA_POD" >/dev/null || return 1
    wait_for_kafka_admin_ready "$namespace" || return 1
    return 0
  fi

  return 1
}
