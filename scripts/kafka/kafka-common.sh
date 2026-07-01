#!/usr/bin/env bash

kafka_project_root() {
  git rev-parse --show-toplevel 2>/dev/null || cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

kafka_load_config() {
  local ENVIRONMENT="${1:-local-dev}"
  local PROJECT_ROOT
  PROJECT_ROOT="$(kafka_project_root)"

  if [ -f "${PROJECT_ROOT}/shared_lib/scripts/config.sh" ]; then
    # shellcheck disable=SC1090
    . "${PROJECT_ROOT}/shared_lib/scripts/config.sh" "$ENVIRONMENT"
  fi
}

kafka_has_docker_container() {
  command -v docker >/dev/null 2>&1 &&
    docker ps --format '{{.Names}}' 2>/dev/null | grep -Eq '^kafka$'
}

kafka_start_homebrew_if_needed() {
  if kafka_has_docker_container; then
    return 0
  fi
  if ! command -v brew >/dev/null 2>&1; then
    return 0
  fi
  if ! brew services list | grep -E "kafka.*started" >/dev/null 2>&1; then
    echo "kafka is not running, starting kafka"
    brew services start kafka
    sleep 5
  fi
}

kafka_bootstrap_server() {
  if kafka_has_docker_container; then
    echo "localhost:9092"
    return
  fi
  echo "${KAFKA_BROKER:-localhost:9092}"
}

kafka_topics() {
  if kafka_has_docker_container; then
    docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh "$@"
    return
  fi
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    kafka-topics.sh "$@"
    return
  fi
  if command -v kafka-topics >/dev/null 2>&1; then
    kafka-topics "$@"
    return
  fi
  if command -v brew >/dev/null 2>&1 && [ -x "$(brew --prefix)/bin/kafka-topics" ]; then
    "$(brew --prefix)/bin/kafka-topics" "$@"
    return
  fi
  echo "Error: kafka-topics command not found" >&2
  return 1
}

kafka_consumer_groups() {
  if kafka_has_docker_container; then
    docker exec kafka /opt/bitnami/kafka/bin/kafka-consumer-groups.sh "$@"
    return
  fi
  if command -v kafka-consumer-groups.sh >/dev/null 2>&1; then
    kafka-consumer-groups.sh "$@"
    return
  fi
  if command -v kafka-consumer-groups >/dev/null 2>&1; then
    kafka-consumer-groups "$@"
    return
  fi
  if command -v brew >/dev/null 2>&1 && [ -x "$(brew --prefix)/bin/kafka-consumer-groups" ]; then
    "$(brew --prefix)/bin/kafka-consumer-groups" "$@"
    return
  fi
  echo "Error: kafka-consumer-groups command not found" >&2
  return 1
}
