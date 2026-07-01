#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/kafka-common.sh"

ENVIRONMENT="${1:-local-dev}"
kafka_load_config "$ENVIRONMENT"
kafka_start_homebrew_if_needed

BROKER="$(kafka_bootstrap_server)"
topics="$(kafka_topics --bootstrap-server "$BROKER" --list)"
if [ -z "$topics" ]; then
  echo "no topics to check"
  exit 0
fi

consumerGroups="$(kafka_consumer_groups --bootstrap-server "$BROKER" --list)"
for topic in $topics; do
  if [ "$topic" = "__consumer_offsets" ]; then
    continue
  fi

  for group in $consumerGroups; do
    kafka_consumer_groups --bootstrap-server "$BROKER" --describe --group "$group"

    output="$(kafka_consumer_groups --bootstrap-server "$BROKER" --describe --group "$group" 2>/dev/null)"
    topic_output="$(echo "$output" | awk -v topic="$topic" '$2 == topic')"

    if [ -n "$topic_output" ]; then
      echo "$topic_output" | while read -r line; do
        lag="$(echo "$line" | awk '{print $6}')"

        echo ""
        echo "-----------------------------"
        echo "Consumer group: $group"
        echo "Topic: $topic"

        if [ "$lag" = "-" ]; then
          echo "No outstanding messages."
        elif [ "$lag" -gt 0 ]; then
          echo "Outstanding messages: $lag"
          echo "$output" | head -n 2
          echo "$output" | awk -v topic="$topic" '$2 == topic'
        fi
        echo "-----------------------------"
      done
    fi
  done
done

echo "done"
