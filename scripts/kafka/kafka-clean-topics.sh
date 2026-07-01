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
  echo "no topics to delete"
else
  for topic in $topics; do
    if [ "$topic" = "__consumer_offsets" ]; then
      continue
    fi
    echo "deleting topic $topic"
    kafka_topics --bootstrap-server "$BROKER" --delete --if-exists --topic "$topic" >/dev/null 2>&1 || true
  done
fi
