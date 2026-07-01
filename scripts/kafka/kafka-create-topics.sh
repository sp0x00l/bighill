#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/kafka-common.sh"
# shellcheck disable=SC1091
. "${PROJECT_ROOT}/scripts/common.sh"

ENVIRONMENT="${1:-local-dev}"
echo "ENV: $ENVIRONMENT"

kafka_load_config "$ENVIRONMENT"
kafka_start_homebrew_if_needed

BROKER="$(kafka_bootstrap_server)"

if [ "$ENVIRONMENT" = "local-dev" ] || [ "$ENVIRONMENT" = "cicd" ]; then
  BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR=1
  kafka_topics --bootstrap-server "$BROKER" --create --if-not-exists --topic __transaction_state --replication-factor "$BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR" --config cleanup.policy=compact >/dev/null 2>&1 || true
else
  echo "PROD: creating __transaction_state --partitions count of 50."
  BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR=3
  kafka_topics --bootstrap-server "$BROKER" --create --if-not-exists --topic __transaction_state --partitions 50 --replication-factor "$BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR" --config cleanup.policy=compact >/dev/null 2>&1 || true
fi

create_kafka_topics "$BROKER" "$PROJECT_ROOT" "true"
