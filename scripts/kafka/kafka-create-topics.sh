#! /usr/bin/env sh

export ENV=$1
echo "ENV: $ENV"

KAFKA_SCRIPT_DIR=$(pwd)

if ! brew services list | grep -q kafka.plist; then
    echo "kafka is not running, starting kafka"
    brew services start kafka
    sleep 5
fi

topicsCmd=$(brew --prefix)/bin/kafka-topics
broker=$(cat $(brew --prefix)/etc/kafka/kraft/broker.properties | grep "advertised.listeners" | cut -d':' -f2- | cut -d'/' -f3)

if [[ "$ENV" == "local-dev" ]]; then
    export BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR=1
    $topicsCmd --bootstrap-server $broker --create --topic __transaction_state --replication-factor $BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR --config cleanup.policy=compact
else
    # production partions count of 50
    # The higher the partitions the more the throughput
    echo "PROD: creating __transaction_state --partitions count of 50 (1 is the defualt value for partitions in the server.properties)."
    export BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR=3
    $topicsCmd --bootstrap-server $broker --create --topic __transaction_state --partitions 50 --replication-factor $BIGHILL_KAFKA_TRANSACTION_REPLICATION_FACTOR --config cleanup.policy=compact
fi
