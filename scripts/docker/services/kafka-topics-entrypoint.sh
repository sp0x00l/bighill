#! /usr/bin/env sh

if [ -z "$BIGHILL_TOPIC_NAMES" ]; then
    echo "BIGHILL_TOPIC_NAMES is not set"
    exit
fi
echo "creating kafka topics: $BIGHILL_TOPIC_NAMES"

topic_names=($BIGHILL_TOPIC_NAMES)
topicsCmd="/opt/bitnami/kafka/bin/kafka-topics.sh"
broker="localhost:${KAFKA_LISTENERS_PORT}"

for topic in "${topic_names[@]}"; do
    echo "creating topic $topic"
    $topicsCmd --bootstrap-server $broker --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
done

echo "kafka topics created successfully!"
