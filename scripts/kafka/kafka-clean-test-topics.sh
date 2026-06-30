#! /usr/bin/env sh

if ! brew services list | grep -q kafka.plist; then
    echo "kafka is not running, starting kafka"
    brew services start kafka
    sleep 5
fi

echo "Cleaning kafka test topics, this will purge logs and data"
topicsCmd=$(brew --prefix)/bin/kafka-topics
broker=$(cat $(brew --prefix)/etc/kafka/kraft/broker.properties | grep "advertised.listeners" | cut -d':' -f2- | cut -d'/' -f3)
topics=$($topicsCmd --bootstrap-server $broker --list)
if [ -z "$topics" ]; then
    echo "no topics to delete"
else
   for topic in $topics; do
        if [[ "$topic" == test-topic* ]]; then
            echo "deleting topic $topic"
            $topicsCmd --bootstrap-server $broker --delete --topic $topic
        fi
    done
fi

echo "Cleaning complete"
