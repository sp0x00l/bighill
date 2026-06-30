#! /usr/bin/env sh

if ! brew services list | grep -q kafka.plist; then
    echo "kafka is not running, starting kafka"
    brew services start kafka
    sleep 5
fi

topicsCmd=$(brew --prefix)/bin/kafka-topics
broker=$(cat $(brew --prefix)/etc/kafka/kraft/broker.properties | grep "advertised.listeners" | cut -d':' -f2- | cut -d'/' -f3)
topics=$($topicsCmd --bootstrap-server $broker --list)
if [ -z "$topics" ]; then
    echo "no topics to check"
    exit 0
fi

consumerGroupCmd=$(brew --prefix)/bin/kafka-consumer-groups

for topic in $topics
do
    if [ "$topic" = "__consumer_offsets" ]; then
        continue
    fi
    consumerGroups=$($consumerGroupCmd --bootstrap-server $broker --list)

    for group in $consumerGroups
    do
        # uncomment to see all the consumer group details
        $consumerGroupCmd --bootstrap-server $broker --describe --group $group

        output=$($consumerGroupCmd --bootstrap-server $broker --describe --group $group 2>/dev/null)
        topic_output=$(echo "$output" | awk -v topic="$topic" '$2 == topic')

        if [[ ! -z "$topic_output" ]]; then
            echo "$topic_output" | while read -r line ; do
                lag=$(echo $line | awk '{print $6}')

                echo ""
                echo "-----------------------------"
                echo "Consumer group: $group"
                echo "Topic: $topic"

                if [[ "$lag" == "-" ]]; then
                    echo "No outstanding messages."
                elif [[ "$lag" -gt 0 ]]; then
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