#! /usr/bin/env bash


KAFKA_DIR=$(brew --prefix)/opt/kafka
TOPIC_NAME="test-topic"
TEST_GROUP="test-group"
KAFKA_BROKER="localhost:9092"
PRODUCER=$KAFKA_DIR/bin/kafka-console-producer
CONSUMER=$KAFKA_DIR/bin/kafka-console-consumer
NUM_MESSAGES=1000
MESSAGE_SIZE=1024
THROUGHPUT_NO_LIMIT=-1
DURATION_MS=1000

mkdir -p ./tmp/kafka-config/
CONFIG_DIR=./tmp/kafka-config


check_messages()
{    
    # parameters are array of strings, read both full array into two variables
    # https://stackoverflow.com/a/51965241
    declare -a OUTPUT=("${!1}")
    declare -a MESSAGES=("${!2}")
    for OUTPUT_LINE in "${OUTPUT[@]}"; do
        FOUND=false
        for MESSAGE in "${MESSAGES[@]}"; do
            if [[ $OUTPUT_LINE == *"$MESSAGE"* ]]; then
                FOUND=true
                break
            fi
        done
        if [ $FOUND == false ]; then
            echo "Failed to find message: $MESSAGE"
            echo "Test failed."
            exit 1
        fi
    done
}


simple_test()
{
    echo "===================================================="
    echo "Starting Kafka simple producer and consumer test."
    echo "Checks the following:"
    echo "-> Producer and consumer can send and receive messages."
    echo "-> No transactions or consumer groups are used."


    MESSAGES_1=("This is Ground Control to Major Tom" "You've really made the grade" "And the papers want to know whose shirts you wear")

    cat <<EOF > $CONFIG_DIR/producer.config
enable.idempotence=true
bootstrap.servers=$KAFKA_BROKER
acks=all
delivery.timeout.ms=120000
transaction.timeout.ms=60000
EOF

    for MESSAGE in "${MESSAGES_1[@]}"; do
        echo "Producing message: $MESSAGE"
        echo $MESSAGE | $PRODUCER --bootstrap-server $KAFKA_BROKER --topic $TOPIC_NAME \
            --producer.config $CONFIG_DIR/producer.config > /dev/null
    done

    sleep 1

    OUTPUT=$($CONSUMER --bootstrap-server $KAFKA_BROKER --topic $TOPIC_NAME \
        --from-beginning --timeout-ms $DURATION_MS)

    check_messages MESSAGES_1[@] OUTPUT[@]
    rm $CONFIG_DIR/producer.config

    echo "Kafka producer and consumer test completed successfully."
}





pref_test()
{
    echo "===================================================="
    echo "Starting Kafka producer and consumer transactional performance test."
    echo "Checks the following:"
    echo "-> Producer and consumer performance."
    echo "-> Producer and consumer throuhput metrics."
    echo "-> Indended for performance investigation."


    local PRODUCER_PREF=$KAFKA_DIR/bin/kafka-producer-perf-test
    local CONSUMER_PREF=$KAFKA_DIR/bin/kafka-consumer-perf-test

    cat <<EOF_1 > $CONFIG_DIR/producer.config
bootstrap.servers=$KAFKA_BROKER
transactional.id=test-transactional-id
acks=all
enable.idempotence=true
EOF_1

    local PRODUCER_OUTPUT=$($PRODUCER_PREF --topic $TOPIC_NAME --num-records $NUM_MESSAGES --record-size $MESSAGE_SIZE --throughput $THROUGHPUT_NO_LIMIT \
        --producer.config $CONFIG_DIR/producer.config --transaction-duration-ms $DURATION_MS --print-metrics)

    cat <<EOF_2 > $CONFIG_DIR/consumer.config
bootstrap.servers=$KAFKA_BROKER
group.id=$TEST_GROUP
enable.auto.commit=false
isolation.level=read_committed
EOF_2

    local CONSUMER_OUTPUT=$($CONSUMER_PREF --broker-list $KAFKA_BROKER --topic $TOPIC_NAME --messages $NUM_MESSAGES \
        --consumer.config $CONFIG_DIR/consumer.config --print-metrics)
    
    rm $CONFIG_DIR/consumer.config $CONFIG_DIR/producer.config

    echo "This is a sample of the metrics, to modify the metrics, change the grep pattern."
    # There may be more important metrics to check.
    echo "TO VIEW ALL METRICS, REMOVE THE GREP COMMAND"
    echo "then add the column name to the grep pattern"

    echo "Producer Metrics:"
    echo "$PRODUCER_OUTPUT" | grep -E "throughput|latency|record-send-rate|record-size"

    echo "Consumer Metrics:"
    echo "$CONSUMER_OUTPUT" | grep -E "throughput|latency|records-consumed|records-per-second|bytes-consumed|fetch-rate"

    echo "Kafka producer and consumer transactional performance test completed."
}




group_test()
{

    echo "===================================================="
    echo "Starting Kafka producer and consumer and consumer group test."
    echo "Checks the following:"
    echo "-> Consumer group is used to consume messages from a topic."
    echo "-> All service instances should use the same group ID."
    echo "-> Delivery guarantees are exactly-once to an instance in the group."

        cat <<EOF_4 > $CONFIG_DIR/producer.config
enable.idempotence=true
bootstrap.servers=$KAFKA_BROKER
acks=all
delivery.timeout.ms=120000
transaction.timeout.ms=60000
EOF_4

    MESSAGES_2=("This is Major Tom to Ground Control" "I'm feeling very still" "And I think my spaceship knows which way to go")
    for MESSAGE in "${MESSAGES_2[@]}"; do 
        echo "Producing message: $MESSAGE"
        echo $MESSAGE | $PRODUCER --bootstrap-server $KAFKA_BROKER --topic $TOPIC_NAME \
            --producer.config $CONFIG_DIR/producer.config > /dev/null
    done

    PIPE=$(mktemp -u)
    mkfifo $PIPE

    cat <<EOF_5 > $CONFIG_DIR/consumer.config
bootstrap.servers=$KAFKA_BROKER
group.id=$TEST_GROUP
enable.auto.commit=true
auto.commit.interval.ms=1000
isolation.level=read_committed
EOF_5

    # Consumer group-id is only created now, implicitly by the consumer but lazily and not explicitly.
    $CONSUMER --bootstrap-server $KAFKA_BROKER --topic $TOPIC_NAME --group $TEST_GROUP \
        --consumer.config $CONFIG_DIR/consumer.config \
        --from-beginning --timeout-ms 10000 > $PIPE &
    CONSUMER_PID=$!

    sleep 2 # wait for consumer to start and offset.commit.interval to pass
    
    # Read the output from the named pipe into an array
    OUTPUT=()
    while IFS= read -r line; do
        echo "read: $line"
        OUTPUT+=("$line")
    done < $PIPE &

    exec 3>&-
    wait $CONSUMER_PID 3>&- 2>/dev/null 
    rm -f $PIPE

    rm $CONFIG_DIR/producer.config $CONFIG_DIR/consumer.config
    check_messages MESSAGES_2[@] OUTPUT[@]

    RETRIES=3
    CONSUMER_GROUPS=$KAFKA_DIR/bin/kafka-consumer-groups

    while [ $RETRIES -gt 0 ]; do
        echo "Verifying consumer group $TEST_GROUP... ($RETRIES retries left)"

        RESULT=$($CONSUMER_GROUPS --group $TEST_GROUP --describe --bootstrap-server $KAFKA_BROKER)

        if [[ $RESULT == *"Error"* ]]; then
            if [[ $RETRIES -eq 0 ]]; then
                echo "Failed to verify consumer group $TEST_GROUP."
                echo "Error: $RESULT"
                echo "Test failed."
                exit 1
            else
                RETRIES=$((RETRIES-1))
                sleep 3
                continue
            fi
        fi
        
        FAILED=false
        # Out put is random rows and then the table with the group information.
        # In result find the table beginning with "GROUP" using awk, 
        # move data row wich is on the next line and assign it to TABLE
        TABLE=$(echo "$RESULT" | awk '/GROUP/ {getline; print}')

        GROUP_RESULT=$(echo $TABLE | awk '{print $1}')
        if [[ $GROUP_RESULT == $TEST_GROUP ]]; then
            echo "Group $TEST_GROUP found."
        else
            echo "Group $TEST_GROUP not found."
            FAILED=true
        fi

        TOPIC_RESULT=$(echo $TABLE | awk '{print $2}')
        if [[ $TOPIC_RESULT == $TOPIC_NAME ]]; then
            echo "Topic $TOPIC_NAME found."
        else
            echo "Topic $TOPIC_NAME not found."
            FAILED=true
        fi

        # Current offset is the number of messages consumed by the consumer group
        CURRENT_OFFSET=$(echo $TABLE | awk '{print $4}')
        if [[ $CURRENT_OFFSET -eq 3 ]]; then
            echo "Current offset is 3."
        else
            echo "Current offset is not 3. Offset: $CURRENT_OFFSET."
            INDEX=0
            for OUTPUT_LINE in "${OUTPUT[@]}"; do
                echo "OUTPUT $INDEX: $OUTPUT_LINE"
                INDEX=$((INDEX+1))
            done
            echo "total events: $INDEX"
            FAILED=true
        fi

        if [ $FAILED == false ]; then
            echo "Consumer group $TEST_GROUP verified."
            break
        else
            echo "Error: $RESULT"
            echo "Test failed."
            exit 1
        fi
    done

    kill $CONSUMER_PID 2> /dev/null

    echo "Kafka producer and consumer with consumer group test completed."
    echo "===================================================="

}

purge_test_data()
{
    $KAFKA_DIR/bin/kafka-topics --delete --topic $TOPIC_NAME --bootstrap-server $KAFKA_BROKER 2> /dev/null
    sleep 5
    $KAFKA_DIR/bin/kafka-topics --create --topic $TOPIC_NAME --bootstrap-server $KAFKA_BROKER --partitions 1 --replication-factor 1 2> /dev/null
}


if [ "$1" == "simple" ]; then
    purge_test_data
    simple_test 
elif [ "$1" == "pref" ]; then
    purge_test_data
    pref_test
elif [ "$1" == "group" ]; then
    purge_test_data
    group_test
else
    echo "running all tests"
    purge_test_data
    simple_test
    purge_test_data
    pref_test
    purge_test_data
    group_test
fi
purge_test_data

if [ -d $CONFIG_DIR ]; then
    rm -rf $CONFIG_DIR
fi
