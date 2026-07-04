#! /usr/bin/env sh

go build -v -o build/kafka-cli -tags debug
# ./build/kafka-cli cluster-state --brokers localhost:9092
# ./build/kafka-cli broker-connectivity --brokers localhost:9092
# ./build/kafka-cli topics-list 
./build/kafka-cli consumer-lag --brokers localhost:9092 --group ingestions-group --topics datasets

# # ./build/kafka-cli producer-flush --brokers localhost:9092 --topic test_topic --count 10 --size 0
# ./build/kafka-cli producer-flush --brokers localhost:9092 --topic test_topic --count 10 --size 0 --producer-config "acks=all,retries=3"

# ./build/kafka-cli topic-partitions --brokers localhost:9092 --topic test_topic
# ./build/kafka-cli producer-metrics --brokers localhost:9092 --producer-config "acks=all,retries=3"

