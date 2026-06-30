# Kafka CLI Tools Provided

## Kafka Cluster State Check

`cmd/cluster_state.go`

Checks the Kafka cluster state; it connects to the Kafka cluster and retrieves metadata to check the cluster's state. It prints the Controller ID and Brokeers Hosts.

## Kafka Broker State

`cmd/broker-connectivity.go`

Prints the brokers list and the connectivity state of each broker. 

## Kafka Topics List

`cmd/topics_list.go`

Lists all topics in the cluster. For every topic it lists the partitions, their replicas and replica `In Sync` state.

### Expected Topics

We use a pub/sub pattern.

Topics are defined in the shared_lib messaging package.

The script `kafka-create-topics.sh` in the directory `./..` is used to setup transactions tropics. The `__transaction_state` topic is the transactional record delivery for "Exactly Once Semantics" (EOS). The default settings for the number of partitions and replication factor is 1 for `local-dev` but 50 for partitions and 3 for replication factor in `prod`.

With transactions we may treat the entire distributed consume-transform-produce process single atomic transaction; is only `read_committed` if all the steps succeed.

The consumer config includes `isolation.level=read_committed` which creates the  `__transaction_state` topic. This is set in the subscriber_client.go via the consumer kafka config.

In a transaction where we successfully go through multiple distribued steps, the transaction coordinator will add a commit marker to the internal `__transaction_state` topic and each of the topic partitions involved in the transaction, including the `__consumer_offsets` topic. This will inform downstream consumers, who are set to read_committed that this data is consumable.

When a consumer with `isolation.level` of `read_committed` fetches data from the broker, it receives events in offset order (as usual) but it only receives those events with an offset lower than the last stable offset (LSO). The leader maintains the LSO.

Kafka internal topics

Kafka consumers store the last consumed message offset id in kafka topic `__consumer_offsets` based on the consumer group id. This enables different consumers in a group to process the next message after the last consumed message and avoid duplicate message processing. Every consumer group maintains its offset per topic partitions.

The local-dev and prod environments have a defualt `__consumer_offsets` topic created at install time with 50 partitions. These are used to stores consumer state. The default settings for the number of partitions and replication factor.

```bash
offsets.topic.num.partitions=50
offsets.topic.replication.factor=3
```

## Kafka Consumer Lag

`cmd/consumer_lag.go`

The consuemr lag tool calculates the difference between the last committed offset and the latest offset for each partition. This is useful to monitor the progress of a consumer group and detect potential issues when committing offsets.

1. Gets the latest offsets (end offsets) for each partition of a topic.
2. Gets the committed offsets for the specified consumer group.
3. Calculate the lag by subtracting the committed offsets from the end offsets.

## Kafka Producer Flush

`cmd/producer-flush.go`

The Kafka producer flush tool produces messages on a Kafka topic and tests flush behavior. If you provide param `size` it will create messages of a given size. Other then in will send the message index as the value.

The producer flush supports transactional messaging with a consumer.

## Kafka Metrics

`cmd/producer_metrics.go`

The Kafka metrics tool samples the Kafka Stats. It follows the same structure as the producer-flush tool but prints associated stats. The number of messages it processes is configurable.

In particular it prints EOS stats.
