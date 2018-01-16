# Kafka Consumer for Fluentd

This integration is a simple Java application that you can use to consume data from Kafka to Fluentd. You can download the application from this page and then complete the following instructions.

## Build

Use gradle 2.1 or later.

    $ gradle shadowJar

## Run

### Run Kafka

You need to be running Kafka for the consumer to work. To test Kafka locally, follow the steps described in [Kafka's Quickstart](http://kafka.apache.org/documentation.html#quickstart).

    # start zookeeper
    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    
    # start kafka
    $ bin/kafka-server-start.sh config/server.properties

Then create a topic called `test`

    # create test topic
    $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

When the 'test' topic is created, add a few messages in it. Make sure message is valid JSON.

    # send multiple messages
    $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test 
    {"a": 1}
    {"a": 1, "b": 2}

You can confirm messages were submitted correctly with this command.

    $ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
    {"a": 1}
    {"a": 1, "b": 2}

### Run Kafka Consumer for Fluentd

Modify `config/fluentd-consumer.properties` with an appropriate configuration. Remember to change to `fluentd.consumer.topics=test`. Then, launch the process like this.

    $ java -Dlog4j.configuration=file:///path/to/log4j.properties -jar build/libs/kafka-fluentd-consumer-0.3.2-all.jar config/fluentd-consumer.properties

This will forward logs to Fluentd (localhost:24224). This consumer uses log4j so you can change logging configurations via `-Dlog4j.configuration` argument.

## Run Kafka Consumer for Fluentd via in_exec

To host a consumer as a child process of Fluentd, use the following code: 

    <source>
      type forward
    </source>
    
    <source>
      type exec
      command java -Dlog4j.configuration=file:///path/to/log4j.properties -jar /path/to/kafka-fluentd-consumer-0.3.2-all.jar /path/to/config/fluentd-consumer.properties
      tag dummy
      format json
    </source>

## Enterprise Fluentd

Treasure Data offers [Enterprise Fluentd](https://fluentd.treasuredata.com) for those looking for enterprise certified connectors for Kafka among other enterprise features (packaging, security, monitoring and support).

## TODO

- Support more format, e.g. msgpack.
- Add metrics features
