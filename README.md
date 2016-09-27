# Kafka Consumer for Fluentd

This is a simple Java application to consume data from Kafka and forward to Fluentd.

## Build

Use gradle 2.1 or later.

    $ gradle shadowJar

## Run

### Run Kafka

You need to be running Kafka for the consumer to work. To test Kafka locally, please follow the steps described in [Kafka's Quickstart](http://kafka.apache.org/documentation.html#quickstart).

    # start zookeeper
    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    
    # start kafka
    $ bin/kafka-server-start.sh config/server.properties

Then create a topic called `test`

    # create test topic
    $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

Once the 'test' topic is created, add a few messages in it. Make sure message is valid JSON.

    # send multiple messages
    $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test 
    {"a": 1}
    {"a": 1, "b": 2}

You can confirm messages were submitted correctly with this command.

    $ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
    {"a": 1}
    {"a": 1, "b": 2}

### Run Kafka Consumer for Fluentd

Modify `config/fluentd-consumer.properties` with an appropriate configuration. Don't forget to change to `fluentd.consumer.topics=test`. Finally please launch the process like this.

    $ java -Dlog4j.configuration=file:///path/to/log4j.properties -jar build/libs/kafka-fluentd-consumer-0.2.4-all.jar config/fluentd-consumer.properties

This will forward logs to Fluentd (localhost:24224). This consumer uses log4j so you can change logging configurations via `-Dlog4j.configuration` argument.

## Run Kafka Consumer for Fluentd via in_exec

A couple of users has been asking to host consumer as a child process of Fluentd. In that case, 

    <source>
      type forward
    </source>
    
    <source>
      type exec
      command java -Dlog4j.configuration=file:///path/to/log4j.properties -jar /path/to/kafka-fluentd-consumer-0.2.4-all.jar /path/to/config/fluentd-consumer.properties
      tag dummy
      format json
    </source>

## TODO

- Support more format, e.g. msgpack.
- Add metrics features
