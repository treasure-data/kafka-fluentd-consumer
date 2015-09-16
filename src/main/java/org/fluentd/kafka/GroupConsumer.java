package org.fluentd.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;

import org.fluentd.logger.FluentLogger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 
public class GroupConsumer {
    private static FluentLogger LOG = FluentLogger.getLogger("");

    private final ConsumerConnector consumer;
    private final String topic;
    private final PropertyConfig config;
    private  ExecutorService executor;
 
    public GroupConsumer(PropertyConfig config) throws IOException {
        this.config = config;
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(config.getProperties()));
        this.topic = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS.key);

        // for testing. Don't use on production
        if (config.getBoolean(PropertyConfig.Constants.FLUENTD_CONSUMER_FROM_BEGINNING.key, false))
            ZkUtils.maybeDeletePath(config.get(PropertyConfig.Constants.KAFKA_ZOOKEEPER_CONNECT.key), "/consumers/" + config.get(PropertyConfig.Constants.KAFKA_GROUP_ID.key));
    }
 
    public void shutdown() {
        System.out.println("Shutting down consumers");

        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }

        LOG.close();
   }
 
    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        executor = Executors.newFixedThreadPool(numThreads);
 
        // now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new FluentdHandler(stream, config, LOG));
            threadNumber++;
        }
    }
 
    public static void main(String[] args) throws IOException {
        PropertyConfig pc = new PropertyConfig(args[0]);
        GroupConsumer gc = new GroupConsumer(pc);
        gc.run(pc.getInt(PropertyConfig.Constants.FLUENTD_CONSUMER_THREADS.key));
 
        try {
            // Need better long running approach.
            while (true) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        gc.shutdown();
    }
}
