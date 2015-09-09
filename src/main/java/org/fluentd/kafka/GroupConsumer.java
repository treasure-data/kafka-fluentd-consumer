package org.fluentd.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;

import org.fluentd.logger.FluentLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;
 
public class GroupConsumer {
    private static FluentLogger LOG = FluentLogger.getLogger("kafka");

    private final ConsumerConnector consumer;
    private final String topic;
    private final Properties props;
    private  ExecutorService executor;
 
    public GroupConsumer(String propFilePath) throws IOException {
        props = loadConsumerConfig(propFilePath);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = props.getProperty("topic");

        // for testing. Don't use on production
        if (Boolean.valueOf(props.getProperty("from-beginning")))
            ZkUtils.maybeDeletePath(props.getProperty("zookeeper.connect"), "/consumers/" + props.getProperty("group.id"));
    }
 
    public void shutdown() {
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
            executor.submit(new FluentdHandler(stream, threadNumber, LOG));
            threadNumber++;
        }
    }
 
    private static Properties loadConsumerConfig(String propFilePath) throws IOException {
        Properties props = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(propFilePath);
            if (input != null) {
                props.load(input);
            } else {
                throw new FileNotFoundException(propFilePath + "' not found");
            }
        } finally {
            input.close();
        }

        return props;
    }
 
    public static void main(String[] args) throws IOException {
        String propFilePath = args[0];
        int threads = Integer.parseInt(args[1]);
 
        GroupConsumer gc = new GroupConsumer(propFilePath);
        gc.run(threads);
 
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
