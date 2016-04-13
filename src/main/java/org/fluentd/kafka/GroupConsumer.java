package org.fluentd.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.consumer.Blacklist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZkUtils;

import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GroupConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(GroupConsumer.class);

    private final ConsumerConnector consumer;
    private final String topic;
    private final PropertyConfig config;
    private ExecutorService executor;
    private final Fluency fluentLogger;

    public GroupConsumer(PropertyConfig config) throws IOException {
        this.config = config;
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(config.getProperties()));
        this.topic = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS.key);
        this.fluentLogger = setupFluentdLogger();

        // for testing. Don't use on production
        if (config.getBoolean(PropertyConfig.Constants.FLUENTD_CONSUMER_FROM_BEGINNING.key, false))
            ZkUtils.maybeDeletePath(config.get(PropertyConfig.Constants.KAFKA_ZOOKEEPER_CONNECT.key), "/consumers/" + config.get(PropertyConfig.Constants.KAFKA_GROUP_ID.key));
    }

    public Fluency setupFluentdLogger() throws IOException {
        return Fluency.defaultFluency(config.getFluentdConnect());
    }
 
    public void shutdown() {
        LOG.info("Shutting down consumers");

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted during shutdown, exiting uncleanly");
                executor.shutdownNow();
            }
        }
        if (consumer != null) consumer.shutdown();

        try {
            fluentLogger.close();
        } catch (IOException e) {
            LOG.error("failed to close fluentd logger completely", e);
        }
   }
 
    public void run() {
        int numThreads = config.getInt(PropertyConfig.Constants.FLUENTD_CONSUMER_THREADS.key);
        List<KafkaStream<byte[], byte[]>> streams = setupKafkaStream(numThreads);

        // now create an object to consume the messages
        executor = Executors.newFixedThreadPool(numThreads);
        for (final KafkaStream stream : streams) {
            executor.submit(new FluentdHandler(consumer, stream, config, fluentLogger));
        }
    }

    public List<KafkaStream<byte[], byte[]>> setupKafkaStream(int numThreads) {
        String topics = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS.key);
        String topicsPattern = config.get(PropertyConfig.Constants.FLUENTD_CONSUMER_TOPICS_PATTERN.key, "whitelist");
        TopicFilter topicFilter;

        switch (topicsPattern) {
        case "whitelist":
            topicFilter = new Whitelist(topics);
            break;
        case "blacklist":
            topicFilter = new Blacklist(topics);
            break;
        default:
            throw new RuntimeException("'" + topicsPattern + "' topics pattern is not supported");
        }

        return consumer.createMessageStreamsByFilter(topicFilter, numThreads);
    }
 
    public static void main(String[] args) throws IOException {
        final PropertyConfig pc = new PropertyConfig(args[0]);
        final GroupConsumer gc = new GroupConsumer(pc);

        gc.run();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    gc.shutdown();
                }
            }));

        try {
            // Need better long running approach.
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            LOG.error("Something happen!", e);
        }
    }
}
