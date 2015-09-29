package org.fluentd.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.fluentd.logger.FluentLogger;
import org.fluentd.kafka.parser.MessageParser;
import org.fluentd.kafka.parser.JsonParser;

public class FluentdHandler implements Runnable {
    private final PropertyConfig config;
    private final FluentdTagger tagger;
    private final KafkaStream stream;
    private final FluentLogger logger;
    private final MessageParser parser;

    public FluentdHandler(KafkaStream stream, PropertyConfig config, FluentLogger logger) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = stream;
        this.logger = logger;
        this.parser = new JsonParser(config);
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> entry = it.next();

            try {
                Map<String, Object> data = parser.parse(entry);
                // TODO: Add kafka metadata like metada and topic
                // TODO: Improve performance with batch insert and need to fallback feature to another fluentd instance
                logger.log(tagger.generate(entry.topic()), data);
            } catch (Exception e) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("message", new String(entry.message()));
                logger.log("failed", data); // should be configurable
            }
        }
    }
}
