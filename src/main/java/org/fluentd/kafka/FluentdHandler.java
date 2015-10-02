package org.fluentd.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.fluentd.logger.FluentLogger;
import org.fluentd.kafka.parser.MessageParser;
import org.fluentd.kafka.parser.JsonParser;
import org.fluentd.kafka.parser.RegexpParser;

public class FluentdHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FluentdHandler.class);

    private final PropertyConfig config;
    private final FluentdTagger tagger;
    private final KafkaStream stream;
    private final FluentLogger logger;
    private final MessageParser parser;
    private final String timeField;
    private final SimpleDateFormat formatter;

    public FluentdHandler(KafkaStream stream, PropertyConfig config, FluentLogger logger) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = stream;
        this.logger = logger;
        this.parser = setupParser();
        this.timeField = config.get("fluentd.record.time.field", null);
        this.formatter = setupTimeFormatter();
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> entry = it.next();

            try {
                Map<String, Object> data = parser.parse(entry);
                // TODO: Add kafka metadata like metada and topic
                // TODO: Improve performance with batch insert and need to fallback feature to another fluentd instance
                if (timeField == null) {
                    logger.log(tagger.generate(entry.topic()), data);
                } else {
                    long time;
                    try {
                        time = formatter.parse((String)data.get(timeField)).getTime() / 1000;
                    } catch (Exception e) {
                        LOG.warn("failed to parse event time: " + e.getMessage());
                        time = System.currentTimeMillis() / 1000;
                    }
                    logger.log(tagger.generate(entry.topic()), data, time);
                }
            } catch (Exception e) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("message", new String(entry.message()));
                logger.log("failed", data); // should be configurable
            }
        }
    }

    private MessageParser setupParser()
    {
        String format = config.get("fluentd.record.format", "json");
        switch (format) {
        case "json":
            return new JsonParser(config);
        case "regexp":
            return new RegexpParser(config);
        default:
            throw new RuntimeException(format + " format is not supported");
        }
    }

    private SimpleDateFormat setupTimeFormatter() {
        if (timeField == null)
            return null;

        return new SimpleDateFormat(config.get("fluentd.record.time.pattern"));
    }
}
