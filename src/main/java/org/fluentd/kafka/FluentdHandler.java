package org.fluentd.kafka;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.komamitsu.fluency.Fluency;
import org.fluentd.kafka.parser.MessageParser;
import org.fluentd.kafka.parser.JsonParser;
import org.fluentd.kafka.parser.RegexpParser;

public class FluentdHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FluentdHandler.class);

    private final PropertyConfig config;
    private final FluentdTagger tagger;
    private final ConsumerConnector consumer;
    private final KafkaStream stream;
    private final Fluency logger;
    private final MessageParser parser;
    private final String timeField;
    private final SimpleDateFormat formatter;

    public FluentdHandler(ConsumerConnector consumer, KafkaStream stream, PropertyConfig config, Fluency logger) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = stream;
        this.logger = logger;
        this.parser = setupParser();
        this.consumer = consumer;
        this.timeField = config.get("fluentd.record.time.field", null);
        this.formatter = setupTimeFormatter();
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int numEvents = 0;

        while (!Thread.interrupted()) {
            while (hasNext(it)) {
                MessageAndMetadata<byte[], byte[]> entry = it.next();

                try {
                    try {
                        Map<String, Object> data = parser.parse(entry);
                        // TODO: Add kafka metadata like metada and topic
                        // TODO: Improve performance with batch insert and need to fallback feature to another fluentd instance
                        if (timeField == null) {
                            emitEvent(tagger.generate(entry.topic()), data);
                        } else {
                            long time;
                            try {
                                time = formatter.parse((String)data.get(timeField)).getTime() / 1000;
                            } catch (Exception e) {
                                LOG.warn("failed to parse event time: " + e.getMessage());
                                time = System.currentTimeMillis() / 1000;
                            }
                            emitEvent(tagger.generate(entry.topic()), data, time);
                        }
                    } catch (IOException e) {
                        throw e;
                    } catch (Exception e) {
                        Map<String, Object> data = new HashMap<String, Object>();
                        data.put("message", new String(entry.message(), StandardCharsets.UTF_8));
                        emitEvent("failed", data);
                    }
                    numEvents++;
                } catch (IOException e) {
                    LOG.error("can't send a log to fluentd. Wait 1 second", e);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ie) {
                        LOG.warn("Interrupted during sleep");
                        Thread.currentThread().interrupt();
                    }
                }

                if (numEvents > 500) {
                    consumer.commitOffsets();
                    numEvents = 0;
                }
            }
        }

        if (numEvents > 0) {
            consumer.commitOffsets();
            numEvents = 0;
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

    private boolean hasNext(ConsumerIterator<byte[], byte[]> it) {
        try {
            it.hasNext();
            return true;
        } catch (ConsumerTimeoutException e) {
            return false;
        }
    }

    private SimpleDateFormat setupTimeFormatter() {
        if (timeField == null)
            return null;

        return new SimpleDateFormat(config.get("fluentd.record.time.pattern"));
    }

    private void emitEvent(String tag, Map<String, Object> data) throws IOException {
        emitEvent(tag, data, 0);
    }

    private void emitEvent(String tag, Map<String, Object> data, long timestamp) throws IOException {
        try {
            if (timestamp == 0)
                logger.emit(tag, data);
            else
                logger.emit(tag, timestamp, data);
        } catch (IllegalArgumentException e) { // MessagePack can't serialize BigInteger larger than 2^64 - 1 so convert it to String
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof BigInteger)
                    entry.setValue(value.toString());
            }

            if (timestamp == 0)
                logger.emit(tag, data);
            else
                logger.emit(tag, timestamp, data);
        }
    }
}
