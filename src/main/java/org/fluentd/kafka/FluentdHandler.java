package org.fluentd.kafka;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
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
import org.komamitsu.fluency.BufferFullException;
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
    private final int batchSize;
    private final ExecutorService executor;

    public FluentdHandler(ConsumerConnector consumer, KafkaStream stream, PropertyConfig config, Fluency logger, ExecutorService executor) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = stream;
        this.logger = logger;
        this.parser = setupParser();
        this.executor = executor;
        this.consumer = consumer;
        this.timeField = config.get("fluentd.record.time.field", null);
        this.formatter = setupTimeFormatter();
        this.batchSize = config.getInt(PropertyConfig.Constants.FLUENTD_CONSUMER_BATCH_SIZE.key, PropertyConfig.Constants.DEFAULT_BATCH_SIZE);
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int numEvents = 0;
        Exception ex = null;

        while (!executor.isShutdown()) {
            while (hasNext(it)) {
                MessageAndMetadata<byte[], byte[]> entry = it.next();
                String tag = tagger.generate(entry.topic());
                Map<String, Object> data = null;
                long time = 0; // 0 means use logger's automatic time generation

                try {
                    data = parser.parse(entry);
                    // TODO: Add kafka metadata like metada and topic
                    if (timeField != null) {
                        try {
                            time = formatter.parse((String)data.get(timeField)).getTime() / 1000;
                        } catch (Exception e) {
                            LOG.warn("failed to parse event time. Use current time: " + e.getMessage());
                        }
                    }
                    emitEvent(tag, data, time);
                } catch (BufferFullException bfe) {
                    LOG.error("fluentd logger reached buffer full. Wait 1 second for retry", bfe);

                    while (true) {
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException ie) {
                            LOG.warn("Interrupted during sleep");
                            Thread.currentThread().interrupt();
                        }

                        try {
                            emitEvent(tag, data, time);
                            LOG.info("Retry emit succeeded. Buffer full is resolved");
                            break;
                        } catch (IOException e) {}

                        LOG.error("fluentd logger is still buffer full. Wait 1 second for next retry");
                    }
                } catch (IOException e) {
                    ex = e;
                } catch (Exception e) {
                    Map<String, Object> failedData = new HashMap<String, Object>();
                    failedData.put("message", new String(entry.message(), StandardCharsets.UTF_8));
                    try {
                        emitEvent("failed", failedData, 0);
                    } catch (IOException e2) {
                        ex = e2;
                    }
                }

                numEvents++;
                if (numEvents > batchSize) {
                    consumer.commitOffsets();
                    numEvents = 0;
                    break;
                }

                if (ex != null) {
                    LOG.error("can't send logs to fluentd. Wait 1 second", ex);
                    ex = null;
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ie) {
                        LOG.warn("Interrupted during sleep");
                        Thread.currentThread().interrupt();
                    }
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
            LOG.debug("Consumption was timed out", e);
            return false;
        }
    }

    private SimpleDateFormat setupTimeFormatter() {
        if (timeField == null)
            return null;

        return new SimpleDateFormat(config.get("fluentd.record.time.pattern"));
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
