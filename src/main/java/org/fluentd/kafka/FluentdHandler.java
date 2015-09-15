package org.fluentd.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.Map;
import org.fluentd.logger.FluentLogger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import java.io.IOException;

public class FluentdHandler implements Runnable {
    private final PropertyConfig config;
    private final FluentdTagger tagger;
    private KafkaStream stream;
    private FluentLogger logger;
    private ObjectMapper mapper;
    private static TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

    public FluentdHandler(KafkaStream stream, PropertyConfig config, FluentLogger logger) {
        this.config = config;
        this.tagger = config.getTagger();
        this.stream = stream;
        this.logger = logger;

        JsonFactory factory = new JsonFactory();
        mapper = new ObjectMapper(factory);
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> entry = it.next();

            try {
                HashMap<String, Object> data = mapper.readValue(new String(entry.message()), typeRef);
                // TODO: Add kafka metadata like metada and topic
                // TODO: Improve performance with batch insert and need to fallback feature to another fluentd instance
                logger.log(tagger.generate(entry.topic()), data);
            } catch (IOException e) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("message", new String(entry.message()));
                logger.log("failed", data); // should be configurable
            }
        }
        System.out.println("Shutting down Thread: " + config.get("fluentd.consumer.threads"));
    }
}
