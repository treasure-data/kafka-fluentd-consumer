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
    private KafkaStream stream;
    private int threadNumber;
    private FluentLogger logger;
    private ObjectMapper mapper;
    private static TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

    public FluentdHandler(KafkaStream stream, int threadNumber, FluentLogger logger) {
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.logger = logger;

        JsonFactory factory = new JsonFactory();
        mapper = new ObjectMapper(factory);
        //mapper = new ObjectMapper();
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> entry = it.next();

            try {
                HashMap<String, Object> data = mapper.readValue(new String(entry.message()), typeRef);
                //String json = new String(entry.message());
                //Map<String, Object> data = mapper.readValue(json, new TypeReference<HashMap<String, Object>>() {});
                //Map<String, Object> data  =  new HashMap<String, Object>();
                //data.put("message", new String(entry.message()));
                //data.put("message", new String(it.next().message()));
                logger.log(entry.topic(), data);
                //logger.log(it.next().topic(), data);
            } catch (IOException e) {
                Map<String, Object> data  =  new HashMap<String, Object>();
                data.put("message", new String(entry.message()));
                data.put("message", new String(it.next().message()));
                logger.log("failed", data);
            }
        }
        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
