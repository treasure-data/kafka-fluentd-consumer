package org.fluentd.kafka.parser;

import java.util.Map;

import kafka.message.MessageAndMetadata;

import org.fluentd.kafka.PropertyConfig;

public abstract class MessageParser {
    protected PropertyConfig config;

    public MessageParser(PropertyConfig config) {
        this.config = config;
    }

    public abstract Map<String, Object> parse(MessageAndMetadata<byte[], byte[]> entry) throws Exception;
}
