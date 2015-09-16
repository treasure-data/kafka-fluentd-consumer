package org.fluentd.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

public class PropertyConfig {
    public enum Constants {
        FLUENTD_TAG("fluentd.tag"),
        FLUENTD_TAG_PREFIX("fluentd.tag.prefix"),
        FLUENTD_CONSUMER_TOPICS("fluentd.consumer.topics"),
        FLUENTD_CONSUMER_THREADS("fluentd.consumer.threads"),
        FLUENTD_CONSUMER_FROM_BEGINNING("fluentd.consumer.from.beginning"),
        KAFKA_ZOOKEEPER_CONNECT("zookeeper.connect"),
        KAFKA_GROUP_ID("group.id");

        public final String key;

        Constants(String key) {
            this.key = key;
        }
    }

    private final Properties props;
    private final FluentdTagger tagger;

    public PropertyConfig(String propFilePath) throws IOException {
        props = loadProperties(propFilePath);
        tagger = setupTagger();
    }

    public Properties getProperties() {
        return props;
    }

    public String get(String key) {
        String value = props.getProperty(key);
        if (value == null)
            throw new RuntimeException(key + " parameter not found in the configuration");
        return value;
    }

    public String get(String key, String defaultValue) {
        String value = props.getProperty(key);
        if (value == null)
            return defaultValue;
        return value;
    }

    public int getInt(String key) {
        return (int)Long.parseLong(get(key));
    }

    public int getInt(String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null)
            return defaultValue;
        return (int)Long.parseLong(get(key));
    }

    public boolean getBoolean(String key) {
        return Boolean.valueOf(get(key)).booleanValue();
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (value == null)
            return defaultValue;
        return Boolean.valueOf(value).booleanValue();
    }

    public FluentdTagger getTagger() {
        return tagger;
    }

    private FluentdTagger setupTagger() {
        String tag = props.getProperty(Constants.FLUENTD_TAG.key);
        String tagPrefix = props.getProperty(Constants.FLUENTD_TAG_PREFIX.key);

        if (tag == null && tagPrefix == null)
            throw new RuntimeException(Constants.FLUENTD_TAG.key + " or " + Constants.FLUENTD_TAG_PREFIX.key + "property is required");
        if (tag != null && tagPrefix != null)
            throw new RuntimeException("can't set " + Constants.FLUENTD_TAG.key + " and " + Constants.FLUENTD_TAG_PREFIX.key + " properties at the same time");

        return new FluentdTagger(tag, tagPrefix);
    }

    private Properties loadProperties(String propFilePath) throws IOException {
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
}
