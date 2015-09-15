package org.fluentd.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

public class PropertyConfig {
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
        String tag = props.getProperty("fluentd.tag");
        String tagPrefix = props.getProperty("fluentd.tag.prefix");

        if (tag == null && tagPrefix == null)
            throw new RuntimeException("fluentd.tag or fluentd.tag.prefix property is required");
        if (tag != null && tagPrefix != null)
            throw new RuntimeException("can't set fluentd.tag and fluentd.tag.prefix properties at the same time");

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
