package org.fluentd.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

public class PropertyConfig {
    private final Properties props;

    public PropertyConfig(String propFilePath) throws IOException {
        this.props = loadProperties(propFilePath);
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

    public int getInt(String key) {
        return (int)Long.parseLong(get(key));
    }

    public boolean getBoolean(String key) {
        return Boolean.valueOf(get(key)).booleanValue();
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
