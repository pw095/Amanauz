package org.meta;

import java.util.HashMap;
import java.util.Map;

public class Properties {
    private static Properties instance;
    private Map<String, String> properties;

    private Properties() {
    }

    public static synchronized Properties getInstance() {
        if (instance == null) {
            instance = new Properties();
        }
        return instance;
    }

    public synchronized void setProperties(Map<String, String> properties) {
            this.properties = new HashMap<>(properties);
    }

    public Map<String, String> getProperties() {
        return new HashMap<String, String>(this.properties);
    }
    public String getProperty(String key) {
        return this.properties.get(key);
    }

}
