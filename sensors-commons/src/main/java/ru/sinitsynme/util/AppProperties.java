package ru.sinitsynme.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppProperties {
    private static final String PROPERTIES_FILE_NAME = "application.properties";
    private static Properties properties;
    private static AppProperties instance;

    private AppProperties() {
        properties = new Properties();
        try (InputStream input = AppProperties.class.getClassLoader()
                .getResourceAsStream(PROPERTIES_FILE_NAME)) {

            if (input == null) {
                throw new RuntimeException(String.format("Unable to find %s", PROPERTIES_FILE_NAME));
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file", e);
        }
    }

    public static synchronized AppProperties getInstance() {
        if (instance == null) {
            instance = new AppProperties();
        }
        return instance;
    }


    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public Double getDoubleProperty(String key) {
        return Double.parseDouble(getProperty(key));
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}