package org.streaming.util;

import java.io.*;
import java.util.*;

/**
 * Configuration loader for the Twitter Trend Momentum processor
 */
public class ConfigLoader {
    private static final String CONFIG_FILE = "src/main/resources/application.properties";
    private static Properties properties;

    static {
        properties = new Properties();
        try (InputStream input = new FileInputStream(CONFIG_FILE)) {
            properties.load(input);
        } catch (IOException ex) {
            System.err.println("Error loading configuration from " + CONFIG_FILE);
            ex.printStackTrace();
        }
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static int getPropertyAsInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                System.err.println("Invalid integer value for property: " + key);
            }
        }
        return defaultValue;
    }

    public static long getPropertyAsLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                System.err.println("Invalid long value for property: " + key);
            }
        }
        return defaultValue;
    }

    public static void printConfiguration() {
        System.out.println("=== Configuration ===");
        properties.forEach((key, value) -> System.out.println(key + " = " + value));
    }
}
