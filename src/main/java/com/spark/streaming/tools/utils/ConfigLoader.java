package com.spark.streaming.tools.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class ConfigLoader implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    public static StreamingConfig getDefaultConfigs() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is;
        is = classloader.getResourceAsStream("streaming.yml");
        StreamingConfig config = (StreamingConfig) new Yaml(new Constructor(StreamingConfig.class))
                .load(is);
        is.close();
        return config;
    }
}