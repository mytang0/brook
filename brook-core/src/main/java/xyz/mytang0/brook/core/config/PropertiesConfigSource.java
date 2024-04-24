package xyz.mytang0.brook.core.config;

import lombok.extern.slf4j.Slf4j;
import xyz.mytang0.brook.common.annotation.Order;
import xyz.mytang0.brook.spi.config.ConfigSource;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Order(Integer.MIN_VALUE + 1)
public class PropertiesConfigSource implements ConfigSource {

    private static final String CONFIG_FILE_NAME = "brook.properties";

    private volatile Properties properties;

    @Nullable
    @Override
    public Object getProperty(String key) {
        loadProperties();
        return properties.getProperty(key);
    }

    private void loadProperties() {
        if (properties == null) {
            synchronized (this) {
                if (properties == null) {
                    properties = new Properties();
                    try (InputStream inputStream =
                                 ConfigSource.class.getClassLoader()
                                         .getResourceAsStream(CONFIG_FILE_NAME)) {
                        if (inputStream != null) {
                            properties.load(inputStream);
                        }
                    } catch (IOException e) {
                        log.warn("Load " + CONFIG_FILE_NAME + " fail!", e);
                    }
                }
            }
        }
    }
}
