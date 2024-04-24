package xyz.mytang0.brook.core.config;

import xyz.mytang0.brook.common.annotation.Order;
import xyz.mytang0.brook.spi.config.ConfigSource;

import javax.annotation.Nullable;

@Order(Integer.MIN_VALUE)
public class EnvConfigSource implements ConfigSource {

    @Nullable
    @Override
    public Object getProperty(String key) {
        Object value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key);
        }
        return value;
    }
}
