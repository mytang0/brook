package xyz.mytang0.brook.spi.cache;

import xyz.mytang0.brook.common.extension.SPI;

@SPI(value = "default")
public interface FlowCache {

    void put(Object key, Object value);

    Object get(Object key);
}
