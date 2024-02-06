package org.mytang.brook.spi.cache;

import org.mytang.brook.common.extension.SPI;

@SPI(value = "default")
public interface FlowCache {

    void put(Object key, Object value);

    Object get(Object key);
}
