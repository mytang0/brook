package org.mytang.brook.spi.cache;

import org.mytang.brook.common.extension.SPI;

@SPI(value = "default")
public interface FlowCacheFactory {

    String DEFAULT_NAME = "default";

    default FlowCache getDefaultCache() {
        return getCache(DEFAULT_NAME);
    }

    FlowCache getCache(String name);
}
