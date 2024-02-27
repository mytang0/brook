package xyz.mytang0.brook.spi.cache;

import xyz.mytang0.brook.common.extension.SPI;

@SPI(value = "default")
public interface FlowCacheFactory {

    String DEFAULT_NAME = "default";

    default FlowCache getDefaultCache() {
        return getCache(DEFAULT_NAME);
    }

    FlowCache getCache(String name);
}
