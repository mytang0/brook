package xyz.mytang0.brook.spi.config;

import xyz.mytang0.brook.common.extension.SPI;

import javax.annotation.Nullable;

@SPI
public interface ConfigSource {

    @Nullable
    Object getProperty(String key);
}
