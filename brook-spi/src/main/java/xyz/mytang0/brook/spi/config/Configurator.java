package xyz.mytang0.brook.spi.config;

import xyz.mytang0.brook.common.extension.SPI;

@SPI(value = "default")
public interface Configurator {

    <T> T getConfig(Class<T> type);

    <T> T refreshConfig(Class<T> type);
}
