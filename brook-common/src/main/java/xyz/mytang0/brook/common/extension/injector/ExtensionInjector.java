package xyz.mytang0.brook.common.extension.injector;

import xyz.mytang0.brook.common.extension.SPI;

@SPI
public interface ExtensionInjector {

    <T> T getInstance(final Class<T> type, final String name);
}
