package org.mytang.brook.common.extension.injector;

import org.mytang.brook.common.extension.SPI;

@SPI
public interface ExtensionInjector {

    <T> T getInstance(final Class<T> type, final String name);
}
