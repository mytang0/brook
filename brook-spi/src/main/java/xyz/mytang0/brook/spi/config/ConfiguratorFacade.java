package xyz.mytang0.brook.spi.config;

import xyz.mytang0.brook.common.extension.ExtensionDirector;

public class ConfiguratorFacade {

    public static <T> T getConfig(Class<T> type) {
        return ExtensionDirector
                .getExtensionLoader(Configurator.class)
                .getDefaultExtension()
                .getConfig(type);
    }

    public static <T> T refreshConfig(Class<T> type) {
        return ExtensionDirector
                .getExtensionLoader(Configurator.class)
                .getDefaultExtension()
                .refreshConfig(type);
    }
}
