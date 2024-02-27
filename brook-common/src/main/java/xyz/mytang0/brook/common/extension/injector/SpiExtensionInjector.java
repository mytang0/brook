package xyz.mytang0.brook.common.extension.injector;


import xyz.mytang0.brook.common.annotation.Order;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.extension.SPI;


@Order(1)
public class SpiExtensionInjector implements ExtensionInjector {

    @Override
    public <T> T getInstance(final Class<T> type, final String name) {
        if (!type.isInterface() || !type.isAnnotationPresent(SPI.class)) {
            return null;
        }
        ExtensionLoader<T> loader = ExtensionLoader.getExtensionLoader(type);
        if (loader == null) {
            return null;
        }
        T extension = null;
        if (!loader.getSupportedExtensions().isEmpty()) {
            extension = loader.getExtension(name);
            if (extension == null) {
                extension = loader.getDefaultExtension();
            }
        }
        return extension;
    }
}
