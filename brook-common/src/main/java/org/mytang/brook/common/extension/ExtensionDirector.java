package org.mytang.brook.common.extension;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExtensionDirector {

    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> extensionLoadersMap = new ConcurrentHashMap<>(64);
    private static final AtomicBoolean destroyed = new AtomicBoolean();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ExtensionDirector::destroy));
    }

    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        checkDestroyed();
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type (" + type + ") is not an interface!");
        }
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type (" + type +
                    ") is not an extension, because it is NOT annotated with @" + SPI.class.getSimpleName() + "!");
        }

        ExtensionLoader<T> loader = (ExtensionLoader<T>) extensionLoadersMap.get(type);

        if (loader == null) {
            loader = createExtensionLoader(type);
        }
        return loader;
    }

    @SuppressWarnings("unchecked")
    private static <T> ExtensionLoader<T> createExtensionLoader(Class<T> type) {
        ExtensionLoader<T> loader;
        extensionLoadersMap.putIfAbsent(type, ExtensionLoader.getExtensionLoader(type));
        loader = (ExtensionLoader<T>) extensionLoadersMap.get(type);
        return loader;
    }

    private static boolean withExtensionAnnotation(Class<?> type) {
        return type.isAnnotationPresent(SPI.class);
    }


    private static void checkDestroyed() {
        if (destroyed.get()) {
            throw new IllegalStateException("ExtensionDirector is destroyed");
        }
    }

    public static void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            for (ExtensionLoader<?> extensionLoader : extensionLoadersMap.values()) {
                extensionLoader.destroy();
            }
            extensionLoadersMap.clear();
        }
    }
}
