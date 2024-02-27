package xyz.mytang0.brook.core.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.Executors.defaultThreadFactory;

public abstract class ThreadUtils {

    private ThreadUtils() {
    }

    /**
     * Creates a {@link ThreadFactory} that creates named threads using the specified naming format.
     *
     * @param nameFormat a {@link String#format(String, Object...)}-compatible
     *                   format string, to which a string will be supplied as the single
     *                   parameter. This string will be unique to this instance of the
     *                   ThreadFactory and will be assigned sequentially.
     * @return the created ThreadFactory
     */
    public static ThreadFactory threadsNamed(String nameFormat) {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setThreadFactory(new ContextClassLoaderThreadFactory(Thread
                        .currentThread()
                        .getContextClassLoader(), defaultThreadFactory()))
                .build();
    }

    /**
     * Creates a {@link ThreadFactory} that creates named daemon threads. using the specified naming format.
     *
     * @param nameFormat see {@link #threadsNamed(String)}
     * @return the created ThreadFactory
     */
    public static ThreadFactory daemonThreadsNamed(String nameFormat) {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(true)
                .setThreadFactory(new ContextClassLoaderThreadFactory(Thread
                        .currentThread()
                        .getContextClassLoader(), defaultThreadFactory()))
                .build();
    }

    private static class ContextClassLoaderThreadFactory implements ThreadFactory {
        private final ClassLoader classLoader;
        private final ThreadFactory delegate;

        public ContextClassLoaderThreadFactory(ClassLoader classLoader, ThreadFactory delegate) {
            this.classLoader = classLoader;
            this.delegate = delegate;
        }

        @Override
        public Thread newThread(@Nonnull Runnable runnable) {
            Thread thread = delegate.newThread(runnable);
            thread.setContextClassLoader(classLoader);
            return thread;
        }
    }
}

