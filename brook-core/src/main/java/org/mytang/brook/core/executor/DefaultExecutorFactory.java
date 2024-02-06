package org.mytang.brook.core.executor;

import org.mytang.brook.core.utils.ThreadUtils;
import org.mytang.brook.spi.executor.ExecutorFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class DefaultExecutorFactory implements ExecutorFactory {

    private static final String DEFAULT = "default";

    private static final Map<String, ExecutorService> executorPool
            = new ConcurrentHashMap<>();

    @Override
    public ExecutorService getSharedExecutor() {
        return getExecutor(DEFAULT);
    }

    @Override
    public ExecutorService getExecutor(String type) {
        ExecutorService executor = executorPool.get(type);
        if (executor == null
                || executor.isShutdown()
                || executor.isTerminated()) {

            if (executor != null) {
                executorPool.remove(type);
            }

            executor = executorPool.computeIfAbsent(type, __ ->
                    Executors.newCachedThreadPool(
                            ThreadUtils.threadsNamed(type + "-%d")
                    ));
        }
        return executor;
    }

    @Override
    public void shutdown() {
        executorPool.keySet().forEach(
                executorName ->
                        Optional.ofNullable(executorPool.remove(executorName))
                                .ifPresent(executor -> {
                                    try {
                                        log.info("Start shutting down {} executor", executorName);
                                        executor.shutdown();
                                        log.info("Finish shutting down {} executor", executorName);
                                    } catch (Throwable ignored) {
                                    }
                                })
        );
    }

    @Override
    public void destroy() {
        this.shutdown();
    }
}
