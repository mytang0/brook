package xyz.mytang0.brook.spi.executor;

import xyz.mytang0.brook.common.extension.Disposable;
import xyz.mytang0.brook.common.extension.SPI;

import java.util.concurrent.ExecutorService;

@SPI(value = "default")
public interface ExecutorFactory extends Disposable {

    ExecutorService getSharedExecutor();

    default ExecutorService getExecutor(Enum<?> type) {
        return getExecutor(type.name());
    }

    ExecutorService getExecutor(String type);

    default void shutdown() {

    }
}
