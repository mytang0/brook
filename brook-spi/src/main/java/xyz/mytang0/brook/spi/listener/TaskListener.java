
package xyz.mytang0.brook.spi.listener;

import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;

@SPI
public interface TaskListener {

    default boolean test(TaskInstance taskInstance) {
        return true;
    }

    default void onCreated(TaskInstance taskInstance) {

    }

    default void onTerminated(TaskInstance taskInstance) {

    }

    default boolean isAsync() {
        return false;
    }
}
