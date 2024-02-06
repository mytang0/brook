
package org.mytang.brook.spi.listener;

import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.common.metadata.instance.TaskInstance;

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
