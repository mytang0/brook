
package org.mytang.brook.spi.listener;

import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.common.metadata.instance.FlowInstance;

@SPI
public interface FlowListener {

    default boolean test(FlowInstance flowInstance) {
        return true;
    }

    default void onCreating(FlowInstance flowInstance) {

    }

    default void onCreated(FlowInstance flowInstance) {

    }

    default void onTerminated(FlowInstance flowInstance) {

    }

    default boolean isAsync() {
        return false;
    }
}
