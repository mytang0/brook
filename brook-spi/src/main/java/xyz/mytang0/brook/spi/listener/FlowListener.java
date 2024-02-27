
package xyz.mytang0.brook.spi.listener;

import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;

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
