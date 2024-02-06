package org.mytang.brook.spi.callback;

import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.common.metadata.instance.TaskInstance;

@SPI("default")
public interface TaskCallback {

    void onCreated(Object input, TaskInstance taskInstance);

    void onTerminated(Object input, TaskInstance taskInstance);
}
