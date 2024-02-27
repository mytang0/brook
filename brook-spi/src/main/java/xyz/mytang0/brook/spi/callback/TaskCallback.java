package xyz.mytang0.brook.spi.callback;

import xyz.mytang0.brook.common.extension.SPI;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;

@SPI("default")
public interface TaskCallback {

    void onCreated(Object input, TaskInstance taskInstance);

    void onTerminated(Object input, TaskInstance taskInstance);
}
