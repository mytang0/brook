package xyz.mytang0.brook.spi.callback;

import xyz.mytang0.brook.common.metadata.instance.TaskInstance;

public interface ClassTaskCallback {

    void onCreated(TaskInstance taskInstance);

    void onTerminated(TaskInstance taskInstance);
}
