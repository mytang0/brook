package org.mytang.brook.spi.callback;

import org.mytang.brook.common.metadata.instance.TaskInstance;

public interface ClassTaskCallback {

    void onCreated(TaskInstance taskInstance);

    void onTerminated(TaskInstance taskInstance);
}
