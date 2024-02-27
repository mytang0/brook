package xyz.mytang0.brook.demo.callback;

import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.spi.callback.ClassTaskCallback;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DemoCallback implements ClassTaskCallback {

    @Override
    public void onCreated(TaskInstance taskInstance) {
        log.info("{} {} callback on created",
                taskInstance.getTaskName(),
                taskInstance.getTaskId());
    }

    @Override
    public void onTerminated(TaskInstance taskInstance) {
        log.info("{} {} callback on terminated",
                taskInstance.getTaskName(),
                taskInstance.getTaskId());
    }
}
