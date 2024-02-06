package org.mytang.brook.demo.litener;

import org.mytang.brook.common.metadata.instance.TaskInstance;
import org.mytang.brook.spi.listener.TaskListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DemoTaskListener implements TaskListener {

    @Override
    public void onCreated(TaskInstance taskInstance) {
        log.info("taskId: {} taskName: {} created",
                taskInstance.getTaskId(),
                taskInstance.getTaskName());
    }

    @Override
    public void onTerminated(TaskInstance taskInstance) {
        log.info("taskId: {} taskName: {} terminated: {}",
                taskInstance.getTaskId(),
                taskInstance.getTaskName(),
                taskInstance.getStatus());
    }
}
