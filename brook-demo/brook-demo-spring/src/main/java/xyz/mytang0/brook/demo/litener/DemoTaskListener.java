package xyz.mytang0.brook.demo.litener;

import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.spi.listener.TaskListener;
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
