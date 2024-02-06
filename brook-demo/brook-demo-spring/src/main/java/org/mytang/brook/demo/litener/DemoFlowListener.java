package org.mytang.brook.demo.litener;

import org.mytang.brook.common.metadata.enums.TaskStatus;
import org.mytang.brook.common.metadata.instance.FlowInstance;
import org.mytang.brook.spi.listener.FlowListener;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Slf4j
@Component
public class DemoFlowListener implements FlowListener {

    @Override
    public void onTerminated(FlowInstance flowInstance) {
        log.info("flowId: {} flowName: {} status: {}, statusSet: {}",
                flowInstance.getFlowId(),
                flowInstance.getFlowName(),
                flowInstance.getStatus(),
                flowInstance.getTaskInstances().stream()
                        .map(taskInstance ->
                                new Status(
                                        taskInstance.getTaskId(),
                                        taskInstance.getTaskName(),
                                        taskInstance.getStatus()))
                        .collect(Collectors.toList()));
    }

    @Data
    @AllArgsConstructor
    private static class Status {

        private String taskId;

        private String taskName;

        private TaskStatus status;

        public String toString() {
            return "\ntaskId: " + taskId
                    + ", taskName: " + taskName
                    + ", status: " + status + "\n";
        }
    }
}
