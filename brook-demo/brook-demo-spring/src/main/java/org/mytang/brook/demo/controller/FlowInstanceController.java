package org.mytang.brook.demo.controller;

import org.mytang.brook.common.metadata.instance.FlowInstance;
import org.mytang.brook.common.metadata.model.StartFlowReq;
import org.mytang.brook.common.metadata.model.TaskResult;
import org.mytang.brook.core.FlowExecutor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;


@RestController
@RequestMapping("/flow/instance")
public class FlowInstanceController {

    private final FlowExecutor<?> flowExecutor;

    public FlowInstanceController(FlowExecutor<?> flowExecutor) {
        this.flowExecutor = flowExecutor;
    }

    @PostMapping("/start")
    public String start(@RequestBody @Valid StartFlowReq startFlowReq) {
        return flowExecutor.startFlow(startFlowReq);
    }

    @PostMapping("/request")
    public FlowInstance request(@RequestBody @Valid StartFlowReq startFlowReq) {
        return flowExecutor.requestFlow(startFlowReq);
    }

    @PostMapping("/execute")
    public void execute(@RequestBody @Valid TaskResult taskResult) {
        flowExecutor.updateTask(taskResult);
    }

    @PutMapping("/decide")
    public void decide(@RequestParam("flowId") String flowId) {
        flowExecutor.execute(flowId);
    }

    @PutMapping("/pause")
    public void pause(@RequestParam("flowId") String flowId) {
        flowExecutor.pause(flowId);
    }

    @PutMapping("/resume")
    public void resume(@RequestParam("flowId") String flowId) {
        flowExecutor.resume(flowId);
    }

    @PutMapping("/terminate")
    public void terminate(@RequestParam("flowId") String flowId,
                          @RequestParam(value = "reason", required = false) String reason) {
        flowExecutor.terminate(flowId, reason);
    }

    @GetMapping("/get")
    public FlowInstance get(@RequestParam("flowId") String flowId) {
        return flowExecutor.getFlow(flowId);
    }
}
