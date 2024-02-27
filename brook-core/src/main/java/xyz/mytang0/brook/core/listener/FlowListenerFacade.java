package xyz.mytang0.brook.core.listener;

import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.spi.executor.ExecutorFactory;
import xyz.mytang0.brook.spi.listener.FlowListener;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@Slf4j
public class FlowListenerFacade {

    private final ExecutorService asyncExecutor;

    private final ExtensionLoader<FlowListener> flowListenerLoader;

    public FlowListenerFacade() {
        this.asyncExecutor = ExtensionDirector
                .getExtensionLoader(ExecutorFactory.class)
                .getDefaultExtension()
                .getExecutor(this.getClass().getSimpleName());
        this.flowListenerLoader = ExtensionDirector.getExtensionLoader(FlowListener.class);
    }

    public void onCreating(FlowInstance flowInstance) {
        flowListenerLoader.getExtensionInstances().forEach(flowListener -> {
            if (flowListener.test(flowInstance)) {
                flowListener.onCreating(flowInstance);
            }
        });
    }

    public void onCreated(FlowInstance flowInstance) {
        flowListenerLoader.getExtensionInstances().forEach(flowListener -> {
            if (flowListener.test(flowInstance)) {
                if (flowListener.isAsync()) {
                    asyncExecutor.execute(() -> {
                        try {
                            flowListener.onCreated(flowInstance);
                        } catch (Throwable throwable) {
                            log.error(String.format("Async execute %s flowId(%s) onCreated exception",
                                            flowListener.getClass().getSimpleName(),
                                            flowInstance.getFlowId()),
                                    throwable);
                        }
                    });
                } else {
                    flowListener.onCreated(flowInstance);
                }
            }
        });
    }

    public void onTerminated(FlowInstance flowInstance) {
        if (!flowInstance.getStatus().isTerminal()) {
            return;
        }
        flowListenerLoader.getExtensionInstances().forEach(flowListener -> {
            if (flowListener.test(flowInstance)) {
                if (flowListener.isAsync()) {
                    asyncExecutor.execute(() -> {
                        try {
                            flowListener.onTerminated(flowInstance);
                        } catch (Throwable throwable) {
                            log.error(String.format("Async execute %s flowId(%s) terminated exception",
                                            flowListener.getClass().getSimpleName(),
                                            flowInstance.getFlowId()),
                                    throwable);
                        }
                    });
                } else {
                    flowListener.onTerminated(flowInstance);
                }
            }
        });
    }
}
