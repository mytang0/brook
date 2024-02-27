package xyz.mytang0.brook.core.aspect;

import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.core.listener.FlowListenerFacade;

public final class FlowAspect {

    private final FlowListenerFacade flowListenerFacade;

    public FlowAspect() {
        this.flowListenerFacade = new FlowListenerFacade();
    }

    public void onCreating(final FlowInstance flowInstance) {
        flowListenerFacade.onCreating(flowInstance);
    }

    public void onCreated(final FlowInstance flowInstance) {
        flowListenerFacade.onCreated(flowInstance);
    }

    public void onTerminated(final FlowInstance flowInstance) {
        flowListenerFacade.onTerminated(flowInstance);
    }
}
