package org.mytang.brook.core.aspect;

import org.mytang.brook.common.metadata.instance.FlowInstance;
import org.mytang.brook.core.listener.FlowListenerFacade;

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
