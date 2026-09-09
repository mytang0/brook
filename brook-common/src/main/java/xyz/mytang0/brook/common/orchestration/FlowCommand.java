package xyz.mytang0.brook.common.orchestration;

import java.io.Serializable;

public interface FlowCommand extends Serializable {

    String getFlowId();
}
