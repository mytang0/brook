package xyz.mytang0.brook.core.exception;

import xyz.mytang0.brook.common.exception.BizException;

public class FlowException extends BizException {

    private static final long serialVersionUID = -1420822787477098823L;

    public FlowException(FlowErrorCode errorCode) {
        super(errorCode);
    }

    public FlowException(FlowErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public FlowException(FlowErrorCode errorCode, Throwable throwable) {
        super(errorCode, throwable);
    }
}
