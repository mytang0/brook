package org.mytang.brook.common.exception;

public class BizException extends RuntimeException {

    private static final long serialVersionUID = 8079686177795882484L;

    private final ErrorCode errorCode;

    public BizException(ErrorCodeSupplier errorCode) {
        this(errorCode, null, null);
    }

    public BizException(ErrorCodeSupplier errorCode, String message) {
        this(errorCode, message, null);
    }

    public BizException(ErrorCodeSupplier errorCode, Throwable throwable) {
        this(errorCode, null, throwable);
    }

    public BizException(ErrorCodeSupplier errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode.toErrorCode();
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
