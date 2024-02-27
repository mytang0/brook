package xyz.mytang0.brook.core.exception;


import xyz.mytang0.brook.common.exception.ErrorCode;
import xyz.mytang0.brook.common.exception.ErrorCodeSupplier;
import xyz.mytang0.brook.common.exception.ErrorType;

public enum FlowErrorCode implements ErrorCodeSupplier {

    FLOW_EXECUTION_ERROR(0x0001_0001, ErrorType.INTERNAL_ERROR),

    FLOW_UNSUPPORTED_TASK(0x0001_0002, ErrorType.INTERNAL_ERROR),

    FLOW_UNSUPPORTED_OPERATION(0x0001_0099, ErrorType.INTERNAL_ERROR),

    ENGINE_NOT_EXIST(0x0001_0003, ErrorType.INTERNAL_ERROR),

    FLOW_INSTANCE_NOT_EXIST(0x0001_0004, ErrorType.INTERNAL_ERROR),

    TASK_INSTANCE_NOT_EXIST(0x0001_0005, ErrorType.INTERNAL_ERROR),

    FLOW_DEF_NOT_EXIST(0x0001_0006, ErrorType.INTERNAL_ERROR),

    FLOW_DEF_NAME_DUPLICATE(0x0001_0007, ErrorType.INTERNAL_ERROR),

    FLOW_EXECUTION_CONFLICT(0x0001_0008, ErrorType.INTERNAL_ERROR),

    FLOW_NOT_EXIST(0x0001_0009, ErrorType.INTERNAL_ERROR),

    TASK_NOT_EXIST(0x0001_000a, ErrorType.INTERNAL_ERROR),

    CONCURRENCY_LIMIT(0x0001_000b, ErrorType.INTERNAL_ERROR);

    private final ErrorCode errorCode;

    FlowErrorCode(int code, ErrorType type) {
        this.errorCode = new ErrorCode(code, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
