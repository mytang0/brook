package xyz.mytang0.brook.common.exception;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ErrorCode {

    private final int code;
    private final String name;
    private final ErrorType type;

    public ErrorCode(int code, String name, ErrorType type) {
        if (code < 0) {
            throw new IllegalArgumentException("code is negative");
        }
        this.code = code;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public ErrorType getType() {
        return type;
    }

    @Override
    public String toString() {
        return name + ":" + code;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ErrorCode that = (ErrorCode) obj;
        return Objects.equals(this.code, that.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code);
    }
}
